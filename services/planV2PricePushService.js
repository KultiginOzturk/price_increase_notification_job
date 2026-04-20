import { query as pgQuery } from '../lib/postgres.js';
import { getRCP, SHARED } from '../config/tables.js';
import { runQuery } from '../utils/bigquery.js';
import { getMonthAbbreviation } from '../utils/repricingScheduling.js';
import { calculatePushArv, calculatePushServiceCharge } from '../utils/pricePushMath.js';

const EFFECTIVE_PERIOD_PATTERN = /^\d{4}-\d{2}$/;

export const PLAN_V2_PUSH_REVIEW_TABS = ['normal', 'vip', 'always_manual', 'watchlist'];

export function normalizeEffectivePeriodParam(value) {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return EFFECTIVE_PERIOD_PATTERN.test(trimmed) ? trimmed : null;
}

export function getEffectivePeriodMeta(effectivePeriod) {
    const normalized = normalizeEffectivePeriodParam(effectivePeriod);
    if (!normalized) return null;

    const year = Number(normalized.slice(0, 4));
    const monthNumber = Number(normalized.slice(5, 7));
    if (!Number.isFinite(year) || !Number.isFinite(monthNumber) || monthNumber < 1 || monthNumber > 12) {
        return null;
    }

    const effectiveDate = new Date(Date.UTC(year, monthNumber - 1, 1));
    effectiveDate.setUTCDate(0);

    return {
        effectivePeriod: normalized,
        cohortNumber: Number(normalized.replace('-', '')),
        monthNumber,
        targetMonth: getMonthAbbreviation(monthNumber) || normalized,
        effectiveDate: effectiveDate.toISOString().slice(0, 10),
    };
}

export async function getPublishedPlanV2(client) {
    const result = await pgQuery(`
        SELECT id, company_key, status, philosophy, published_at, last_generated_at, speed_limit_global_pct
        FROM planv2_plan
        WHERE company_key = $1
          AND status = 'published'
        ORDER BY published_at DESC NULLS LAST, id DESC
        LIMIT 1
    `, [client]);
    return result.rows[0] || null;
}

export async function getPlanV2PushPeriods(planId, client) {
    const result = await pgQuery(`
        WITH eligible_periods AS (
            SELECT
                ad.effective_period,
                COUNT(*)::int AS account_count,
                COALESCE(SUM(
                    CASE
                        WHEN ad.override_increase_pct IS NOT NULL
                            THEN COALESCE(ad.total_recurring_revenue, 0) * ad.override_increase_pct / 100
                        ELSE COALESCE(ad.computed_increase_dollar, 0)
                    END
                ), 0) AS total_revenue_impact
            FROM planv2_account_decision ad
            INNER JOIN planv2_plan p
                ON p.id = ad.plan_id
               AND p.company_key = $2
            LEFT JOIN planv2_client_response account_skip
                ON account_skip.plan_id = ad.plan_id
               AND account_skip.client = $2
               AND account_skip.master_account_id = ad.master_account_id
               AND account_skip.subscription_id IS NULL
               AND account_skip.action = 'skip'
            WHERE ad.plan_id = $1
              AND ad.effective_period IS NOT NULL
              AND ad.is_ghost = FALSE
              AND ad.review_tab = ANY($3::text[])
              AND COALESCE(ad.override_increase_pct, ad.computed_increase_pct, 0) > 0
              AND account_skip.id IS NULL
            GROUP BY ad.effective_period
        )
        SELECT
            effective_period,
            account_count,
            total_revenue_impact
        FROM eligible_periods
        ORDER BY effective_period
    `, [planId, client, PLAN_V2_PUSH_REVIEW_TABS]);

    return result.rows.map((row) => {
        const meta = getEffectivePeriodMeta(row.effective_period);
        return {
            effectivePeriod: row.effective_period,
            cohortNumber: meta?.cohortNumber || null,
            targetMonth: meta?.targetMonth || row.effective_period,
            effectiveDate: meta?.effectiveDate || null,
            accountCount: Number(row.account_count) || 0,
            totalRevenueImpact: Number(row.total_revenue_impact) || 0,
        };
    });
}

function extractSimpleId(value) {
    if (value === null || value === undefined) return null;
    const strVal = String(value).trim();
    if (strVal.startsWith('{') || strVal.startsWith('[')) {
        const match = strVal.match(/['"]?ticketID['"]?\s*:\s*['"]?(\d+)['"]?/i);
        return match ? match[1] : null;
    }
    if (/^[\w-]+$/.test(strVal)) {
        return strVal;
    }
    return null;
}

export async function buildPlanV2PricePushSource({ client, effectivePeriod }) {
    const normalizedPeriod = normalizeEffectivePeriodParam(effectivePeriod);
    if (!normalizedPeriod) {
        const error = new Error('effectivePeriod must be in YYYY-MM format');
        error.statusCode = 400;
        throw error;
    }

    const periodMeta = getEffectivePeriodMeta(normalizedPeriod);
    const plan = await getPublishedPlanV2(client);
    if (!plan) {
        return {
            client,
            plan: null,
            effectivePeriod: normalizedPeriod,
            effectiveDate: periodMeta?.effectiveDate || null,
            periods: [],
            summary: {
                totalAccounts: 0,
                totalSubscriptions: 0,
                excludedSubscriptions: 0,
                avgIncreasePct: 0,
                totalRevenueImpact: 0,
            },
            accounts: [],
            subscriptions: [],
        };
    }

    const periods = await getPlanV2PushPeriods(plan.id, client);
    const selectedPeriod = periods.find((period) => period.effectivePeriod === normalizedPeriod) || null;

    const accountResult = await pgQuery(`
        SELECT
            ad.id AS account_decision_id,
            ad.master_account_id,
            COALESCE(ad.account_name, ad.master_account_id) AS account_name,
            COALESCE(ad.margin_bucket_label, ad.review_tab, 'scheduled') AS margin_segment,
            ad.review_tab,
            COALESCE(ad.override_increase_pct, ad.computed_increase_pct, 0) AS effective_increase_pct,
            COALESCE(
                CASE
                    WHEN ad.override_increase_pct IS NOT NULL
                        THEN COALESCE(ad.total_recurring_revenue, 0) * ad.override_increase_pct / 100
                    ELSE ad.computed_increase_dollar
                END,
                0
            ) AS effective_increase_dollar,
            COALESCE(ad.total_recurring_revenue, 0) AS total_recurring_revenue,
            COALESCE(ad.subscription_count, 0) AS subscription_count,
            COALESCE(ad.ntm_gross_margin_pct, 0) AS current_margin_pct
        FROM planv2_account_decision ad
        INNER JOIN planv2_plan p
            ON p.id = ad.plan_id
           AND p.company_key = $2
        LEFT JOIN planv2_client_response account_skip
            ON account_skip.plan_id = ad.plan_id
           AND account_skip.client = $2
           AND account_skip.master_account_id = ad.master_account_id
           AND account_skip.subscription_id IS NULL
           AND account_skip.action = 'skip'
        WHERE ad.plan_id = $1
          AND ad.effective_period = $3
          AND ad.is_ghost = FALSE
          AND ad.review_tab = ANY($4::text[])
          AND COALESCE(ad.override_increase_pct, ad.computed_increase_pct, 0) > 0
          AND account_skip.id IS NULL
        ORDER BY COALESCE(ad.account_name, ad.master_account_id), ad.master_account_id
    `, [plan.id, client, normalizedPeriod, PLAN_V2_PUSH_REVIEW_TABS]);

    if (accountResult.rows.length === 0) {
        return {
            client,
            plan,
            effectivePeriod: normalizedPeriod,
            effectiveDate: selectedPeriod?.effectiveDate || periodMeta?.effectiveDate || null,
            periods,
            summary: {
                totalAccounts: 0,
                totalSubscriptions: 0,
                excludedSubscriptions: 0,
                avgIncreasePct: 0,
                totalRevenueImpact: 0,
            },
            accounts: [],
            subscriptions: [],
        };
    }

    const subscriptionResult = await pgQuery(`
        SELECT
            sd.account_decision_id,
            sd.master_account_id,
            sd.subscription_id,
            sd.customer_id,
            COALESCE(sd.service_type_name, 'Unknown') AS service_type_name,
            COALESCE(sd.current_price, 0) AS current_price,
            COALESCE(sd.new_price, 0) AS new_price,
            COALESCE(sd.increase_pct, 0) AS increase_pct,
            COALESCE(sd.increase_dollar_annual, 0) AS increase_dollar_annual
        FROM planv2_subscription_decision sd
        INNER JOIN planv2_account_decision ad
            ON ad.id = sd.account_decision_id
           AND ad.plan_id = sd.plan_id
        INNER JOIN planv2_plan p
            ON p.id = ad.plan_id
           AND p.company_key = $2
        LEFT JOIN planv2_client_response account_skip
            ON account_skip.plan_id = sd.plan_id
           AND account_skip.client = $2
           AND account_skip.master_account_id = sd.master_account_id
           AND account_skip.subscription_id IS NULL
           AND account_skip.action = 'skip'
        LEFT JOIN planv2_client_response subscription_skip
            ON subscription_skip.plan_id = sd.plan_id
           AND subscription_skip.client = $2
           AND subscription_skip.master_account_id = sd.master_account_id
           AND subscription_skip.subscription_id = sd.subscription_id
           AND subscription_skip.action = 'skip'
        WHERE sd.plan_id = $1
          AND ad.effective_period = $3
          AND ad.is_ghost = FALSE
          AND ad.review_tab = ANY($4::text[])
          AND COALESCE(ad.override_increase_pct, ad.computed_increase_pct, 0) > 0
          AND account_skip.id IS NULL
          AND subscription_skip.id IS NULL
          AND COALESCE(sd.current_price, 0) > 0
          AND COALESCE(sd.new_price, 0) > 0
          AND ABS(COALESCE(sd.new_price, 0) - COALESCE(sd.current_price, 0)) > 0.0001
        ORDER BY sd.master_account_id, sd.service_type_name, sd.subscription_id
    `, [plan.id, client, normalizedPeriod, PLAN_V2_PUSH_REVIEW_TABS]);

    const subscriptionIds = [...new Set(subscriptionResult.rows
        .map((row) => String(row.subscription_id || '').trim())
        .filter(Boolean))];

    const subscriptionMeta = new Map();
    if (subscriptionIds.length > 0) {
        const RCP = getRCP(client);
        const subscriptionMetaRows = await runQuery(`
            SELECT
                CAST(cs.subscription_id AS STRING) AS subscription_id,
                CAST(cs.customer_id AS STRING) AS customer_id,
                COALESCE(cs.recurring_invoice, '') AS recurring_ticket_id,
                CAST(sm.recurring_price AS FLOAT64) AS recurring_price,
                CAST(sm.annual_revenue AS FLOAT64) AS annual_revenue,
                CAST(sm.services_per_year AS FLOAT64) AS services_per_year,
                CAST(st.ttm_total_revenue AS FLOAT64) AS old_production_value,
                SAFE_DIVIDE(CAST(st.ttm_margin AS FLOAT64), NULLIF(CAST(st.ttm_total_revenue AS FLOAT64), 0)) * 100 AS subscription_margin_pct,
                sm.service_type_name,
                CAST(sm.is_active AS BOOL) AS is_active
            FROM ${SHARED.curSubscription} cs
            LEFT JOIN ${RCP.subscriptionMaster} sm
                ON CAST(cs.subscription_id AS STRING) = CAST(sm.subscription_id AS STRING)
               AND cs.client = sm.client
            LEFT JOIN ${RCP.subscriptionTtm} st
                ON CAST(cs.subscription_id AS STRING) = CAST(st.subscription_id AS STRING)
               AND cs.client = st.client
            WHERE cs.client = @client
              AND CAST(cs.subscription_id AS STRING) IN UNNEST(@subscriptionIds)
        `, { client, subscriptionIds }, 'planv2-price-push-subscription-meta');

        for (const row of subscriptionMetaRows) {
            subscriptionMeta.set(String(row.subscription_id), row);
        }
    }

    const subscriptions = [];
    for (const row of subscriptionResult.rows) {
        const meta = subscriptionMeta.get(String(row.subscription_id)) || {};
        const oldServiceCharge = Number(meta.recurring_price ?? row.current_price) || 0;
        const calculatedIncreasePct = Number(row.increase_pct) || 0;
        const newServiceCharge = oldServiceCharge * (1 + calculatedIncreasePct / 100);
        const recurringTicketId = extractSimpleId(meta.recurring_ticket_id);
        if (!recurringTicketId || oldServiceCharge <= 0 || newServiceCharge <= 0) {
            continue;
        }

        const servicesPerYear = Number(meta.services_per_year) || 0;
        const oldArv = Number(meta.annual_revenue)
            || (servicesPerYear > 0 ? oldServiceCharge * servicesPerYear : 0);
        const annualIncrease = Number(row.increase_dollar_annual) || 0;
        const newArv = calculatePushArv({
            oldArv,
            annualIncrease,
            increasePct: calculatedIncreasePct,
        });

        subscriptions.push({
            accountDecisionId: Number(row.account_decision_id),
            masterAccountId: String(row.master_account_id),
            subscriptionId: String(row.subscription_id),
            customerId: row.customer_id ? String(row.customer_id) : (meta.customer_id ? String(meta.customer_id) : null),
            recurringTicketId,
            oldServiceCharge,
            newServiceCharge,
            oldArv,
            newArv: Number.isFinite(newArv) ? newArv : 0,
            oldProductionValue: Number(meta.old_production_value) || oldArv,
            subscriptionMarginPct: Number(meta.subscription_margin_pct) || 0,
            calculatedIncreasePct,
            serviceTypeName: meta.service_type_name || row.service_type_name || 'Unknown',
            isActive: meta.is_active === null || meta.is_active === undefined ? true : Boolean(meta.is_active),
            annualIncrease,
        });
    }

    const subsByAccount = new Map();
    for (const subscription of subscriptions) {
        if (!subsByAccount.has(subscription.masterAccountId)) {
            subsByAccount.set(subscription.masterAccountId, []);
        }
        subsByAccount.get(subscription.masterAccountId).push(subscription);
    }

    const accounts = accountResult.rows
        .map((row) => {
            const accountSubscriptions = subsByAccount.get(String(row.master_account_id)) || [];
            if (accountSubscriptions.length === 0) return null;

            const snapshotTotalArv = accountSubscriptions.reduce((sum, subscription) => sum + subscription.oldArv, 0);
            const totalRevenueImpact = accountSubscriptions.reduce((sum, subscription) => sum + subscription.annualIncrease, 0);

            return {
                masterAccountId: String(row.master_account_id),
                accountName: row.account_name,
                marginSegment: row.margin_segment,
                reviewTab: row.review_tab,
                increasePct: Number(row.effective_increase_pct) || 0,
                currentRevenue: snapshotTotalArv,
                newRevenue: snapshotTotalArv + totalRevenueImpact,
                currentMarginPct: Number(row.current_margin_pct) || 0,
                subscriptionCount: accountSubscriptions.length,
                totalRevenueImpact,
                isPurgeCandidate: row.review_tab === 'watchlist',
                subscriptions: accountSubscriptions,
            };
        })
        .filter(Boolean);

    const allSubscriptions = accounts.flatMap((account) => account.subscriptions);
    const totalRevenueImpact = accounts.reduce((sum, account) => sum + account.totalRevenueImpact, 0);
    const avgIncreasePct = allSubscriptions.length > 0
        ? allSubscriptions.reduce((sum, subscription) => sum + subscription.calculatedIncreasePct, 0) / allSubscriptions.length
        : 0;

    return {
        client,
        plan,
        effectivePeriod: normalizedPeriod,
        effectiveDate: selectedPeriod?.effectiveDate || periodMeta?.effectiveDate || null,
        periods,
        summary: {
            totalAccounts: accounts.length,
            totalSubscriptions: allSubscriptions.length,
            excludedSubscriptions: 0,
            avgIncreasePct,
            totalRevenueImpact,
        },
        accounts,
        subscriptions: allSubscriptions,
    };
}
