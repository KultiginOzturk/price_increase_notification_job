import { randomUUID } from 'crypto';
import { query as pgQuery } from '../lib/postgres.js';
import { getRCP, INPUTS, SHARED, PLAN_TABLES } from '../config/tables.js';
import { runQuery } from '../utils/bigquery.js';
import { deriveTimingFromPeriod } from '../routes/planV2/timing.js';
import { sendPriceIncreaseEmail, validateEmails } from './emailService.js';
import { PLAN_V2_PUSH_REVIEW_TABS, buildPlanV2PricePushSource } from './planV2PricePushService.js';
import logger from '../lib/logger.js';
import { loadActiveNotificationConfig } from './pricingPipeline/notification/configLoader.js';
import {
    openNotificationRun,
    markNotificationRunSending,
    insertNotificationRecipient,
    updateNotificationRecipientOutcome,
    closeNotificationRun,
} from './pricingPipeline/notification/runRepo.js';
import { emitNotificationSendEvent } from './pricingPipeline/notification/sendEventRecorder.js';
import { transition as transitionJourneyState } from './pricingPipeline/push/journeyStateRepo.js';
import { emitDecisionEventsBatch } from './pricingPipeline/decisionEvents/emit.js';
import { EMITTED_BY } from './pricingPipeline/decisionEvents/types.js';

// decision_event phase='notification' codes — one event per account per send
// run. Mirrors the inp_price_increase_notification_events status values so
// the unified trace UI shows notification outcomes alongside eligibility /
// pricing / push.
const NOTIFICATION_DECISION_CODE = Object.freeze({
    sent:                 'NOTIFICATION_SENT',
    test:                 'NOTIFICATION_TEST',
    failed:               'NOTIFICATION_FAILED',
    skipped_no_email:     'NOTIFICATION_SUPPRESSED_NO_EMAIL',
    skipped_unsubscribed: 'NOTIFICATION_SUPPRESSED_UNSUBSCRIBED',
    skipped_already_sent: 'NOTIFICATION_SUPPRESSED_ALREADY_SENT',
    skipped_excluded_tag: 'NOTIFICATION_SUPPRESSED_EXCLUDED_TAG',
});

const PRE_PUSH_MODE = 'pre_push';
const POST_PUSH_MODE = 'post_push';

const normalizeEmail = (value) => {
    if (typeof value !== 'string') return null;
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    const candidates = value.split(/[;,]/).map((part) => part.trim().toLowerCase()).filter(Boolean);
    const firstValid = candidates.find((candidate) => emailRegex.test(candidate));
    return firstValid || candidates[0] || null;
};

const escapeSqlString = (value) => String(value)
    .replace(/\\/g, '\\\\')
    .replace(/'/g, "\\'");

const toSqlString = (value) => {
    if (value === null || value === undefined || value === '') return 'NULL';
    return `'${escapeSqlString(value)}'`;
};

const toSqlDate = (value) => {
    if (!value) return 'NULL';
    return `DATE '${escapeSqlString(value)}'`;
};

const toSqlNumber = (value, fallback = 0) => {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? String(numeric) : String(fallback);
};

const isValidEmail = (email) => {
    if (!email) return false;
    return validateEmails([email]).valid.length > 0;
};

const buildCustomerName = (contact, fallbackName = null) => {
    const name = [contact?.first_name, contact?.last_name].filter(Boolean).join(' ').trim();
    return name || contact?.company_name?.trim() || fallbackName || null;
};

const buildServiceRow = (service) => {
    const currentPrice = Number(service.currentPrice ?? service.oldServiceCharge ?? 0) || 0;
    const newPrice = Number(service.newPrice ?? service.newServiceCharge ?? 0) || 0;
    const increaseAmount = Number(service.increaseAmount ?? (newPrice - currentPrice)) || 0;
    const increasePct = Number(
        service.increasePct
        ?? service.calculatedIncreasePct
        ?? (currentPrice > 0 ? ((newPrice - currentPrice) / currentPrice) * 100 : 0)
    ) || 0;

    return {
        serviceTypeName: service.serviceTypeName || service.service_type_name || 'Service',
        currentPrice,
        newPrice,
        increaseAmount,
        increasePct,
        // Billing data for template engine
        billingFrequency: service.billingFrequency ?? service.billing_frequency ?? null,
        servicesPerYear: service.servicesPerYear ?? service.services_per_year ?? null,
        recurringPriceCharge: service.recurringPriceCharge ?? service.recurring_price_charge ?? null,
        // Authoritative annual totals (pre-push: oldArv/newArv/annualIncrease from planV2PricePushService).
        // emailService prefers these over reconstructing via per-cycle × spy, which breaks when
        // service cadence differs from billing cadence.
        annualCurrent: Number(service.annualCurrent ?? service.annual_current ?? service.oldArv ?? service.old_arv) || 0,
        annualNew: Number(service.annualNew ?? service.annual_new ?? service.newArv ?? service.new_arv) || 0,
        annualIncrease: Number(service.annualIncrease ?? service.annual_increase) || 0,
        // Square-footage pricing tier (e.g. "sqft_3k_4k") for the {square_footage_tier} token.
        pricingTier: service.pricingTier ?? service.pricing_tier ?? null,
    };
};

const buildSelectionId = (target) => (
    target.mode === POST_PUSH_MODE ? String(target.queueId) : String(target.masterAccountId)
);

const buildDedupKey = (target) => (
    target.mode === POST_PUSH_MODE
        ? `${POST_PUSH_MODE}:${target.queueId}`
        : `${PRE_PUSH_MODE}:${target.planId || ''}:${target.effectivePeriod || ''}:${target.masterAccountId}`
);

export const buildNotificationSummary = (targets) => {
    const summary = {
        total: targets.length,
        eligible: 0,
        noEmail: 0,
        unsubscribed: 0,
        alreadySent: 0,
        excludedTag: 0,
    };

    for (const target of targets) {
        switch (target.eligibility) {
            case 'eligible':
                summary.eligible++;
                break;
            case 'no_email':
                summary.noEmail++;
                break;
            case 'unsubscribed':
                summary.unsubscribed++;
                break;
            case 'already_sent':
                summary.alreadySent++;
                break;
            case 'excluded_tag':
                summary.excludedTag++;
                break;
            default:
                break;
        }
    }

    return summary;
};

const normalizeDateOnly = (value) => {
    if (!value) return null;
    const asString = String(value).slice(0, 10);
    return /^\d{4}-\d{2}-\d{2}$/.test(asString) ? asString : null;
};

export const isDuePrePushNotificationPeriod = ({ targetDate, noticeDate, effectiveDate }) => {
    const normalizedTargetDate = normalizeDateOnly(targetDate);
    const normalizedNoticeDate = normalizeDateOnly(noticeDate);
    const normalizedEffectiveDate = normalizeDateOnly(effectiveDate);

    if (!normalizedTargetDate || !normalizedNoticeDate || !normalizedEffectiveDate) {
        return false;
    }

    return normalizedTargetDate >= normalizedNoticeDate && normalizedTargetDate <= normalizedEffectiveDate;
};

const fetchCustomerContacts = async (client, customerIds) => {
    const dedupedCustomerIds = [...new Set((customerIds || []).map((id) => String(id || '').trim()).filter(Boolean))];
    if (dedupedCustomerIds.length === 0) {
        return new Map();
    }

    // Prefer billing_first_name when present: cur_customer lacks it, norm_customer_fieldroutes
    // lacks it, it only lives on raw_layer.FR_CUSTOMER. Raw uses the parent client key
    // ('CDPC' covers all CDPC_* offices) + OFFICE_ID, so we hop through norm to pick up the
    // office_id for this (client, customer_id) tuple.
    const rows = await runQuery(`
        SELECT
            CAST(c.customer_id AS STRING) AS customer_id,
            c.email,
            c.billing_email,
            COALESCE(NULLIF(TRIM(f.billing_first_name), ''), c.first_name) AS first_name,
            c.last_name,
            c.company_name
        FROM ${SHARED.curCustomer} c
        LEFT JOIN ${SHARED.normCustomerFieldroutes} n
            ON CAST(n.customer_id AS STRING) = CAST(c.customer_id AS STRING)
           AND n.client = c.client
        LEFT JOIN (
            SELECT
                CAST(CUSTOMER_ID AS STRING) AS customer_id,
                CAST(OFFICE_ID AS STRING) AS office_id,
                MAX(NULLIF(TRIM(BILLING_FIRST_NAME), '')) AS billing_first_name
            FROM ${SHARED.rawCustomer}
            GROUP BY customer_id, office_id
        ) f
            ON f.customer_id = CAST(c.customer_id AS STRING)
           AND f.office_id = CAST(n.office_id AS STRING)
        WHERE c.client = @client
          AND CAST(c.customer_id AS STRING) IN UNNEST(@customerIds)
    `, { client, customerIds: dedupedCustomerIds }, 'price-increase-notify-customer-contacts');

    return new Map(rows.map((row) => [String(row.customer_id), {
        ...row,
        email: normalizeEmail(row.billing_email) || normalizeEmail(row.email) || null,
    }]));
};

const pickPrimaryContact = (customerIds, contactsById) => {
    const candidates = (customerIds || [])
        .map((customerId) => contactsById.get(String(customerId)))
        .filter(Boolean);

    if (candidates.length === 0) return null;

    const withEmail = candidates.find((candidate) => normalizeEmail(candidate.email));
    return withEmail || candidates[0] || null;
};

export async function buildPrePushNotificationTargets({ client, batch }) {
    const customerIds = batch.accounts.flatMap((account) =>
        (account.subscriptions || []).map((subscription) => String(subscription.customerId || '').trim()).filter(Boolean)
    );
    const contactsById = await fetchCustomerContacts(client, customerIds);

    return batch.accounts.map((account) => {
        const accountCustomerIds = [...new Set(
            (account.subscriptions || []).map((subscription) => String(subscription.customerId || '').trim()).filter(Boolean)
        )];
        const primaryContact = pickPrimaryContact(accountCustomerIds, contactsById);

        return {
            mode: PRE_PUSH_MODE,
            selectionId: String(account.masterAccountId),
            dedupKey: `${PRE_PUSH_MODE}:${batch.plan?.id || ''}:${batch.effectivePeriod || ''}:${account.masterAccountId}`,
            queueId: null,
            planId: batch.plan?.id ? String(batch.plan.id) : null,
            effectivePeriod: batch.effectivePeriod || null,
            effectiveDate: batch.effectiveDate || null,
            masterAccountId: String(account.masterAccountId),
            accountName: account.accountName || String(account.masterAccountId),
            customerName: buildCustomerName(primaryContact, account.accountName || String(account.masterAccountId)),
            email: normalizeEmail(primaryContact?.email),
            services: (account.subscriptions || []).map(buildServiceRow),
        };
    });
}

export async function fetchPostPushNotificationTargets({ client, queueIds = null }) {
    const RCP = getRCP(client);
    const useQueueFilter = Array.isArray(queueIds) && queueIds.length > 0;

    const accountRows = await runQuery(`
        WITH queue_accounts AS (
            SELECT
                q.id AS queue_id,
                q.plan_id,
                q.master_account_id,
                q.effective_date,
                COALESCE(am.account_display_name, q.master_account_id) AS account_name,
                am.root_customer_id,
                COALESCE(am.customer_ids, ARRAY<STRING>[]) AS customer_ids
            FROM ${INPUTS.pricePushQueue} q
            LEFT JOIN ${RCP.accountMaster} am
                ON q.master_account_id = am.master_account_id AND q.client = am.client
            WHERE q.client = @client
              AND q.status = 'pushed'
              AND (@useQueueFilter = FALSE OR q.id IN UNNEST(@queueIds))
        ),
        customer_emails AS (
            SELECT
                qa.queue_id,
                c.email,
                COALESCE(NULLIF(TRIM(f.billing_first_name), ''), c.first_name) AS first_name,
                c.last_name,
                c.company_name,
                c.customer_id,
                ROW_NUMBER() OVER (
                    PARTITION BY qa.queue_id
                    ORDER BY
                        CASE WHEN c.email IS NOT NULL AND TRIM(c.email) != '' THEN 0 ELSE 1 END,
                        c.customer_id
                ) AS rn
            FROM queue_accounts qa
            LEFT JOIN UNNEST(
                CASE
                    WHEN ARRAY_LENGTH(qa.customer_ids) > 0 THEN qa.customer_ids
                    WHEN qa.root_customer_id IS NOT NULL THEN [CAST(qa.root_customer_id AS STRING)]
                    ELSE ARRAY<STRING>[]
                END
            ) AS customer_id
                ON TRUE
            LEFT JOIN ${SHARED.curCustomer} c
                ON customer_id = c.customer_id AND @client = c.client
            LEFT JOIN ${SHARED.normCustomerFieldroutes} n
                ON CAST(n.customer_id AS STRING) = CAST(c.customer_id AS STRING)
               AND n.client = c.client
            LEFT JOIN (
                SELECT
                    CAST(CUSTOMER_ID AS STRING) AS customer_id,
                    CAST(OFFICE_ID AS STRING) AS office_id,
                    MAX(NULLIF(TRIM(BILLING_FIRST_NAME), '')) AS billing_first_name
                FROM ${SHARED.rawCustomer}
                GROUP BY customer_id, office_id
            ) f
                ON f.customer_id = CAST(c.customer_id AS STRING)
               AND f.office_id = CAST(n.office_id AS STRING)
        )
        SELECT
            qa.queue_id,
            qa.plan_id,
            qa.master_account_id,
            qa.effective_date,
            qa.account_name,
            ce.email,
            ce.first_name,
            ce.last_name,
            ce.company_name
        FROM queue_accounts qa
        LEFT JOIN customer_emails ce
            ON qa.queue_id = ce.queue_id AND ce.rn = 1
        ORDER BY qa.account_name
    `, {
        client,
        queueIds: queueIds || [],
        useQueueFilter,
    }, 'price-increase-notify-postpush-accounts');

    const serviceRows = await runQuery(`
        SELECT
            d.queue_id,
            d.service_type_name,
            d.old_service_charge AS current_price,
            d.new_service_charge AS new_price,
            d.calculated_increase_pct AS increase_pct,
            SAFE_CAST(cs.billing_frequency AS INT64) AS billing_frequency,
            cs.services_per_year,
            sm.pricing_tier
        FROM ${INPUTS.pricePushSubscriptionDetail} d
        INNER JOIN ${INPUTS.pricePushQueue} q
            ON d.queue_id = q.id AND d.client = q.client
        LEFT JOIN ${SHARED.curSubscription} cs
            ON CAST(d.subscription_id AS STRING) = CAST(cs.subscription_id AS STRING)
            AND d.client = cs.client
        LEFT JOIN ${RCP.subscriptionMaster} sm
            ON CAST(d.subscription_id AS STRING) = CAST(sm.subscription_id AS STRING)
            AND d.client = sm.client
        WHERE d.client = @client
          AND d.status = 'pushed'
          AND q.status = 'pushed'
          AND (@useQueueFilter = FALSE OR d.queue_id IN UNNEST(@queueIds))
          AND NOT COALESCE(d.is_excluded, FALSE)
        ORDER BY d.queue_id, d.service_type_name
    `, {
        client,
        queueIds: queueIds || [],
        useQueueFilter,
    }, 'price-increase-notify-postpush-services');

    const servicesByQueueId = new Map();
    for (const row of serviceRows) {
        const queueId = String(row.queue_id);
        if (!servicesByQueueId.has(queueId)) {
            servicesByQueueId.set(queueId, []);
        }
        servicesByQueueId.get(queueId).push(buildServiceRow({
            serviceTypeName: row.service_type_name,
            currentPrice: row.current_price,
            newPrice: row.new_price,
            increasePct: row.increase_pct,
            billing_frequency: row.billing_frequency,
            services_per_year: row.services_per_year,
            pricing_tier: row.pricing_tier,
        }));
    }

    return accountRows.map((row) => {
        const queueId = String(row.queue_id);
        const customerName = buildCustomerName(row, row.account_name || String(row.master_account_id));
        return {
            mode: POST_PUSH_MODE,
            selectionId: queueId,
            dedupKey: `${POST_PUSH_MODE}:${queueId}`,
            queueId,
            planId: row.plan_id ? String(row.plan_id) : null,
            effectivePeriod: null,
            effectiveDate: row.effective_date || null,
            masterAccountId: String(row.master_account_id),
            accountName: row.account_name || String(row.master_account_id),
            customerName,
            email: normalizeEmail(row.email),
            services: servicesByQueueId.get(queueId) || [],
        };
    });
}

const fetchExcludedAccountTags = async (client, masterAccountIds, excludedTagKeys) => {
    if (!excludedTagKeys || excludedTagKeys.length === 0) {
        return new Map();
    }
    const dedupedIds = [...new Set((masterAccountIds || []).map((id) => String(id || '').trim()).filter(Boolean))];
    if (dedupedIds.length === 0) {
        return new Map();
    }

    const result = await pgQuery(`
        SELECT master_account_id, tag_type_key
        FROM inp_account_tags
        WHERE client = $1
          AND master_account_id = ANY($2::text[])
          AND tag_type_key = ANY($3::text[])
          AND is_current = TRUE
          AND (expires_at IS NULL OR expires_at > NOW())
    `, [client, dedupedIds, excludedTagKeys]);

    const byAccount = new Map();
    for (const row of result.rows) {
        const id = String(row.master_account_id);
        if (!byAccount.has(id)) byAccount.set(id, []);
        byAccount.get(id).push(row.tag_type_key);
    }
    return byAccount;
};

const fetchExcludedTagBreakdown = async (client, planId, excludedTagKeys) => {
    if (!excludedTagKeys || excludedTagKeys.length === 0 || !planId) {
        return Object.fromEntries((excludedTagKeys || []).map((k) => [k, { total: 0, byLocation: {} }]));
    }

    const result = await pgQuery(`
        SELECT
            t.tag_type_key,
            CASE
                WHEN pad.plan_id IS NULL THEN 'not_in_plan'
                WHEN pad.is_ghost THEN 'ghost'
                WHEN pad.effective_period IS NOT NULL THEN pad.effective_period
                WHEN pad.review_tab = 'always_manual' THEN 'always_manual'
                ELSE 'unscheduled'
            END AS location,
            COUNT(DISTINCT t.master_account_id)::int AS n
        FROM inp_account_tags t
        LEFT JOIN ${PLAN_TABLES.accountDecision} pad
            ON pad.master_account_id = t.master_account_id
           AND pad.plan_id = $1
        WHERE t.client = $2
          AND t.tag_type_key = ANY($3::text[])
          AND t.is_current = TRUE
          AND (t.expires_at IS NULL OR t.expires_at > NOW())
        GROUP BY t.tag_type_key, location
    `, [planId, client, excludedTagKeys]);

    const breakdown = Object.fromEntries(excludedTagKeys.map((k) => [k, { total: 0, byLocation: {} }]));
    for (const row of result.rows) {
        if (!breakdown[row.tag_type_key]) {
            breakdown[row.tag_type_key] = { total: 0, byLocation: {} };
        }
        breakdown[row.tag_type_key].byLocation[row.location] = row.n;
        breakdown[row.tag_type_key].total += row.n;
    }
    return breakdown;
};

const fetchUnsubscribedEmails = async (client, emails) => {
    const normalizedEmails = [...new Set((emails || []).map(normalizeEmail).filter(Boolean))];
    if (normalizedEmails.length === 0) {
        return new Set();
    }

    const rows = await runQuery(`
        SELECT LOWER(TRIM(email)) AS email
        FROM ${INPUTS.emailUnsubscribes}
        WHERE client = @client
          AND LOWER(TRIM(email)) IN UNNEST(@emails)
          AND email_type IN ('price_increase', 'all')
          AND is_active = TRUE
    `, { client, emails: normalizedEmails }, 'price-increase-notify-unsubscribes');

    return new Set(rows.map((row) => normalizeEmail(row.email)).filter(Boolean));
};

const fetchAlreadySentKeys = async (client, mode, targets) => {
    if (!targets.length) return new Set();

    const keys = new Set();

    if (mode === POST_PUSH_MODE) {
        const queueIds = [...new Set(targets.map((target) => String(target.queueId || '')).filter(Boolean))];
        if (queueIds.length > 0) {
            const [legacyRows, eventRows] = await Promise.all([
                runQuery(`
                    SELECT queue_id
                    FROM ${INPUTS.priceIncreaseNotifications}
                    WHERE client = @client
                      AND status = 'sent'
                      AND queue_id IN UNNEST(@queueIds)
                `, { client, queueIds }, 'price-increase-notify-legacy-dupes'),
                runQuery(`
                    SELECT queue_id
                    FROM ${INPUTS.priceIncreaseNotificationEvents}
                    WHERE client = @client
                      AND mode = '${POST_PUSH_MODE}'
                      AND status = 'sent'
                      AND queue_id IN UNNEST(@queueIds)
                `, { client, queueIds }, 'price-increase-notify-event-dupes-post'),
            ]);

            for (const row of [...legacyRows, ...eventRows]) {
                if (row.queue_id) {
                    keys.add(`${POST_PUSH_MODE}:${row.queue_id}`);
                }
            }
        }

        return keys;
    }

    const planId = targets[0]?.planId || null;
    const effectivePeriod = targets[0]?.effectivePeriod || null;
    const masterAccountIds = [...new Set(targets.map((target) => String(target.masterAccountId || '')).filter(Boolean))];

    if (!planId || !effectivePeriod || masterAccountIds.length === 0) {
        return keys;
    }

    const rows = await runQuery(`
        SELECT master_account_id
        FROM ${INPUTS.priceIncreaseNotificationEvents}
        WHERE client = @client
          AND mode = '${PRE_PUSH_MODE}'
          AND status = 'sent'
          AND plan_id = @planId
          AND effective_period = @effectivePeriod
          AND master_account_id IN UNNEST(@masterAccountIds)
    `, {
        client,
        planId,
        effectivePeriod,
        masterAccountIds,
    }, 'price-increase-notify-event-dupes-pre');

    for (const row of rows) {
        if (row.master_account_id) {
            keys.add(`${PRE_PUSH_MODE}:${planId}:${effectivePeriod}:${row.master_account_id}`);
        }
    }

    return keys;
};

export async function annotateNotificationEligibility({ client, mode, targets, excludedTagKeys = [], skipAlreadySent = false }) {
    const [unsubscribedEmails, alreadySentKeys, excludedTagsByAccount] = await Promise.all([
        fetchUnsubscribedEmails(client, targets.map((target) => target.email)),
        // skipAlreadySent — operator correction re-send: bypass the already-sent
        // dedup so recipients who already got the original are notified again.
        skipAlreadySent ? Promise.resolve(new Set()) : fetchAlreadySentKeys(client, mode, targets),
        fetchExcludedAccountTags(client, targets.map((target) => target.masterAccountId), excludedTagKeys),
    ]);

    const withEligibility = targets.map((target) => {
        const email = normalizeEmail(target.email);
        const matchedTags = excludedTagsByAccount.get(String(target.masterAccountId)) || [];
        let eligibility = 'eligible';
        if (matchedTags.length > 0) {
            eligibility = 'excluded_tag';
        } else if (!email || !isValidEmail(email)) {
            eligibility = 'no_email';
        } else if (unsubscribedEmails.has(email)) {
            eligibility = 'unsubscribed';
        } else if (alreadySentKeys.has(buildDedupKey(target))) {
            eligibility = 'already_sent';
        }

        return {
            ...target,
            selectionId: buildSelectionId(target),
            dedupKey: buildDedupKey(target),
            email,
            eligibility,
            excludedByTags: matchedTags,
        };
    });

    return {
        targets: withEligibility,
        summary: buildNotificationSummary(withEligibility),
    };
}

export async function fetchNotificationConfig(client) {
    const rows = await runQuery(`
        SELECT setting_key, value_string
        FROM ${INPUTS.clientSettings}
        WHERE client = @client
          AND setting_key LIKE 'notification_%'
          AND is_current = TRUE
    `, { client }, 'price-increase-notify-config');

    const settings = new Map(rows.map((row) => [row.setting_key, row.value_string]));
    const customDomain = settings.get('notification_from_domain')?.trim();
    const customAddress = settings.get('notification_from_address')?.trim();
    const customName = settings.get('notification_from_name')?.trim();

    let fromEmail = null;
    if (customAddress) {
        fromEmail = customAddress.includes('@')
            ? customAddress
            : `${customAddress}@${customDomain || process.env.MAILERSEND_FROM_DOMAIN || 'pestnotifications.com'}`;
    }

    // Reply-to: client's own verified address when present; otherwise central fallback.
    const replyToFallback = process.env.NOTIFICATION_REPLY_TO_FALLBACK || 'nate@pestnotifications.com';
    const replyTo = customAddress && customAddress.includes('@') ? customAddress : replyToFallback;

    // Build templateConfig from all notification_* settings
    const templateConfig = {};
    for (const [key, value] of settings) {
        if (value != null) templateConfig[key] = value;
    }

    // Excluded tags come from inp_tag_handling_rules (Cloud SQL) — the
    // client-ops-pilot routing-rules system. Any tag whose effective routing
    // for this client is 'physical_mail' or 'no_notification' is excluded
    // from email send. Client-specific rules override global rules.
    //
    // Historical: this used to read the per-client `notification_excluded_tags`
    // setting from inp_client_settings; that was replaced after migration 133
    // so the email and physical-mail routing share one source of truth.
    const excludedTagKeys = await fetchExcludedTagKeysForClient(client);

    return {
        fromEmail,
        fromName: customName || null,
        replyTo,
        templateConfig, // All notification_* settings as a flat object
        excludedTagKeys,
    };
}

/**
 * Resolve the effective set of tag keys that exclude an account from email
 * send for this client, by reading inp_tag_handling_rules.
 *
 * Routing precedence (mirrors client-ops-pilot's tag-handling-rules read):
 *   1. client-specific rule (client = @client AND is_current)
 *   2. global rule        (client IS NULL AND is_current)
 *   3. default 'email'    (no rule → not excluded)
 *
 * A key is excluded when its effective routing is 'physical_mail' or
 * 'no_notification'.
 */
async function fetchExcludedTagKeysForClient(client) {
    const result = await pgQuery(`
        WITH client_rules AS (
            SELECT tag_type_key, notification_routing
              FROM inp_tag_handling_rules
             WHERE client = $1 AND is_current = TRUE
        ),
        global_rules AS (
            SELECT tag_type_key, notification_routing
              FROM inp_tag_handling_rules
             WHERE client IS NULL AND is_current = TRUE
        ),
        effective AS (
            SELECT COALESCE(cr.tag_type_key, gr.tag_type_key) AS tag_type_key,
                   COALESCE(cr.notification_routing, gr.notification_routing) AS notification_routing
              FROM client_rules cr
              FULL OUTER JOIN global_rules gr USING (tag_type_key)
        )
        SELECT DISTINCT tag_type_key
          FROM effective
         WHERE notification_routing IN ('physical_mail', 'no_notification')
    `, [client]);
    return result.rows.map((row) => row.tag_type_key);
}

// Legacy alias for backward compatibility
export const fetchNotificationSenderConfig = fetchNotificationConfig;

const buildEventInsertSql = (rows) => rows.map((row) => (
    `(${toSqlString(row.id)}, ${toSqlString(row.client)}, ${toSqlString(row.mode)}, ${toSqlString(row.plan_id)}, ` +
    `${toSqlString(row.effective_period)}, ${toSqlDate(row.effective_date)}, ${toSqlString(row.queue_id)}, ` +
    `${toSqlString(row.master_account_id)}, ${toSqlString(row.account_name)}, ${toSqlString(row.recipient_email)}, ` +
    `${toSqlString(row.recipient_name)}, ${toSqlString(row.status)}, ${toSqlString(row.error_message)}, ` +
    `${toSqlString(row.mailersend_message_id)}, ${toSqlNumber(row.service_count)}, ${toSqlString(row.sent_by)}, CURRENT_TIMESTAMP())`
)).join(',\n');

const buildLegacyInsertSql = (rows) => rows.map((row) => (
    `(${toSqlString(row.id)}, ${toSqlString(row.client)}, ${toSqlString(row.queue_id)}, ${toSqlString(row.master_account_id)}, ` +
    `${toSqlString(row.recipient_email)}, ${toSqlString(row.recipient_name)}, ${toSqlString(row.status)}, ` +
    `${toSqlString(row.error_message)}, ${toSqlString(row.mailersend_message_id)}, ${toSqlNumber(row.subscription_count)}, ` +
    `${toSqlString(row.sent_by)}, CURRENT_TIMESTAMP())`
)).join(',\n');

export async function sendNotificationTargets({
    client,
    mode,
    targets,
    selectedIds,
    sentBy = 'user',
    baseUrl,
    senderConfig = {},
    testRecipient = null, // When set, overrides the real recipient email for test sends
    existingRunId = null, // When set, reuse this app-created notification_run
                          // instead of opening a fresh one (NOTIFICATION_RUN_ID).
}) {
    const selectionSet = new Set((selectedIds || []).map((id) => String(id)));
    const selectedTargets = targets.filter((target) => selectionSet.has(String(target.selectionId)));

    const results = {
        success: true,
        sent: 0,
        failed: 0,
        skippedNoEmail: 0,
        skippedUnsubscribed: 0,
        skippedAlreadySent: 0,
        skippedExcludedTag: 0,
        total: selectedTargets.length,
        details: [],
    };

    const eventRows = [];
    const legacyRows = [];

    // Open (or reuse) a notification_run + capture per-recipient outcomes.
    // Goal: every send (and every skip) produces (a) a notification_recipient
    // PG row + (b) a notification_send_event BQ row with the rendered email
    // payload. The trace UI joins the two via (bq_payload_dataset, row_id).
    //
    // When existingRunId is set (NOTIFICATION_RUN_ID — the client-ops-pilot
    // /launch/notify batch trigger), the run row was already created by the
    // app; reuse it and flip it pending → sending. Otherwise open a fresh run.
    //
    // Graceful degradation: if office_notification_config isn't backfilled,
    // skip the new audit and continue with the legacy path.
    let notificationRunId = null;
    let notificationConfigVersionId = null;
    const firstTarget = selectedTargets[0] ?? null;
    const planIdForRun = firstTarget?.planId ?? null;
    const effectivePeriodForRun = firstTarget?.effectivePeriod ?? '';
    if (existingRunId) {
        // App-created batch: bind to the run id up front so even a config-load
        // failure can't strand the run unclosed. closeNotificationRun finalizes
        // it at the end of this function.
        notificationRunId = Number(existingRunId);
        try {
            await markNotificationRunSending(notificationRunId);
        } catch (err) {
            logger.warn(
                { err: err?.message || String(err), notification_run_id: notificationRunId },
                '[notification] could not flip reused run to sending',
            );
        }
        try {
            notificationConfigVersionId = (await loadActiveNotificationConfig(client)).versionId;
        } catch (err) {
            logger.warn(
                { err: err?.message || String(err), office_key: client },
                '[notification] config load failed for reused run; decision_event config_version_id falls back to 0',
            );
        }
        logger.info(
            { office_key: client, mode, notification_run_id: notificationRunId, notification_config_version_id: notificationConfigVersionId, total: selectedTargets.length },
            '[notification] reusing app-created run (NOTIFICATION_RUN_ID)',
        );
    } else if (planIdForRun) {
        try {
            const notifConfig = await loadActiveNotificationConfig(client);
            notificationConfigVersionId = notifConfig.versionId;
            const opened = await openNotificationRun({
                planId:           Number(planIdForRun),
                officeKey:        client,
                mode,
                effectivePeriod:  effectivePeriodForRun,
                configVersionId:  notifConfig.versionId,
                triggeredBy:      sentBy,
            });
            notificationRunId = opened.id;
            logger.info(
                { office_key: client, plan_id: planIdForRun, mode, notification_run_id: notificationRunId, notification_config_version_id: notificationConfigVersionId, total: selectedTargets.length },
                '[notification] run opened',
            );
        } catch (err) {
            logger.warn(
                { err: err?.message || String(err), office_key: client, plan_id: planIdForRun },
                '[notification] no office_notification_config; skipping new audit (legacy path continues)',
            );
        }
    }

    const recordRecipient = async ({ target, email, status, statusNote }) => {
        if (!notificationRunId) return null;
        try {
            const row = await insertNotificationRecipient({
                notificationRunId,
                masterAccountId: target.masterAccountId,
                recipientEmail:  email ?? '(missing)',
                status,
                statusNote: statusNote ?? null,
            });
            return row.id;
        } catch (err) {
            logger.warn(
                { err: err?.message || String(err), office_key: client, master_account_id: target.masterAccountId, status },
                '[notification] failed to insert notification_recipient',
            );
            return null;
        }
    };

    let auditSucceeded = 0;
    let auditFailed = 0;
    let auditSuppressed = 0;

    for (const target of selectedTargets) {
        const detail = {
            selectionId: target.selectionId,
            queueId: target.queueId,
            masterAccountId: target.masterAccountId,
            accountName: target.accountName,
        };

        if (target.eligibility === 'no_email') {
            results.skippedNoEmail++;
            results.details.push({ ...detail, status: 'skipped_no_email', email: target.email || undefined });
            eventRows.push({
                id: randomUUID(),
                client,
                mode,
                plan_id: target.planId,
                effective_period: target.effectivePeriod,
                effective_date: target.effectiveDate,
                queue_id: target.queueId,
                master_account_id: target.masterAccountId,
                account_name: target.accountName,
                recipient_email: target.email,
                recipient_name: target.customerName,
                status: 'skipped_no_email',
                error_message: null,
                mailersend_message_id: null,
                service_count: target.services.length,
                sent_by: sentBy,
            });
            await recordRecipient({ target, email: target.email, status: 'suppressed_no_email' });
            auditSuppressed++;
            continue;
        }

        if (target.eligibility === 'excluded_tag') {
            results.skippedExcludedTag++;
            results.details.push({ ...detail, status: 'skipped_excluded_tag', email: target.email || undefined });
            eventRows.push({
                id: randomUUID(),
                client,
                mode,
                plan_id: target.planId,
                effective_period: target.effectivePeriod,
                effective_date: target.effectiveDate,
                queue_id: target.queueId,
                master_account_id: target.masterAccountId,
                account_name: target.accountName,
                recipient_email: target.email,
                recipient_name: target.customerName,
                status: 'skipped_excluded_tag',
                error_message: null,
                mailersend_message_id: null,
                service_count: target.services.length,
                sent_by: sentBy,
            });
            await recordRecipient({
                target,
                email: target.email,
                status: 'suppressed_unsubscribed',
                statusNote: 'excluded_tag',
            });
            auditSuppressed++;
            continue;
        }

        if (target.eligibility === 'unsubscribed') {
            results.skippedUnsubscribed++;
            results.details.push({ ...detail, status: 'skipped_unsubscribed', email: target.email || undefined });
            eventRows.push({
                id: randomUUID(),
                client,
                mode,
                plan_id: target.planId,
                effective_period: target.effectivePeriod,
                effective_date: target.effectiveDate,
                queue_id: target.queueId,
                master_account_id: target.masterAccountId,
                account_name: target.accountName,
                recipient_email: target.email,
                recipient_name: target.customerName,
                status: 'skipped_unsubscribed',
                error_message: null,
                mailersend_message_id: null,
                service_count: target.services.length,
                sent_by: sentBy,
            });
            await recordRecipient({ target, email: target.email, status: 'suppressed_unsubscribed' });
            auditSuppressed++;
            continue;
        }

        if (target.eligibility === 'already_sent') {
            results.skippedAlreadySent++;
            results.details.push({ ...detail, status: 'skipped_already_sent', email: target.email || undefined });
            eventRows.push({
                id: randomUUID(),
                client,
                mode,
                plan_id: target.planId,
                effective_period: target.effectivePeriod,
                effective_date: target.effectiveDate,
                queue_id: target.queueId,
                master_account_id: target.masterAccountId,
                account_name: target.accountName,
                recipient_email: target.email,
                recipient_name: target.customerName,
                status: 'skipped_already_sent',
                error_message: null,
                mailersend_message_id: null,
                service_count: target.services.length,
                sent_by: sentBy,
            });
            await recordRecipient({ target, email: target.email, status: 'deduped_already_sent' });
            auditSuppressed++;
            continue;
        }

        // Skip if no services or all services have zero increase
        const hasNonZeroIncrease = target.services.some(s => (Number(s.increaseAmount) || 0) > 0);
        if (!target.services.length || !hasNonZeroIncrease) {
            results.failed++;
            const reason = !target.services.length ? 'No services available for notification' : 'All services have zero increase';
            results.details.push({ ...detail, status: 'failed', email: target.email || undefined, error: reason });
            eventRows.push({
                id: randomUUID(),
                client,
                mode,
                plan_id: target.planId,
                effective_period: target.effectivePeriod,
                effective_date: target.effectiveDate,
                queue_id: target.queueId,
                master_account_id: target.masterAccountId,
                account_name: target.accountName,
                recipient_email: target.email,
                recipient_name: target.customerName,
                status: 'failed',
                error_message: reason,
                mailersend_message_id: null,
                service_count: target.services.length,
                sent_by: sentBy,
            });
            continue;
        }

        const unsubToken = Buffer
            .from(JSON.stringify({ client, email: target.email, masterAccountId: target.masterAccountId }))
            .toString('base64');
        const unsubscribeUrl = `${baseUrl}/api/repricing/price-push/unsubscribe?token=${encodeURIComponent(unsubToken)}`;

        const actualRecipientEmail = testRecipient || target.email;

        // Record-as-queued before the send so a crash mid-batch leaves a
        // visible "we got this far" audit row instead of a missing record.
        const notificationRecipientId = await recordRecipient({
            target,
            email: actualRecipientEmail,
            status: 'queued',
        });

        const sendResult = await sendPriceIncreaseEmail({
            recipient: actualRecipientEmail,
            recipientName: target.customerName,
            customerName: target.customerName,
            accountName: target.accountName || target.masterAccountId,
            clientName: client,
            effectiveDate: target.effectiveDate,
            effectivePeriod: target.effectivePeriod,
            services: target.services,
            unsubscribeUrl,
            ...(senderConfig.fromEmail ? { fromEmail: senderConfig.fromEmail } : {}),
            ...(senderConfig.fromName ? { fromName: senderConfig.fromName } : {}),
            ...(senderConfig.replyTo ? { replyTo: senderConfig.replyTo } : {}),
            ...(senderConfig.templateConfig ? { templateConfig: senderConfig.templateConfig } : {}),
        });

        // When test recipient is used, log as 'test' so the already_sent
        // guard doesn't block the real send later.
        const statusLabel = sendResult.success ? (testRecipient ? 'test' : 'sent') : 'failed';
        const errorMessage = sendResult.success ? null : (sendResult.error || 'Unknown error');
        const messageId = sendResult.success ? (sendResult.messageId || null) : null;

        if (sendResult.success) {
            results.sent++;
            results.details.push({ ...detail, status: statusLabel, email: actualRecipientEmail || undefined, testMode: !!testRecipient });
            auditSucceeded++;
        } else {
            results.failed++;
            results.details.push({ ...detail, status: statusLabel, email: actualRecipientEmail || undefined, error: errorMessage });
            auditFailed++;
        }

        // Capture rendered payload to BQ + update PG outcome row.
        if (notificationRunId && notificationRecipientId) {
            const sentAt = new Date();
            const rendered = sendResult.rendered ?? null;
            const bqPayloadDataset = `rcp_${client}`;
            const bqPayloadRowId = `${notificationRunId}:${notificationRecipientId}`;

            if (sendResult.success && rendered) {
                try {
                    await emitNotificationSendEvent({
                        notification_run_id:        notificationRunId,
                        notification_recipient_id:  notificationRecipientId,
                        office_key:                 client,
                        master_account_id:          target.masterAccountId,
                        recipient_email:            actualRecipientEmail,
                        provider:                   'mailersend',
                        provider_message_id:        messageId,
                        subject:                    rendered.subject,
                        html_body:                  rendered.htmlBody,
                        text_body:                  rendered.textBody,
                        template_id:                senderConfig?.templateConfig?.notification_template_id ?? null,
                        template_variables:         rendered.templateVariables,
                        provider_request: {
                            from:        rendered.senderEmail,
                            from_name:   rendered.senderName,
                            to:          rendered.recipient,
                            cc:          rendered.cc,
                            bcc:         rendered.bcc,
                            reply_to:    rendered.replyToEmail,
                            mode:        rendered.mode,
                            unsubscribe_url: unsubscribeUrl,
                        },
                        provider_response: {
                            message_id:     messageId,
                            mock:           Boolean(sendResult.mock),
                            cc_bcc_failed:  Boolean(sendResult.ccBccFailed),
                        },
                        sent_at: sentAt,
                    });
                } catch (bqErr) {
                    logger.warn(
                        { err: bqErr?.message || String(bqErr), office_key: client, master_account_id: target.masterAccountId, notification_run_id: notificationRunId },
                        '[notification] notification_send_event BQ insert failed; PG audit row still recorded',
                    );
                }
            }

            await updateNotificationRecipientOutcome({
                id: notificationRecipientId,
                status: sendResult.success ? (testRecipient ? 'test' : 'sent') : 'failed',
                statusNote: errorMessage,
                providerMessageId: messageId,
                bqPayloadDataset: sendResult.success ? bqPayloadDataset : null,
                bqPayloadRowId:   sendResult.success ? bqPayloadRowId : null,
                sentAt: sendResult.success ? sentAt : null,
            }).catch((updErr) => {
                logger.warn(
                    { err: updErr?.message || String(updErr), notification_recipient_id: notificationRecipientId },
                    '[notification] notification_recipient outcome update failed',
                );
            });

            // Step 8b: pre-push real sends advance journey state to 'notified'.
            // Skip for test sends (testRecipient) and post-push (already past notified).
            if (sendResult.success && !testRecipient && mode === PRE_PUSH_MODE && planIdForRun) {
                try {
                    await transitionJourneyState({
                        planId: Number(planIdForRun),
                        masterAccountId: target.masterAccountId,
                        to: 'notified',
                        enteredBy: sentBy,
                        note: `notification_run=${notificationRunId}`,
                        allowInsert: true,
                    });
                } catch (jsErr) {
                    logger.warn(
                        { err: jsErr?.message || String(jsErr), plan_id: planIdForRun, master_account_id: target.masterAccountId },
                        '[notification] journey-state transition to notified failed',
                    );
                }
            }
        }

        const eventRow = {
            id: randomUUID(),
            client,
            mode: testRecipient ? `test_${mode}` : mode,
            plan_id: target.planId,
            effective_period: target.effectivePeriod,
            effective_date: target.effectiveDate,
            queue_id: target.queueId,
            master_account_id: target.masterAccountId,
            account_name: target.accountName,
            recipient_email: actualRecipientEmail,
            recipient_name: target.customerName,
            status: statusLabel,
            error_message: errorMessage,
            mailersend_message_id: messageId,
            service_count: target.services.length,
            sent_by: sentBy,
        };
        eventRows.push(eventRow);

        if (mode === POST_PUSH_MODE && target.queueId) {
            legacyRows.push({
                id: randomUUID(),
                client,
                queue_id: target.queueId,
                master_account_id: target.masterAccountId,
                recipient_email: target.email,
                recipient_name: target.customerName,
                status: statusLabel,
                error_message: errorMessage,
                mailersend_message_id: messageId,
                subscription_count: target.services.length,
                sent_by: sentBy,
            });
        }
    }

    if (eventRows.length > 0) {
        await runQuery(`
            INSERT INTO ${INPUTS.priceIncreaseNotificationEvents}
                (id, client, mode, plan_id, effective_period, effective_date, queue_id, master_account_id, account_name,
                 recipient_email, recipient_name, status, error_message, mailersend_message_id, service_count, sent_by, created_at)
            VALUES ${buildEventInsertSql(eventRows)}
        `, {}, 'price-increase-notify-insert-events');
    }

    // ── decision_event phase='notification' — one event per account ──────
    // Every recipient processed in the loop above produced exactly one
    // eventRow; fan those into the unified decision_event taxonomy so the
    // trace UI surfaces notification outcomes per account. Non-blocking:
    // a BQ failure here is logged, never aborts the send run.
    if (eventRows.length > 0) {
        const evaluatorRunId = randomUUID();
        const evaluatedAt = new Date();
        const decisionRows = eventRows
            .map((row) => {
                // eventRow.status for real sends is 'sent'/'test'/'failed';
                // for suppressions it's 'skipped_*'. Map to a decision_code.
                const code = NOTIFICATION_DECISION_CODE[row.status] || null;
                if (!code) return null;
                if (!row.master_account_id) return null;
                return {
                    plan_id:           row.plan_id != null ? Number(row.plan_id) : null,
                    office_key:        client,
                    master_account_id: String(row.master_account_id),
                    subscription_id:   null,
                    phase:             'notification',
                    rule_id:           'notification_send',
                    decision_code:     code,
                    decision_reason:   row.error_message || null,
                    inputs: {
                        mode:                  row.mode,
                        effective_period:      row.effective_period ?? null,
                        recipient_email:       row.recipient_email ?? null,
                        service_count:         row.service_count ?? null,
                        mailersend_message_id: row.mailersend_message_id ?? null,
                    },
                    outputs:           null,
                    // config_version_id is a REQUIRED int in the emit schema;
                    // fall back to 0 when this office has no notification
                    // config row (legacy path).
                    config_version_id: Number(notificationConfigVersionId) || 0,
                    evaluated_at:      evaluatedAt,
                    evaluator_run_id:  evaluatorRunId,
                    emitted_by:        EMITTED_BY.NOTIFICATION_SERVICE,
                };
            })
            .filter(Boolean);
        if (decisionRows.length > 0) {
            try {
                await emitDecisionEventsBatch(decisionRows);
            } catch (deErr) {
                logger.warn(
                    { err: deErr?.message || String(deErr), office_key: client, count: decisionRows.length },
                    '[notification] decision_event emit failed; per-account audit (notification_recipient + inp_price_increase_notification_events) still recorded',
                );
            }
        }
    }

    if (legacyRows.length > 0) {
        await runQuery(`
            INSERT INTO ${INPUTS.priceIncreaseNotifications}
                (id, client, queue_id, master_account_id, recipient_email, recipient_name, status, error_message, mailersend_message_id, subscription_count, sent_by, created_at)
            VALUES ${buildLegacyInsertSql(legacyRows)}
        `, {}, 'price-increase-notify-insert-legacy');
    }

    if (notificationRunId) {
        try {
            await closeNotificationRun({
                id:               notificationRunId,
                totalRecipients:  selectedTargets.length,
                succeededCount:   auditSucceeded,
                failedCount:      auditFailed,
                suppressedCount:  auditSuppressed,
            });
            logger.info(
                {
                    notification_run_id: notificationRunId,
                    notification_config_version_id: notificationConfigVersionId,
                    total: selectedTargets.length,
                    succeeded: auditSucceeded,
                    failed: auditFailed,
                    suppressed: auditSuppressed,
                },
                '[notification] run closed',
            );
        } catch (closeErr) {
            logger.warn(
                { err: closeErr?.message || String(closeErr), notification_run_id: notificationRunId },
                '[notification] run close failed; counts may be inaccurate',
            );
        }
    }

    return results;
}

export async function findDuePrePushNotificationPeriods({ targetDate = null, clients = null, ignoreDueDate = false } = {}) {
    const normalizedTargetDate = normalizeDateOnly(targetDate) || new Date().toISOString().slice(0, 10);
    const requestedClients = Array.isArray(clients)
        ? [...new Set(clients.map((client) => String(client || '').trim()).filter(Boolean))]
        : [];

    const params = [PLAN_V2_PUSH_REVIEW_TABS];
    let clientFilterSql = '';
    if (requestedClients.length > 0) {
        params.push(requestedClients);
        clientFilterSql = ` AND company_key = ANY($${params.length}::text[])`;
    }

    const result = await pgQuery(`
        WITH latest_published AS (
            SELECT DISTINCT ON (company_key)
                id,
                company_key,
                published_at
            FROM ${PLAN_TABLES.plan}
            WHERE status = 'published'
            ${clientFilterSql}
            ORDER BY company_key, published_at DESC NULLS LAST, id DESC
        ),
        eligible_periods AS (
            SELECT
                lp.id AS plan_id,
                lp.company_key AS client,
                ad.effective_period,
                COUNT(*)::int AS account_count
            FROM latest_published lp
            INNER JOIN ${PLAN_TABLES.accountDecision} ad
                ON ad.plan_id = lp.id
            LEFT JOIN ${PLAN_TABLES.clientResponse} account_skip
                ON account_skip.plan_id = ad.plan_id
               AND account_skip.client = lp.company_key
               AND account_skip.master_account_id = ad.master_account_id
               AND account_skip.subscription_id IS NULL
               AND account_skip.action = 'skip'
            WHERE ad.effective_period IS NOT NULL
              AND ad.is_ghost = FALSE
              AND ad.review_tab = ANY($1::text[])
              AND COALESCE(ad.override_increase_pct, ad.computed_increase_pct, 0) > 0
              AND account_skip.id IS NULL
            GROUP BY lp.id, lp.company_key, ad.effective_period
        )
        SELECT plan_id, client, effective_period, account_count
        FROM eligible_periods
        ORDER BY client, effective_period
    `, params);

    const periodRows = result.rows.map((row) => {
        const timing = deriveTimingFromPeriod(row.effective_period);
        return {
            planId: row.plan_id ? String(row.plan_id) : null,
            client: row.client,
            effectivePeriod: row.effective_period,
            accountCount: Number(row.account_count) || 0,
            noticeDate: timing.noticeDate,
            effectiveDate: timing.effectiveDate,
        };
    });

    // ignoreDueDate — app-triggered sends (NOTIFICATION_RUN_ID) bypass the
    // notice/effective-date window: the operator explicitly chose to send
    // this batch now. The scheduled cron keeps the date gate.
    if (ignoreDueDate) return periodRows;

    return periodRows.filter((row) => isDuePrePushNotificationPeriod({
        targetDate: normalizedTargetDate,
        noticeDate: row.noticeDate,
        effectiveDate: row.effectiveDate,
    }));
}

export async function runDuePrePushNotifications({
    targetDate = null,
    clients = null,
    baseUrl,
    sentBy = 'cloud_run_job',
    testRecipient = null,
    sendLimit = null,
    accountIds = null, // optional master_account_id allowlist — when set, only
                       // these accounts are notified. Powers the app-triggered
                       // cohort send (client-ops-pilot /launch/notify).
    existingRunId = null, // optional app-created notification_run id to reuse
                          // (NOTIFICATION_RUN_ID). Consumed by the first period
                          // that has eligible targets — the cohort filter keeps
                          // an app batch scoped to a single period anyway.
    preflight = null, // optional async ({ period, senderConfig, counts, sample, sanity }) => boolean
    resend = false,   // operator correction re-send: bypass BOTH the already-sent
                      // dedup and the notice/effective-date due window, so a batch
                      // that already went out can be re-sent (e.g. a corrected
                      // template). Off for the cron and all normal sends.
} = {}) {
    const normalizedTargetDate = normalizeDateOnly(targetDate) || new Date().toISOString().slice(0, 10);
    const trimmedBaseUrl = typeof baseUrl === 'string' ? baseUrl.trim().replace(/\/+$/, '') : '';
    if (!trimmedBaseUrl) {
        throw new Error('baseUrl is required for unsubscribe links');
    }

    const duePeriods = await findDuePrePushNotificationPeriods({
        targetDate: normalizedTargetDate,
        clients,
        // App-triggered batches (NOTIFICATION_RUN_ID) and operator correction
        // re-sends (resend) send the chosen period immediately — the cron's
        // notice-date gate doesn't apply.
        ignoreDueDate: existingRunId != null || resend,
    });

    // Cohort filter: when accountIds is a non-empty list, the run is scoped
    // to exactly those master accounts (app-triggered send). Empty/null =
    // the unrestricted scheduled-cron behavior.
    const accountIdSet = Array.isArray(accountIds) && accountIds.length > 0
        ? new Set(accountIds.map((id) => String(id).trim()).filter(Boolean))
        : null;

    // The app-created run is consumed once — by the first period that actually
    // sends. Cleared afterwards so any further period opens its own run.
    let runIdToReuse = existingRunId ? Number(existingRunId) : null;

    const summary = {
        targetDate: normalizedTargetDate,
        duePeriodCount: duePeriods.length,
        processedPeriodCount: 0,
        sent: 0,
        failed: 0,
        eligible: 0,
        noEmail: 0,
        unsubscribed: 0,
        alreadySent: 0,
        excludedTag: 0,
        periods: [],
        records: [],
    };

    for (const duePeriod of duePeriods) {
        const batch = await buildPlanV2PricePushSource({
            client: duePeriod.client,
            effectivePeriod: duePeriod.effectivePeriod,
        });

        if (!batch.plan || batch.summary.totalAccounts === 0) {
            summary.periods.push({
                ...duePeriod,
                status: 'skipped_empty_batch',
                batchAccountCount: batch.summary.totalAccounts,
                batchSubscriptionCount: batch.summary.totalSubscriptions,
            });
            continue;
        }

        const targets = await buildPrePushNotificationTargets({ client: duePeriod.client, batch });
        const senderConfig = await fetchNotificationConfig(duePeriod.client);
        const eligibility = await annotateNotificationEligibility({
            client: duePeriod.client,
            mode: PRE_PUSH_MODE,
            targets,
            excludedTagKeys: senderConfig.excludedTagKeys,
            skipAlreadySent: resend,
        });
        // eligibility.summary covers the whole period. When a cohort filter
        // (accountIds) is active, report counts scoped to the cohort — else
        // "eligible" reflects the entire period, not this batch.
        const reportSummary = accountIdSet
            ? eligibility.targets.reduce((acc, t) => {
                if (!accountIdSet.has(String(t.masterAccountId))) return acc;
                const bucket = {
                    eligible: 'eligible', no_email: 'noEmail', unsubscribed: 'unsubscribed',
                    already_sent: 'alreadySent', excluded_tag: 'excludedTag',
                }[t.eligibility];
                if (bucket) acc[bucket] += 1;
                return acc;
            }, { eligible: 0, noEmail: 0, unsubscribed: 0, alreadySent: 0, excludedTag: 0 })
            : eligibility.summary;
        let selectedIds = eligibility.targets
            .filter((target) => target.eligibility === 'eligible')
            .filter((target) => accountIdSet == null || accountIdSet.has(String(target.masterAccountId)))
            .map((target) => target.selectionId);

        if (sendLimit != null) {
            const remaining = sendLimit - summary.sent;
            if (remaining <= 0) {
                selectedIds = [];
            } else {
                selectedIds = selectedIds.slice(0, remaining);
            }
        }

        const periodSummary = {
            ...duePeriod,
            planId: batch.plan?.id ? String(batch.plan.id) : duePeriod.planId,
            batchAccountCount: batch.summary.totalAccounts,
            batchSubscriptionCount: batch.summary.totalSubscriptions,
            eligible: reportSummary.eligible,
            noEmail: reportSummary.noEmail,
            unsubscribed: reportSummary.unsubscribed,
            alreadySent: reportSummary.alreadySent,
            excludedTag: reportSummary.excludedTag,
            sent: 0,
            failed: 0,
            status: selectedIds.length > 0 ? 'ready' : 'skipped_no_eligible_targets',
        };

        summary.eligible += reportSummary.eligible;
        summary.noEmail += reportSummary.noEmail;
        summary.unsubscribed += reportSummary.unsubscribed;
        summary.alreadySent += reportSummary.alreadySent;
        summary.excludedTag += reportSummary.excludedTag;
        summary.processedPeriodCount++;

        const selectedIdSet = new Set(selectedIds.map(String));

        // Preflight validation — render one sample and hand control to caller.
        if (preflight && selectedIds.length > 0) {
            const sampleTarget = eligibility.targets.find((t) => selectedIdSet.has(String(t.selectionId)));
            let sample = null;
            if (sampleTarget) {
                const sampleUnsubToken = Buffer
                    .from(JSON.stringify({ client: duePeriod.client, email: sampleTarget.email, masterAccountId: sampleTarget.masterAccountId }))
                    .toString('base64');
                const sampleUnsubUrl = `${trimmedBaseUrl}/api/repricing/price-push/unsubscribe?token=${encodeURIComponent(sampleUnsubToken)}`;
                const rendered = await sendPriceIncreaseEmail({
                    recipient: testRecipient || sampleTarget.email,
                    recipientName: sampleTarget.customerName,
                    customerName: sampleTarget.customerName,
                    accountName: sampleTarget.accountName || sampleTarget.masterAccountId,
                    clientName: duePeriod.client,
                    effectiveDate: sampleTarget.effectiveDate,
                    effectivePeriod: sampleTarget.effectivePeriod,
                    services: sampleTarget.services,
                    unsubscribeUrl: sampleUnsubUrl,
                    ...(senderConfig.fromEmail ? { fromEmail: senderConfig.fromEmail } : {}),
                    ...(senderConfig.fromName ? { fromName: senderConfig.fromName } : {}),
                    ...(senderConfig.replyTo ? { replyTo: senderConfig.replyTo } : {}),
                    ...(senderConfig.templateConfig ? { templateConfig: senderConfig.templateConfig } : {}),
                    dryRun: true,
                });
                sample = { target: sampleTarget, rendered: rendered.rendered };
            }

            // Sanity checks
            const sanity = { warnings: [] };
            if (sample) {
                const blob = `${sample.rendered.subject}\n${sample.rendered.textContent}`;
                const unresolved = blob.match(/\{[a-z_][a-z0-9_]*\}/gi);
                if (unresolved && unresolved.length > 0) {
                    sanity.warnings.push(`Unresolved template placeholders in rendered sample: ${[...new Set(unresolved)].join(', ')}`);
                }
                for (const svc of sample.target.services) {
                    const oldP = Number(svc.oldCharge ?? svc.currentPrice) || 0;
                    const newP = Number(svc.newCharge ?? svc.newPrice) || 0;
                    if (oldP > 0 && newP <= oldP) {
                        sanity.warnings.push(`Service "${svc.serviceTypeName}" has newPrice (${newP}) <= oldPrice (${oldP})`);
                    }
                    if (oldP > 0) {
                        const pct = ((newP - oldP) / oldP) * 100;
                        if (pct > 20) sanity.warnings.push(`Service "${svc.serviceTypeName}" increase ${pct.toFixed(1)}% exceeds 20%`);
                    }
                }
            }
            if (!senderConfig.fromEmail) sanity.warnings.push('senderConfig.fromEmail is null — using fallback');
            if (!senderConfig.replyTo) sanity.warnings.push('senderConfig.replyTo is null');

            const excludedTargets = eligibility.targets.filter((t) => t.eligibility === 'excluded_tag');
            const countsByTag = Object.fromEntries(senderConfig.excludedTagKeys.map((k) => [k, 0]));
            for (const t of excludedTargets) {
                for (const tag of (t.excludedByTags || [])) {
                    countsByTag[tag] = (countsByTag[tag] || 0) + 1;
                }
            }
            const breakdown = await fetchExcludedTagBreakdown(
                duePeriod.client,
                batch.plan?.id,
                senderConfig.excludedTagKeys,
            );
            const excludedSamples = excludedTargets.slice(0, 10).map((t) => ({
                accountId: t.masterAccountId,
                accountName: t.accountName,
                email: t.email || null,
                matchedTags: t.excludedByTags || [],
            }));

            const proceed = await preflight({
                period: periodSummary,
                senderConfig,
                counts: {
                    eligible: eligibility.summary.eligible,
                    noEmail: eligibility.summary.noEmail,
                    unsubscribed: eligibility.summary.unsubscribed,
                    alreadySent: eligibility.summary.alreadySent,
                    excludedTag: eligibility.summary.excludedTag,
                    toSend: selectedIds.length,
                },
                excluded: {
                    keys: senderConfig.excludedTagKeys,
                    countsByTag,
                    breakdown,
                    currentPeriod: duePeriod.effectivePeriod,
                    samples: excludedSamples,
                },
                sample,
                sanity,
            });
            if (!proceed) {
                periodSummary.status = 'skipped_preflight_declined';
                summary.periods.push(periodSummary);
                continue;
            }
        }

        let result = { details: [] };
        if (selectedIds.length > 0) {
            result = await sendNotificationTargets({
                client: duePeriod.client,
                mode: PRE_PUSH_MODE,
                targets: eligibility.targets,
                selectedIds,
                sentBy,
                baseUrl: trimmedBaseUrl,
                senderConfig,
                testRecipient,
                existingRunId: runIdToReuse,
            });
            runIdToReuse = null; // consumed — later periods open their own run

            periodSummary.sent = result.sent;
            periodSummary.failed = result.failed;
            periodSummary.totalSelected = result.total;
            periodSummary.status = result.failed > 0 ? 'completed_with_failures' : 'completed';

            summary.sent += result.sent;
            summary.failed += result.failed;
        }

        const sendStatusBySelectionId = new Map(
            (result.details || []).map((d) => [String(d.selectionId), d])
        );

        for (const target of eligibility.targets) {
            const totalIncrease = target.services.reduce((sum, s) => sum + (Number(s.increaseAmount) || 0), 0);
            let sendStatus;
            if (selectedIdSet.has(String(target.selectionId))) {
                const detail = sendStatusBySelectionId.get(String(target.selectionId));
                sendStatus = detail ? detail.status : 'not_sent';
                if (detail?.error) sendStatus = `${sendStatus}: ${detail.error}`;
            } else if (target.eligibility !== 'eligible') {
                sendStatus = `skipped_${target.eligibility}`;
                if (target.eligibility === 'excluded_tag' && target.excludedByTags?.length) {
                    sendStatus += ` (${target.excludedByTags.join(', ')})`;
                }
            } else {
                sendStatus = 'skipped_send_limit';
            }

            summary.records.push({
                accountId: target.masterAccountId,
                accountName: target.accountName,
                customerName: target.customerName,
                email: target.email || '',
                eligibility: target.eligibility,
                effectivePeriod: duePeriod.effectivePeriod,
                effectiveDate: duePeriod.effectiveDate,
                serviceCount: target.services.length,
                totalIncrease: totalIncrease.toFixed(2),
                services: target.services.map(s => `${s.serviceTypeName}: $${Number(s.currentPrice).toFixed(2)} -> $${Number(s.newPrice).toFixed(2)}`).join(' | '),
                sendStatus,
            });
        }

        summary.periods.push(periodSummary);
    }

    return summary;
}
