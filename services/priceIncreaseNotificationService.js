import { randomUUID } from 'crypto';
import { query as pgQuery } from '../lib/postgres.js';
import { getRCP, INPUTS, SHARED } from '../config/tables.js';
import { runQuery } from '../utils/bigquery.js';
import { deriveTimingFromPeriod } from '../routes/planV2/timing.js';
import { sendPriceIncreaseEmail, validateEmails } from './emailService.js';
import { PLAN_V2_PUSH_REVIEW_TABS, buildPlanV2PricePushSource } from './planV2PricePushService.js';

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

    const rows = await runQuery(`
        SELECT
            CAST(customer_id AS STRING) AS customer_id,
            email,
            billing_email,
            first_name,
            last_name,
            company_name
        FROM ${SHARED.curCustomer}
        WHERE client = @client
          AND CAST(customer_id AS STRING) IN UNNEST(@customerIds)
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
                c.first_name,
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
            cs.services_per_year
        FROM ${INPUTS.pricePushSubscriptionDetail} d
        INNER JOIN ${INPUTS.pricePushQueue} q
            ON d.queue_id = q.id AND d.client = q.client
        LEFT JOIN ${SHARED.curSubscription} cs
            ON CAST(d.subscription_id AS STRING) = CAST(cs.subscription_id AS STRING)
            AND d.client = cs.client
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

export async function annotateNotificationEligibility({ client, mode, targets }) {
    const unsubscribedEmails = await fetchUnsubscribedEmails(client, targets.map((target) => target.email));
    const alreadySentKeys = await fetchAlreadySentKeys(client, mode, targets);

    const withEligibility = targets.map((target) => {
        const email = normalizeEmail(target.email);
        let eligibility = 'eligible';
        if (!email || !isValidEmail(email)) {
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

    return {
        fromEmail,
        fromName: customName || null,
        replyTo,
        templateConfig, // All notification_* settings as a flat object
    };
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
        total: selectedTargets.length,
        details: [],
    };

    const eventRows = [];
    const legacyRows = [];

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

        const sendResult = await sendPriceIncreaseEmail({
            recipient: actualRecipientEmail,
            recipientName: target.customerName,
            customerName: target.customerName,
            accountName: target.accountName || target.masterAccountId,
            clientName: client,
            effectiveDate: target.effectiveDate,
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
        } else {
            results.failed++;
            results.details.push({ ...detail, status: statusLabel, email: actualRecipientEmail || undefined, error: errorMessage });
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

    if (legacyRows.length > 0) {
        await runQuery(`
            INSERT INTO ${INPUTS.priceIncreaseNotifications}
                (id, client, queue_id, master_account_id, recipient_email, recipient_name, status, error_message, mailersend_message_id, subscription_count, sent_by, created_at)
            VALUES ${buildLegacyInsertSql(legacyRows)}
        `, {}, 'price-increase-notify-insert-legacy');
    }

    return results;
}

export async function findDuePrePushNotificationPeriods({ targetDate = null, clients = null } = {}) {
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
            FROM planv2_plan
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
            INNER JOIN planv2_account_decision ad
                ON ad.plan_id = lp.id
            LEFT JOIN planv2_client_response account_skip
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

    return result.rows
        .map((row) => {
            const timing = deriveTimingFromPeriod(row.effective_period);
            return {
                planId: row.plan_id ? String(row.plan_id) : null,
                client: row.client,
                effectivePeriod: row.effective_period,
                accountCount: Number(row.account_count) || 0,
                noticeDate: timing.noticeDate,
                effectiveDate: timing.effectiveDate,
            };
        })
        .filter((row) => isDuePrePushNotificationPeriod({
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
    preflight = null, // optional async ({ period, senderConfig, counts, sample, sanity }) => boolean
} = {}) {
    const normalizedTargetDate = normalizeDateOnly(targetDate) || new Date().toISOString().slice(0, 10);
    const trimmedBaseUrl = typeof baseUrl === 'string' ? baseUrl.trim().replace(/\/+$/, '') : '';
    if (!trimmedBaseUrl) {
        throw new Error('baseUrl is required for unsubscribe links');
    }

    const duePeriods = await findDuePrePushNotificationPeriods({
        targetDate: normalizedTargetDate,
        clients,
    });

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
        const eligibility = await annotateNotificationEligibility({
            client: duePeriod.client,
            mode: PRE_PUSH_MODE,
            targets,
        });
        const senderConfig = await fetchNotificationConfig(duePeriod.client);
        let selectedIds = eligibility.targets
            .filter((target) => target.eligibility === 'eligible')
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
            eligible: eligibility.summary.eligible,
            noEmail: eligibility.summary.noEmail,
            unsubscribed: eligibility.summary.unsubscribed,
            alreadySent: eligibility.summary.alreadySent,
            sent: 0,
            failed: 0,
            status: selectedIds.length > 0 ? 'ready' : 'skipped_no_eligible_targets',
        };

        summary.eligible += eligibility.summary.eligible;
        summary.noEmail += eligibility.summary.noEmail;
        summary.unsubscribed += eligibility.summary.unsubscribed;
        summary.alreadySent += eligibility.summary.alreadySent;
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

            const proceed = await preflight({
                period: periodSummary,
                senderConfig,
                counts: {
                    eligible: eligibility.summary.eligible,
                    noEmail: eligibility.summary.noEmail,
                    unsubscribed: eligibility.summary.unsubscribed,
                    alreadySent: eligibility.summary.alreadySent,
                    toSend: selectedIds.length,
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
            });

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
