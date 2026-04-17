import { config } from 'dotenv';
import { randomUUID } from 'crypto';
import { existsSync, readFileSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const envPath = join(__dirname, '..', '.env');
if (existsSync(envPath)) {
    config({ path: envPath });
    console.log('[backfill] Loaded .env');
}

const { INPUTS } = await import('../config/tables.js');
const { runQuery } = await import('../utils/bigquery.js');
const { closePool } = await import('../lib/postgres.js');
const {
    findDuePrePushNotificationPeriods,
    buildPrePushNotificationTargets,
    annotateNotificationEligibility,
} = await import('../services/priceIncreaseNotificationService.js');
const { buildPlanV2PricePushSource } = await import('../services/planV2PricePushService.js');

const PRE_PUSH_MODE = 'pre_push';

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
    const n = Number(value);
    return Number.isFinite(n) ? String(n) : String(fallback);
};

const buildEventInsertSql = (rows) => rows.map((row) => (
    `(${toSqlString(row.id)}, ${toSqlString(row.client)}, ${toSqlString(row.mode)}, ${toSqlString(row.plan_id)}, ` +
    `${toSqlString(row.effective_period)}, ${toSqlDate(row.effective_date)}, ${toSqlString(row.queue_id)}, ` +
    `${toSqlString(row.master_account_id)}, ${toSqlString(row.account_name)}, ${toSqlString(row.recipient_email)}, ` +
    `${toSqlString(row.recipient_name)}, ${toSqlString(row.status)}, ${toSqlString(row.error_message)}, ` +
    `${toSqlString(row.mailersend_message_id)}, ${toSqlNumber(row.service_count)}, ${toSqlString(row.sent_by)}, CURRENT_TIMESTAMP())`
)).join(',\n');

function parseLog(logText) {
    const re = /email sent to\s+(\S+)\.\s+MessageId:\s+(\S+)/g;
    const pairs = [];
    let m;
    while ((m = re.exec(logText)) !== null) {
        pairs.push({ email: m[1].trim().toLowerCase(), messageId: m[2].trim() });
    }
    return pairs;
}

async function main() {
    const client = process.env.NOTIFICATION_CLIENTS || 'MODERN';
    const sentBy = process.env.NOTIFICATION_SENT_BY || 'manual_send';
    const dryRun = process.env.DRY_RUN === 'true';

    const logPath = join(__dirname, 'backfill-log.txt');
    const logText = readFileSync(logPath, 'utf-8');
    const pairs = parseLog(logText);
    console.log(`[backfill] Parsed ${pairs.length} email/messageId pairs from log`);

    const clients = client.split(',').map((c) => c.trim()).filter(Boolean);
    const duePeriods = await findDuePrePushNotificationPeriods({ clients });
    console.log(`[backfill] Found ${duePeriods.length} due period(s)`);

    // Map normalized email -> target (for fast lookup). If multiple targets share an email,
    // we still want to log both messages against the first matching target.
    const targetByEmail = new Map();
    for (const duePeriod of duePeriods) {
        const batch = await buildPlanV2PricePushSource({
            client: duePeriod.client,
            effectivePeriod: duePeriod.effectivePeriod,
        });
        if (!batch.plan || batch.summary.totalAccounts === 0) continue;

        const targets = await buildPrePushNotificationTargets({ client: duePeriod.client, batch });
        const eligibility = await annotateNotificationEligibility({
            client: duePeriod.client,
            mode: PRE_PUSH_MODE,
            targets,
        });

        for (const target of eligibility.targets) {
            if (!target.email) continue;
            const key = `${duePeriod.client}::${target.email.toLowerCase()}`;
            if (!targetByEmail.has(key)) {
                targetByEmail.set(key, { ...target, client: duePeriod.client, planIdResolved: batch.plan?.id ? String(batch.plan.id) : target.planId });
            }
        }
    }
    console.log(`[backfill] Built lookup for ${targetByEmail.size} unique eligible emails`);

    const eventRows = [];
    const unmatched = [];
    for (const { email, messageId } of pairs) {
        let target = null;
        for (const c of clients) {
            const t = targetByEmail.get(`${c}::${email}`);
            if (t) { target = t; break; }
        }
        if (!target) {
            unmatched.push({ email, messageId });
            continue;
        }
        eventRows.push({
            id: randomUUID(),
            client: target.client,
            mode: PRE_PUSH_MODE,
            plan_id: target.planIdResolved || target.planId,
            effective_period: target.effectivePeriod,
            effective_date: target.effectiveDate,
            queue_id: target.queueId,
            master_account_id: target.masterAccountId,
            account_name: target.accountName,
            recipient_email: target.email,
            recipient_name: target.customerName,
            status: 'sent',
            error_message: null,
            mailersend_message_id: messageId,
            service_count: target.services.length,
            sent_by: sentBy,
        });
    }

    console.log(`[backfill] Built ${eventRows.length} event row(s); ${unmatched.length} unmatched`);
    if (unmatched.length > 0) {
        console.log('[backfill] Unmatched entries (will NOT be inserted):');
        for (const u of unmatched) console.log(`  - ${u.email}  (MessageId ${u.messageId})`);
    }

    if (eventRows.length === 0) {
        console.log('[backfill] Nothing to insert. Exiting.');
        return;
    }

    if (dryRun) {
        console.log('[backfill] DRY_RUN=true — printing first row and SQL preview, not inserting.');
        console.log('First event row:', JSON.stringify(eventRows[0], null, 2));
        console.log('SQL preview (first 2 rows):');
        console.log(buildEventInsertSql(eventRows.slice(0, 2)));
        return;
    }

    const sql = `
        INSERT INTO ${INPUTS.priceIncreaseNotificationEvents}
            (id, client, mode, plan_id, effective_period, effective_date, queue_id, master_account_id, account_name,
             recipient_email, recipient_name, status, error_message, mailersend_message_id, service_count, sent_by, created_at)
        VALUES ${buildEventInsertSql(eventRows)}
    `;

    await runQuery(sql, {}, 'backfill-insert-events');
    console.log(`[backfill] Inserted ${eventRows.length} event row(s) into priceIncreaseNotificationEvents`);
}

try {
    await main();
} catch (err) {
    console.error('[backfill] Failed:', err?.message || err);
    process.exitCode = 1;
} finally {
    try { await closePool(); } catch (e) { console.error('[backfill] pool close failed:', e?.message || e); }
}
