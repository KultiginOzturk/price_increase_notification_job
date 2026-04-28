import { config } from 'dotenv';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import XLSX from 'xlsx';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const envPath = join(__dirname, '..', '.env');
if (existsSync(envPath)) {
    config({ path: envPath });
    console.log('[enrich] Loaded .env');
}

const { INPUTS } = await import('../config/tables.js');
const { runQuery } = await import('../utils/bigquery.js');
const { closePool } = await import('../lib/postgres.js');
const { buildPlanV2PricePushSource } = await import('../services/planV2PricePushService.js');

const INPUT_XLSX = join(__dirname, '..', '1777030286_activity.xlsx');
const OUTPUT_XLSX = join(__dirname, '..', '1777030286_activity_enriched.xlsx');

async function main() {
    const wb = XLSX.readFile(INPUT_XLSX);
    const sheet = wb.Sheets[wb.SheetNames[0]];
    const inputRows = XLSX.utils.sheet_to_json(sheet, { defval: null });
    console.log(`[enrich] Read ${inputRows.length} rows from ${INPUT_XLSX}`);

    const emails = [...new Set(
        inputRows.map((r) => String(r.recipient || '').trim().toLowerCase()).filter(Boolean)
    )];
    console.log(`[enrich] ${emails.length} unique recipient emails`);

    // 1. Most recent 'sent' event per email
    const eventRows = await runQuery(`
        SELECT
            recipient_email,
            client,
            master_account_id,
            account_name,
            recipient_name,
            plan_id,
            effective_period,
            created_at
        FROM (
            SELECT
                LOWER(TRIM(recipient_email)) AS recipient_email,
                client,
                master_account_id,
                account_name,
                recipient_name,
                plan_id,
                effective_period,
                created_at,
                ROW_NUMBER() OVER (
                    PARTITION BY LOWER(TRIM(recipient_email))
                    ORDER BY created_at DESC
                ) AS rn
            FROM ${INPUTS.priceIncreaseNotificationEvents}
            WHERE LOWER(TRIM(recipient_email)) IN UNNEST(@emails)
              AND status = 'sent'
        )
        WHERE rn = 1
    `, { emails }, 'enrich-events-lookup');
    console.log(`[enrich] Matched ${eventRows.length} emails to events`);

    const eventByEmail = new Map();
    for (const row of eventRows) {
        eventByEmail.set(row.recipient_email, row);
    }

    // 2. For each unique (client, effective_period), fetch subscriptions
    const planGroups = new Map();
    for (const row of eventRows) {
        const key = `${row.client}::${row.effective_period}`;
        if (!planGroups.has(key)) {
            planGroups.set(key, { client: row.client, effectivePeriod: row.effective_period });
        }
    }
    console.log(`[enrich] ${planGroups.size} unique (client, effectivePeriod) group(s) to fetch`);

    // account key = client::master_account_id → subscriptions[]
    const subsByAccount = new Map();
    for (const { client, effectivePeriod } of planGroups.values()) {
        console.log(`[enrich] Fetching plan source for client=${client} period=${effectivePeriod}`);
        try {
            const batch = await buildPlanV2PricePushSource({ client, effectivePeriod });
            for (const account of batch.accounts || []) {
                const key = `${client}::${account.masterAccountId}`;
                if (!subsByAccount.has(key)) subsByAccount.set(key, []);
                for (const sub of account.subscriptions || []) {
                    subsByAccount.get(key).push(sub);
                }
            }
        } catch (err) {
            console.error(`[enrich] Failed to fetch plan for ${client}/${effectivePeriod}:`, err?.message || err);
        }
    }
    console.log(`[enrich] Built subscription lookup for ${subsByAccount.size} accounts`);

    // 3. Build output rows (one per subscription, or one placeholder row if no subs found)
    const output = [];
    for (const inputRow of inputRows) {
        const email = String(inputRow.recipient || '').trim().toLowerCase();
        const event = eventByEmail.get(email);

        if (!event) {
            output.push({
                ...inputRow,
                'Customer ID': null,
                'Subscription ID': null,
                'Client Name': null,
                'Name': null,
                'Service Type': null,
                'Quarterly Increase $': null,
                'Monthly Increase $': null,
            });
            continue;
        }

        const subs = subsByAccount.get(`${event.client}::${event.master_account_id}`) || [];
        const name = event.recipient_name || event.account_name || null;

        if (subs.length === 0) {
            output.push({
                ...inputRow,
                'Customer ID': event.master_account_id,
                'Subscription ID': null,
                'Client Name': event.client,
                'Name': name,
                'Service Type': null,
                'Quarterly Increase $': null,
                'Monthly Increase $': null,
            });
            continue;
        }

        for (const sub of subs) {
            const annual = Number(sub.annualIncrease) || 0;
            output.push({
                ...inputRow,
                'Customer ID': sub.customerId || event.master_account_id,
                'Subscription ID': sub.subscriptionId,
                'Client Name': event.client,
                'Name': name,
                'Service Type': sub.serviceTypeName,
                'Quarterly Increase $': Number((annual / 4).toFixed(2)),
                'Monthly Increase $': Number((annual / 12).toFixed(2)),
            });
        }
    }

    // 4. Stats
    const unmatchedEmails = inputRows
        .map((r) => String(r.recipient || '').trim().toLowerCase())
        .filter((e) => e && !eventByEmail.has(e));
    console.log(`[enrich] Output rows: ${output.length}`);
    console.log(`[enrich] Unmatched emails (no event record): ${unmatchedEmails.length}`);
    if (unmatchedEmails.length > 0) {
        console.log('[enrich] Unmatched:', [...new Set(unmatchedEmails)].join(', '));
    }

    // 5. Write output xlsx
    const outSheet = XLSX.utils.json_to_sheet(output);
    const outWb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(outWb, outSheet, 'Sheet1');
    XLSX.writeFile(outWb, OUTPUT_XLSX);
    console.log(`[enrich] Wrote ${OUTPUT_XLSX}`);
}

try {
    await main();
} catch (err) {
    console.error('[enrich] Failed:', err?.message || err);
    process.exitCode = 1;
} finally {
    try { await closePool(); } catch (e) { console.error('[enrich] pool close failed:', e?.message || e); }
}
