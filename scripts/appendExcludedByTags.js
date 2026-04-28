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
    console.log('[excluded] Loaded .env');
}

const { closePool } = await import('../lib/postgres.js');
const { buildPlanV2PricePushSource } = await import('../services/planV2PricePushService.js');
const {
    findDuePrePushNotificationPeriods,
    buildPrePushNotificationTargets,
    annotateNotificationEligibility,
    fetchNotificationConfig,
} = await import('../services/priceIncreaseNotificationService.js');

const ENRICHED_XLSX = join(__dirname, '..', '1777030286_activity_enriched.xlsx');
const PRE_PUSH_MODE = 'pre_push';

async function main() {
    const wb = XLSX.readFile(ENRICHED_XLSX);
    const sheet = wb.Sheets[wb.SheetNames[0]];
    const existingRows = XLSX.utils.sheet_to_json(sheet, { defval: null });
    console.log(`[excluded] Loaded ${existingRows.length} existing rows from ${ENRICHED_XLSX}`);

    const duePeriods = await findDuePrePushNotificationPeriods({});
    const cdpcPeriods = duePeriods.filter((p) => p.client.startsWith('CDPC_'));
    console.log(`[excluded] Found ${cdpcPeriods.length} due CDPC period(s):`,
        cdpcPeriods.map((p) => `${p.client}/${p.effectivePeriod}`).join(', '));

    const appendedRows = [];
    for (const due of cdpcPeriods) {
        const { client, effectivePeriod } = due;
        console.log(`[excluded] Processing ${client}/${effectivePeriod}`);

        const batch = await buildPlanV2PricePushSource({ client, effectivePeriod });
        if (!batch.plan || batch.accounts.length === 0) {
            console.log(`[excluded]   -> no accounts in plan, skipping`);
            continue;
        }
        const subsByAccount = new Map();
        for (const account of batch.accounts) {
            subsByAccount.set(String(account.masterAccountId), account.subscriptions || []);
        }

        const targets = await buildPrePushNotificationTargets({ client, batch });
        const senderConfig = await fetchNotificationConfig(client);
        const eligibility = await annotateNotificationEligibility({
            client,
            mode: PRE_PUSH_MODE,
            targets,
            excludedTagKeys: senderConfig.excludedTagKeys || [],
        });

        const excluded = eligibility.targets.filter((t) => t.eligibility === 'excluded_tag');
        console.log(`[excluded]   -> ${excluded.length} excluded-by-tag accounts`);

        for (const target of excluded) {
            const subs = subsByAccount.get(String(target.masterAccountId)) || [];
            const name = target.customerName || target.accountName || null;
            const tags = (target.excludedByTags || []).join(', ');

            if (subs.length === 0) {
                appendedRows.push({
                    event: 'excluded_by_tag',
                    recipient: target.email || null,
                    subject: tags || null,
                    domain: null,
                    'Customer ID': target.masterAccountId,
                    'Subscription ID': null,
                    'Client Name': client,
                    'Name': name,
                    'Service Type': null,
                    'Quarterly Increase $': null,
                    'Monthly Increase $': null,
                });
                continue;
            }

            for (const sub of subs) {
                const annual = Number(sub.annualIncrease) || 0;
                appendedRows.push({
                    event: 'excluded_by_tag',
                    recipient: target.email || null,
                    subject: tags || null,
                    domain: null,
                    'Customer ID': sub.customerId || target.masterAccountId,
                    'Subscription ID': sub.subscriptionId,
                    'Client Name': client,
                    'Name': name,
                    'Service Type': sub.serviceTypeName,
                    'Quarterly Increase $': Number((annual / 4).toFixed(2)),
                    'Monthly Increase $': Number((annual / 12).toFixed(2)),
                });
            }
        }
    }

    console.log(`[excluded] Appending ${appendedRows.length} rows`);
    const combined = [...existingRows, ...appendedRows];

    const outSheet = XLSX.utils.json_to_sheet(combined);
    const outWb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(outWb, outSheet, 'Sheet1');
    XLSX.writeFile(outWb, ENRICHED_XLSX);
    console.log(`[excluded] Wrote ${combined.length} total rows to ${ENRICHED_XLSX}`);
}

try {
    await main();
} catch (err) {
    console.error('[excluded] Failed:', err?.message || err);
    process.exitCode = 1;
} finally {
    try { await closePool(); } catch (e) { console.error('[excluded] pool close failed:', e?.message || e); }
}
