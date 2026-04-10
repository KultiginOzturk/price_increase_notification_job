import { config } from 'dotenv';
import { existsSync } from 'fs';
import XLSX from 'xlsx';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const envPath = join(__dirname, '.env');
if (existsSync(envPath)) config({ path: envPath });

// Dynamic imports so dotenv is loaded before any module reads process.env
const { closePool } = await import('./lib/postgres.js');
const {
    findDuePrePushNotificationPeriods,
    buildPrePushNotificationTargets,
    annotateNotificationEligibility,
} = await import('./services/priceIncreaseNotificationService.js');
const { buildPlanV2PricePushSource } = await import('./services/planV2PricePushService.js');

const client = process.argv[2] || process.env.NOTIFICATION_CLIENTS;
if (!client) {
    console.error('Usage: node export-eligible.js <CLIENT>');
    console.error('Example: node export-eligible.js MODERN');
    process.exit(1);
}
const targetDate = process.env.NOTIFICATION_TARGET_DATE || null;

try {
    const duePeriods = await findDuePrePushNotificationPeriods({
        targetDate,
        clients: [client],
    });

    console.log(`Found ${duePeriods.length} due period(s) for ${client}`);

    const rows = [];

    for (const duePeriod of duePeriods) {
        const batch = await buildPlanV2PricePushSource({
            client: duePeriod.client,
            effectivePeriod: duePeriod.effectivePeriod,
        });

        if (!batch.plan || batch.summary.totalAccounts === 0) {
            console.log(`Skipping empty batch for period ${duePeriod.effectivePeriod}`);
            continue;
        }

        const targets = await buildPrePushNotificationTargets({ client: duePeriod.client, batch });
        const { targets: annotated } = await annotateNotificationEligibility({
            client: duePeriod.client,
            mode: 'pre_push',
            targets,
        });

        for (const t of annotated) {
            const totalIncrease = t.services.reduce((sum, s) => sum + (Number(s.increaseAmount) || 0), 0);
            rows.push({
                masterAccountId: t.masterAccountId,
                accountName: t.accountName,
                customerName: t.customerName,
                email: t.email || '',
                eligibility: t.eligibility,
                effectivePeriod: duePeriod.effectivePeriod,
                effectiveDate: duePeriod.effectiveDate,
                serviceCount: t.services.length,
                totalIncrease: totalIncrease.toFixed(2),
                services: t.services.map(s => `${s.serviceTypeName}: $${Number(s.currentPrice).toFixed(2)} -> $${Number(s.newPrice).toFixed(2)}`).join(' | '),
            });
        }
    }

    if (rows.length === 0) {
        console.log('No customers found.');
    } else {
        const ws = XLSX.utils.json_to_sheet(rows);
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, 'Eligible');

        const outPath = join(__dirname, `eligible-${client}.xlsx`);
        XLSX.writeFile(wb, outPath);
        console.log(`Exported ${rows.length} customers to ${outPath}`);

        const eligible = rows.filter(r => r.eligibility === 'eligible').length;
        const noEmail = rows.filter(r => r.eligibility === 'no_email').length;
        const unsubscribed = rows.filter(r => r.eligibility === 'unsubscribed').length;
        const alreadySent = rows.filter(r => r.eligibility === 'already_sent').length;
        console.log(`Summary: eligible=${eligible} no_email=${noEmail} unsubscribed=${unsubscribed} already_sent=${alreadySent}`);
    }
} catch (error) {
    console.error('Export failed:', error?.message || error);
    process.exitCode = 1;
} finally {
    await closePool();
}
