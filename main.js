import { closePool } from './lib/postgres.js';
import { runDuePrePushNotifications } from './services/priceIncreaseNotificationService.js';

function parseClientList(rawValue) {
    if (!rawValue) return null;
    const clients = String(rawValue)
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean);
    return clients.length > 0 ? [...new Set(clients)] : null;
}

function logPeriodSummary(period) {
    const detailParts = [
        `status=${period.status}`,
        `client=${period.client}`,
        `effectivePeriod=${period.effectivePeriod}`,
        `noticeDate=${period.noticeDate || 'n/a'}`,
        `effectiveDate=${period.effectiveDate || 'n/a'}`,
        `eligible=${period.eligible ?? 0}`,
        `noEmail=${period.noEmail ?? 0}`,
        `unsubscribed=${period.unsubscribed ?? 0}`,
        `alreadySent=${period.alreadySent ?? 0}`,
        `sent=${period.sent ?? 0}`,
        `failed=${period.failed ?? 0}`,
    ];
    console.log(`[price-increase-notification-job] ${detailParts.join(' ')}`);
}

async function main() {
    const targetDate = process.env.NOTIFICATION_TARGET_DATE || null;
    const clients = parseClientList(process.env.NOTIFICATION_CLIENTS || process.env.CLIENT || null);
    const baseUrl = process.env.APP_URL || '';
    const sentBy = process.env.NOTIFICATION_SENT_BY || 'cloud_run_job';
    const testRecipient = process.env.NOTIFICATION_TEST_RECIPIENT || null;

    console.log(
        `[price-increase-notification-job] Starting targetDate=${targetDate || 'today'} ` +
        `clients=${clients ? clients.join(',') : 'all'} testRecipient=${testRecipient || 'none'}`
    );

    const summary = await runDuePrePushNotifications({
        targetDate,
        clients,
        baseUrl,
        sentBy,
        testRecipient,
    });

    for (const period of summary.periods) {
        logPeriodSummary(period);
    }

    console.log(
        `[price-increase-notification-job] Complete targetDate=${summary.targetDate} ` +
        `duePeriods=${summary.duePeriodCount} processedPeriods=${summary.processedPeriodCount} ` +
        `eligible=${summary.eligible} sent=${summary.sent} failed=${summary.failed}`
    );

    if (summary.failed > 0) {
        throw new Error(`Price increase notification job completed with ${summary.failed} failed send(s)`);
    }
}

try {
    await main();
} catch (error) {
    console.error('[price-increase-notification-job] Job failed:', error?.message || error);
    process.exitCode = 1;
} finally {
    try {
        await closePool();
    } catch (closeError) {
        console.error('[price-increase-notification-job] Failed to close PostgreSQL pool:', closeError?.message || closeError);
        process.exitCode = process.exitCode || 1;
    }
}
