/**
 * sync-email-status.js — entry point for the app-triggered email-status ETL.
 *
 * Two stages:
 *   1. Pull hard-bounce / spam-complaint activity from the MailerSend Activity
 *      API into email_health (global — every domain on the account).
 *   2. If a cohort is specified, reconcile reason-coded deliverability tags for
 *      that cohort so freshly-bounced accounts get email_hard_bounced /
 *      email_spam_complaint and show up on the no-email page.
 *
 * Re-run safe: the email_health upsert keeps only the latest timestamp per
 * event type, so re-scanning an overlapping window cannot corrupt state.
 * Bounces and complaints arrive for days after a send — re-run accordingly.
 *
 * Env:
 *   NOTIFICATION_STATUS_SINCE_DAYS  lookback window in days (default 30)
 *   MAILERSEND_SYNC_DOMAINS         comma-separated domain ids/names to limit to
 *   NOTIFICATION_CLIENTS            cohort to reconcile tags for (optional)
 *   NOTIFICATION_ACCOUNT_IDS        master_account_id allowlist (optional)
 *   NOTIFICATION_TARGET_DATE        YYYY-MM-DD (default: today)
 *
 * With no NOTIFICATION_CLIENTS / NOTIFICATION_ACCOUNT_IDS the ETL still runs —
 * email_health is updated and tag reconciliation is skipped.
 */

import { config } from 'dotenv';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = join(__dirname, '.env');
if (existsSync(envPath)) config({ path: envPath });

const { closePool } = await import('./lib/postgres.js');
const { ensureEmailHealthSchema } = await import('./lib/emailHealth.js');
const { resolveCohortTargets } = await import('./lib/cohort.js');
const { reconcileCohortDeliverabilityTags } = await import('./lib/accountTags.js');
const { syncEmailStatus } = await import('./services/mailerSendActivityService.js');

const LOG = '[sync-email-status]';

function parseList(rawValue) {
    if (!rawValue) return [];
    return [...new Set(
        String(rawValue).split(',').map((value) => value.trim()).filter(Boolean),
    )];
}

function formatByKey(byKey) {
    const entries = Object.entries(byKey || {}).sort();
    return entries.length > 0 ? entries.map(([k, v]) => `${k}=${v}`).join(' ') : 'none';
}

async function main() {
    await ensureEmailHealthSchema();

    const sinceDays = process.env.NOTIFICATION_STATUS_SINCE_DAYS
        ? Number(process.env.NOTIFICATION_STATUS_SINCE_DAYS)
        : null;

    // Stage 1 — pull activity into email_health.
    console.log(`${LOG} Starting activity sync lookback=${sinceDays ?? 'default'} day(s)`);
    const summary = await syncEmailStatus({ sinceDays });
    for (const domain of summary.domains) {
        console.log(
            `${LOG} domain=${domain.name} window=${domain.from}..${domain.to} ` +
            `events=${domain.events} hardBounced=${domain.hardBounced} ` +
            `spamComplaints=${domain.spamComplaints}`,
        );
    }
    console.log(
        `${LOG} Activity sync complete domains=${summary.domains.length} ` +
        `hardBounced=${summary.totalHardBounced} spamComplaints=${summary.totalSpamComplaints}`,
    );

    // Stage 2 — reconcile tags for the cohort, if one was specified.
    const clients = parseList(process.env.NOTIFICATION_CLIENTS || process.env.CLIENT);
    const accountIds = parseList(process.env.NOTIFICATION_ACCOUNT_IDS);
    const targetDate = process.env.NOTIFICATION_TARGET_DATE || null;

    if (clients.length === 0 && accountIds.length === 0) {
        console.log(`${LOG} No cohort specified — email_health updated, tag reconciliation skipped`);
        return;
    }

    console.log(
        `${LOG} Reconciling tags for cohort clients=${clients.join(',') || 'all'} ` +
        `accountIds=${accountIds.length || 'all'} targetDate=${targetDate || 'today'}`,
    );
    const targets = await resolveCohortTargets({ clients, accountIds, targetDate });
    console.log(`${LOG} Resolved ${targets.length} account(s) in cohort`);

    if (targets.length === 0) {
        console.log(`${LOG} Empty cohort — nothing to reconcile`);
        return;
    }

    const tagSummary = await reconcileCohortDeliverabilityTags({
        targets,
        source: 'email_health',
        actor: 'sync-email-status',
    });
    console.log(
        `${LOG} Tags: accounts=${tagSummary.accounts} clients=${tagSummary.clients} ` +
        `tagged=${tagSummary.inserted} cleared=${tagSummary.endDated} unchanged=${tagSummary.unchanged}`,
    );
    console.log(`${LOG} Tagged by key: ${formatByKey(tagSummary.insertedByKey)}`);
}

try {
    await main();
} catch (error) {
    console.error(`${LOG} Job failed:`, error?.message || error);
    process.exitCode = 1;
} finally {
    try {
        await closePool();
    } catch (closeError) {
        console.error(`${LOG} Failed to close PostgreSQL pool:`, closeError?.message || closeError);
        process.exitCode = process.exitCode || 1;
    }
}
