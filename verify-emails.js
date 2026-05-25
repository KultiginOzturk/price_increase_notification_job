/**
 * verify-emails.js — entry point for the app-triggered email verification step.
 *
 * Resolves the same recipient cohort the send job builds, verifies every
 * recipient email through MailerSend (skipping addresses already verified
 * inside the TTL window), caches the verdicts in email_health, then reconciles
 * reason-coded deliverability tags onto inp_account_tags:
 *   no email on file        → no_email
 *   verification hard fail  → email_invalid
 *   (a prior hard bounce / spam complaint already in email_health is also
 *    reflected — email_hard_bounced / email_spam_complaint)
 *
 * Cohort selection (env), mirroring the send job:
 *   NOTIFICATION_CLIENTS      comma-separated client list (default: all due)
 *   NOTIFICATION_ACCOUNT_IDS  master_account_id allowlist (app-triggered cohort)
 *   NOTIFICATION_TARGET_DATE  YYYY-MM-DD (default: today)
 *   VERIFY_EMAILS             comma-separated explicit addresses — verifies them
 *                             only, no cohort resolution / no tagging (ad-hoc)
 *   EMAIL_VERIFY_FORCE        true → re-verify even fresh cached addresses
 */

import { config } from 'dotenv';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = join(__dirname, '.env');
if (existsSync(envPath)) config({ path: envPath });

const { closePool } = await import('./lib/postgres.js');
const { ensureEmailHealthSchema, normalizeEmail } = await import('./lib/emailHealth.js');
const { resolveCohortTargets } = await import('./lib/cohort.js');
const { reconcileCohortDeliverabilityTags } = await import('./lib/accountTags.js');
const { verifyEmails } = await import('./services/emailVerificationService.js');

const LOG = '[verify-emails]';

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

    const force = /^(1|true|yes)$/i.test(String(process.env.EMAIL_VERIFY_FORCE || ''));
    const explicit = parseList(process.env.VERIFY_EMAILS);

    // Ad-hoc mode: verify explicit addresses only — no cohort, no tag reconcile.
    if (explicit.length > 0) {
        const emails = [...new Set(explicit.map(normalizeEmail).filter(Boolean))];
        console.log(`${LOG} Ad-hoc: verifying ${emails.length} explicit address(es) from VERIFY_EMAILS`);
        const { results, stats } = await verifyEmails({ emails, force });
        const byStatus = {};
        for (const result of results.values()) {
            const key = result.error ? 'error' : (result.status || 'unknown');
            byStatus[key] = (byStatus[key] || 0) + 1;
        }
        console.log(
            `${LOG} Complete total=${stats.total} verified=${stats.verified} ` +
            `cached=${stats.cached} failed=${stats.failed} mocked=${stats.mocked}`,
        );
        console.log(`${LOG} By verdict: ${formatByKey(byStatus)}`);
        return;
    }

    const clients = parseList(process.env.NOTIFICATION_CLIENTS || process.env.CLIENT);
    const accountIds = parseList(process.env.NOTIFICATION_ACCOUNT_IDS);
    const targetDate = process.env.NOTIFICATION_TARGET_DATE || null;

    console.log(
        `${LOG} Resolving cohort clients=${clients.join(',') || 'all'} ` +
        `accountIds=${accountIds.length || 'all'} targetDate=${targetDate || 'today'}`,
    );
    const targets = await resolveCohortTargets({ clients, accountIds, targetDate });
    console.log(`${LOG} Resolved ${targets.length} account(s) in cohort`);

    if (targets.length === 0) {
        console.log(`${LOG} Empty cohort — nothing to do`);
        return;
    }

    // Verify every resolvable recipient email (no-email accounts skip straight
    // to tag reconciliation as no_email).
    const emails = [...new Set(targets.map((t) => t.email).filter(Boolean))];
    const { results, stats } = await verifyEmails({ emails, force });

    const byStatus = {};
    for (const result of results.values()) {
        const key = result.error ? 'error' : (result.status || 'unknown');
        byStatus[key] = (byStatus[key] || 0) + 1;
    }
    console.log(
        `${LOG} Verification: total=${stats.total} verified=${stats.verified} ` +
        `cached=${stats.cached} failed=${stats.failed} mocked=${stats.mocked}`,
    );
    console.log(`${LOG} By verdict: ${formatByKey(byStatus)}`);

    // Materialize email_health into reason-coded tags for the cohort.
    const tagSummary = await reconcileCohortDeliverabilityTags({
        targets,
        source: 'email_health',
        actor: 'verify-emails',
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
