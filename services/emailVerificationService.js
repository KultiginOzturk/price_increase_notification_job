/**
 * Email verification via the MailerSend Email Verification API.
 *
 * verifyEmails() takes a set of addresses and, for each one NOT already
 * verified inside the TTL window, calls MailerSend's single-email verify
 * endpoint and caches the verdict in email_health. Addresses with a fresh
 * cached verdict are skipped — that is the "don't waste credits revalidating"
 * requirement.
 *
 * The verdict is stored raw (valid / catch_all / mailbox_not_found / ...);
 * the send/no-send policy lives in lib/emailHealth.js (evaluateEmailHealth).
 *
 * NOTE: this uses the single-email endpoint (one credit + one request per
 * uncached address). For very large first-time cohorts the bulk-list endpoint
 * (emailVerification.create → verifyList → getListResult) is cheaper per call;
 * the TTL cache makes every subsequent run small, so single-email is the
 * simpler default.
 */

import { MailerSend } from 'mailersend';
import logger from '../lib/logger.js';
import {
    normalizeEmail,
    lookupEmailHealth,
    isVerificationFresh,
    upsertVerificationResult,
} from '../lib/emailHealth.js';

const MAILERSEND_API_KEY = process.env.MAILERSEND_API_KEY;
const mailerSend = MAILERSEND_API_KEY ? new MailerSend({ apiKey: MAILERSEND_API_KEY }) : null;

// In-process rate limiter for the verification endpoint (separate quota from
// the send endpoint). Sliding 60s window.
const MAX_PER_MINUTE = Math.max(1, Number(process.env.MAILERSEND_VERIFY_MAX_PER_MINUTE) || 60);
const WINDOW_MS = 60 * 1000;
const callTimestamps = [];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForVerifySlot() {
    while (true) {
        const now = Date.now();
        while (callTimestamps.length > 0 && now - callTimestamps[0] >= WINDOW_MS) {
            callTimestamps.shift();
        }
        if (callTimestamps.length < MAX_PER_MINUTE) {
            callTimestamps.push(now);
            return;
        }
        await sleep(Math.max(250, WINDOW_MS - (now - callTimestamps[0]) + 50));
    }
}

/**
 * Verify a set of email addresses, using the email_health cache to skip
 * addresses already verified inside the TTL window.
 *
 * @param {Object} args
 * @param {string[]} args.emails
 * @param {boolean} [args.force]  re-verify even if a fresh cached verdict exists
 * @returns {Promise<{ results: Map<string, {status: string|null, fromCache: boolean, error?: string}>, stats: object }>}
 */
export async function verifyEmails({ emails, force = false }) {
    const unique = [...new Set((emails || []).map(normalizeEmail).filter(Boolean))];
    const results = new Map();
    const stats = { total: unique.length, verified: 0, cached: 0, failed: 0, mocked: 0 };
    if (unique.length === 0) return { results, stats };

    const health = await lookupEmailHealth(unique);

    for (const email of unique) {
        const existing = health.get(email);

        if (!force && isVerificationFresh(existing)) {
            results.set(email, { status: existing.verification_status, fromCache: true });
            stats.cached++;
            continue;
        }

        if (!mailerSend) {
            // No API key — mirror emailService's log-only behavior so local
            // runs without credentials don't hard-fail.
            results.set(email, { status: 'unknown', fromCache: false, mock: true });
            stats.mocked++;
            continue;
        }

        try {
            await waitForVerifySlot();
            const response = await mailerSend.emailVerification.verifyEmail(email);
            const status = response?.body?.status || response?.body?.result || 'unknown';
            await upsertVerificationResult({ email, status, checkedAt: new Date() });
            results.set(email, { status, fromCache: false });
            stats.verified++;
        } catch (error) {
            // MailerSend SDK throws { body, statusCode } on API errors.
            const detail = error?.body?.message
                ?? (typeof error?.body === 'string' ? error.body : null)
                ?? error?.message
                ?? String(error);
            logger.warn({ err: detail, email }, '[verify] MailerSend verification call failed');
            results.set(email, { status: null, fromCache: false, error: detail });
            stats.failed++;
        }
    }

    return { results, stats };
}
