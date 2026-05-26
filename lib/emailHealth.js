/**
 * email_health — address-level email deliverability store.
 *
 * One row per email address (global, not client-scoped — a bounce or a
 * verification verdict is a property of the address, not of any one account).
 *
 * Two writers:
 *   - verify-emails.js      → verification_status / verification_checked_at
 *   - sync-email-status.js  → last_hard_bounce_at / last_spam_complaint_at
 *
 * One reader of record: evaluateEmailHealth() — the single source of truth for
 * "can we email this address?", consumed by tag reconciliation and routing.
 *
 * Schema mirror lives in migrations/001_email_health.sql; ensureEmailHealthSchema()
 * creates it on demand so the job is self-bootstrapping.
 */

import { query } from './postgres.js';

// How long a verification verdict is trusted before it is re-checked. Keeps
// MailerSend credits from being spent re-verifying the same address every run.
const VERIFICATION_TTL_DAYS = Math.max(1, Number(process.env.EMAIL_VERIFICATION_TTL_DAYS) || 90);

// MailerSend verification verdicts that mean "do not email this address".
// Policy: LOOSE — only unambiguous hard failures block. typo / disposable /
// mailbox_blocked / catch_all / role_based / mailbox_full / unknown are all
// still sent to (maximize email reach, accept some bounces — bounces are then
// caught by the Activity ETL). Adjust this set to tighten the policy.
const VERIFICATION_BLOCK = new Set([
    'syntax_error',
    'mailbox_not_found',
    'failed',
]);

// Domains whose mail servers defeat SMTP RCPT TO probing as an anti-harvesting
// measure — MailerSend returns mailbox_not_found for valid mailboxes here, so
// we ignore that specific verdict for these domains and rely on the Activity
// ETL bounce signal instead. syntax_error / failed still block as usual.
const PROBE_UNRELIABLE_DOMAINS = new Set([
    'yahoo.com',
    'aol.com',
]);

export const normalizeEmail = (value) => {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim().toLowerCase();
    return trimmed || null;
};

export async function ensureEmailHealthSchema() {
    await query(`
        CREATE TABLE IF NOT EXISTS email_health (
            email                   text PRIMARY KEY,
            verification_status     varchar(32),
            verification_checked_at timestamptz,
            last_hard_bounce_at     timestamptz,
            last_spam_complaint_at  timestamptz,
            last_bounce_reason      text,
            created_at              timestamptz NOT NULL DEFAULT now(),
            updated_at              timestamptz NOT NULL DEFAULT now()
        )
    `);
}

/**
 * Fetch email_health rows for a set of addresses.
 * @param {string[]} emails
 * @returns {Promise<Map<string, object>>} normalized email → row
 */
export async function lookupEmailHealth(emails) {
    const unique = [...new Set((emails || []).map(normalizeEmail).filter(Boolean))];
    if (unique.length === 0) return new Map();
    const result = await query(
        `SELECT * FROM email_health WHERE email = ANY($1::text[])`,
        [unique],
    );
    return new Map(result.rows.map((row) => [row.email, row]));
}

/**
 * True when a row carries a verification verdict still inside the TTL window —
 * i.e. re-verifying would just waste a MailerSend credit.
 */
export function isVerificationFresh(row, ttlDays = VERIFICATION_TTL_DAYS) {
    if (!row || !row.verification_checked_at || !row.verification_status) return false;
    const ageMs = Date.now() - new Date(row.verification_checked_at).getTime();
    return ageMs < ttlDays * 86400 * 1000;
}

/**
 * Single source of truth for "can we reach this address by email?".
 * A missing row → deliverable (an unverified address is not assumed bad).
 *
 * @param {object|null} row  an email_health row
 * @returns {{ deliverable: boolean, reason: ('hard_bounced'|'spam_complaint'|'verification_failed'|null) }}
 */
export function evaluateEmailHealth(row) {
    if (!row) return { deliverable: true, reason: null };
    if (row.last_hard_bounce_at) return { deliverable: false, reason: 'hard_bounced' };
    if (row.last_spam_complaint_at) return { deliverable: false, reason: 'spam_complaint' };
    if (row.verification_status && VERIFICATION_BLOCK.has(row.verification_status)) {
        const domain = String(row.email || '').split('@')[1] || '';
        const probeUnreliable = row.verification_status === 'mailbox_not_found'
            && PROBE_UNRELIABLE_DOMAINS.has(domain);
        if (!probeUnreliable) {
            return { deliverable: false, reason: 'verification_failed' };
        }
    }
    return { deliverable: true, reason: null };
}

/**
 * Store one verification verdict. Called per-address right after the MailerSend
 * call so a crash mid-batch keeps the verdicts already paid for.
 */
export async function upsertVerificationResult({ email, status, checkedAt = new Date() }) {
    const normalized = normalizeEmail(email);
    if (!normalized) return;
    await query(
        `INSERT INTO email_health (email, verification_status, verification_checked_at, updated_at)
         VALUES ($1, $2, $3, now())
         ON CONFLICT (email) DO UPDATE SET
             verification_status     = EXCLUDED.verification_status,
             verification_checked_at = EXCLUDED.verification_checked_at,
             updated_at              = now()`,
        [normalized, status || null, checkedAt],
    );
}

/**
 * Record hard-bounce / spam-complaint events.
 *
 * Idempotent: only the latest occurrence timestamp per type is kept (GREATEST),
 * so overlapping sync windows re-delivering the same events cannot corrupt
 * state — re-running the sync is always safe.
 *
 * @param {Array<{email: string, eventType: 'hard_bounced'|'spam_complaints', occurredAt: Date, reason?: string}>} events
 * @returns {Promise<number>} count of distinct addresses written
 */
export async function recordBounceEvents(events) {
    // Collapse to one row per email — keep the latest timestamp seen per type.
    const byEmail = new Map();
    for (const ev of events || []) {
        const email = normalizeEmail(ev.email);
        if (!email) continue;
        const occurredAt = ev.occurredAt instanceof Date
            ? ev.occurredAt
            : new Date(ev.occurredAt || Date.now());
        const agg = byEmail.get(email) || { hardBounceAt: null, spamComplaintAt: null, reason: null };
        if (ev.eventType === 'hard_bounced') {
            if (!agg.hardBounceAt || occurredAt > agg.hardBounceAt) agg.hardBounceAt = occurredAt;
            agg.reason = ev.reason || agg.reason || 'hard_bounced';
        } else if (ev.eventType === 'spam_complaints') {
            if (!agg.spamComplaintAt || occurredAt > agg.spamComplaintAt) agg.spamComplaintAt = occurredAt;
            agg.reason = agg.reason || ev.reason || 'spam_complaint';
        }
        byEmail.set(email, agg);
    }

    for (const [email, agg] of byEmail) {
        await query(
            `INSERT INTO email_health
                 (email, last_hard_bounce_at, last_spam_complaint_at, last_bounce_reason, updated_at)
             VALUES ($1, $2, $3, $4, now())
             ON CONFLICT (email) DO UPDATE SET
                 last_hard_bounce_at    = GREATEST(email_health.last_hard_bounce_at, EXCLUDED.last_hard_bounce_at),
                 last_spam_complaint_at = GREATEST(email_health.last_spam_complaint_at, EXCLUDED.last_spam_complaint_at),
                 last_bounce_reason     = COALESCE(EXCLUDED.last_bounce_reason, email_health.last_bounce_reason),
                 updated_at             = now()`,
            [email, agg.hardBounceAt, agg.spamComplaintAt, agg.reason],
        );
    }
    return byEmail.size;
}
