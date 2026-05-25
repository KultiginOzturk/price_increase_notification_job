/**
 * Deliverability tag reconciliation for inp_account_tags.
 *
 * Materializes the email_health verdict for a cohort of accounts into
 * reason-coded tags. One distinct tag_type_key per reason:
 *
 *   no_email              — no email address on file
 *   email_invalid         — email failed MailerSend verification (hard fail)
 *   email_hard_bounced    — email hard-bounced via the MailerSend Activity API
 *   email_spam_complaint  — recipient marked a message as spam
 *
 * SCD-style lifecycle: a tag that no longer applies (account became
 * deliverable, or its reason changed) is end-dated (is_current=FALSE,
 * end_date=today) rather than deleted.
 *
 * OWNERSHIP BOUNDARY: this module only ever reads/writes/end-dates tags whose
 * `source` column equals the value passed in (default 'email_health'). Legacy
 * client-created / manual tags (source='manual' etc.) are never touched — the
 * consolidation of those into this scheme is a separate migration.
 */

import { query, withTransaction } from './postgres.js';
import { normalizeEmail, lookupEmailHealth, evaluateEmailHealth } from './emailHealth.js';

// The tags this module owns. evaluateEmailHealth reasons map onto three of
// them; the fourth (no_email) is decided from the absence of an address.
export const MANAGED_DELIVERABILITY_KEYS = Object.freeze([
    'no_email',
    'email_invalid',
    'email_hard_bounced',
    'email_spam_complaint',
]);

const REASON_TO_KEY = Object.freeze({
    verification_failed: 'email_invalid',
    hard_bounced: 'email_hard_bounced',
    spam_complaint: 'email_spam_complaint',
});

const DEFAULT_SOURCE = 'email_health';

const fmtDate = (value) => {
    if (!value) return 'unknown date';
    try {
        return new Date(value).toISOString().slice(0, 10);
    } catch {
        return String(value);
    }
};

/**
 * The deliverability tag an account should carry, given its resolved email and
 * email_health row. Returns { key, note } — key is null when the account is
 * reachable by email (no deliverability tag needed).
 */
export function desiredDeliverabilityTag({ email, healthRow }) {
    if (!email) {
        return { key: 'no_email', note: 'no email address on file' };
    }
    const { deliverable, reason } = evaluateEmailHealth(healthRow);
    if (deliverable) return { key: null, note: null };

    const key = REASON_TO_KEY[reason] || null;
    if (!key) return { key: null, note: null };

    if (key === 'email_invalid') {
        return { key, note: `verification verdict: ${healthRow?.verification_status || 'unknown'}` };
    }
    if (key === 'email_hard_bounced') {
        return { key, note: `hard bounce observed ${fmtDate(healthRow?.last_hard_bounce_at)}` };
    }
    return { key, note: `spam complaint observed ${fmtDate(healthRow?.last_spam_complaint_at)}` };
}

/**
 * Reconcile managed tags for one client's accounts in a single transaction.
 *
 * @param {Object} args
 * @param {string} args.client
 * @param {Array<{masterAccountId: string, desired: {key: string|null, note: string|null}}>} args.accounts
 * @param {string} args.source
 * @param {string} args.actor
 */
async function reconcileClientTags({ client, accounts, source, actor }) {
    const result = { inserted: 0, endDated: 0, unchanged: 0, insertedByKey: {} };
    const accountIds = accounts.map((a) => String(a.masterAccountId));
    if (accountIds.length === 0) return result;

    const existing = await query(
        `SELECT tag_id, master_account_id, tag_type_key
           FROM inp_account_tags
          WHERE client = $1
            AND master_account_id = ANY($2::text[])
            AND tag_type_key = ANY($3::text[])
            AND source = $4
            AND is_current = TRUE`,
        [client, accountIds, MANAGED_DELIVERABILITY_KEYS, source],
    );

    const currentByAccount = new Map(); // master_account_id → [{ tagId, key }]
    for (const row of existing.rows) {
        const mid = String(row.master_account_id);
        if (!currentByAccount.has(mid)) currentByAccount.set(mid, []);
        currentByAccount.get(mid).push({ tagId: row.tag_id, key: row.tag_type_key });
    }

    const tagIdsToEndDate = [];
    const rowsToInsert = []; // { masterAccountId, key, note }

    for (const account of accounts) {
        const mid = String(account.masterAccountId);
        const desiredKey = account.desired?.key || null;
        const current = currentByAccount.get(mid) || [];
        const alreadyHasDesired = desiredKey && current.some((c) => c.key === desiredKey);

        // End-date every managed tag that isn't the desired one (stale reason,
        // or account became deliverable so desiredKey is null).
        for (const tag of current) {
            if (tag.key !== desiredKey) tagIdsToEndDate.push(tag.tagId);
        }

        if (desiredKey && !alreadyHasDesired) {
            rowsToInsert.push({ masterAccountId: mid, key: desiredKey, note: account.desired?.note || null });
        } else if (desiredKey && alreadyHasDesired) {
            result.unchanged++;
        }
    }

    if (tagIdsToEndDate.length === 0 && rowsToInsert.length === 0) return result;

    await withTransaction(async (tx) => {
        if (tagIdsToEndDate.length > 0) {
            await tx.query(
                `UPDATE inp_account_tags
                    SET is_current = FALSE,
                        end_date   = CURRENT_DATE,
                        updated_at = now(),
                        updated_by = $2
                  WHERE tag_id = ANY($1::int[])`,
                [tagIdsToEndDate, actor],
            );
        }
        if (rowsToInsert.length > 0) {
            const valuesSql = [];
            const params = [];
            let i = 1;
            for (const row of rowsToInsert) {
                valuesSql.push(`($${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++}, $${i++})`);
                params.push(client, row.masterAccountId, row.key, source, actor, actor, row.note);
            }
            await tx.query(
                `INSERT INTO inp_account_tags
                     (client, master_account_id, tag_type_key, source, created_by, updated_by, notes)
                 VALUES ${valuesSql.join(', ')}`,
                params,
            );
        }
    });

    result.endDated = tagIdsToEndDate.length;
    result.inserted = rowsToInsert.length;
    for (const row of rowsToInsert) {
        result.insertedByKey[row.key] = (result.insertedByKey[row.key] || 0) + 1;
    }
    return result;
}

/**
 * Reconcile deliverability tags for a whole cohort.
 *
 * @param {Object} args
 * @param {Array<{client: string, masterAccountId: string, email: string|null}>} args.targets
 * @param {string} [args.source]  tag ownership marker (default 'email_health')
 * @param {string} [args.actor]   created_by/updated_by audit label
 * @returns {Promise<{clients: number, accounts: number, inserted: number, endDated: number, unchanged: number, insertedByKey: object}>}
 */
export async function reconcileCohortDeliverabilityTags({ targets, source = DEFAULT_SOURCE, actor = 'system' }) {
    const totals = { clients: 0, accounts: (targets || []).length, inserted: 0, endDated: 0, unchanged: 0, insertedByKey: {} };
    if (!targets || targets.length === 0) return totals;

    const emails = [...new Set(targets.map((t) => normalizeEmail(t.email)).filter(Boolean))];
    const health = await lookupEmailHealth(emails);

    const byClient = new Map();
    for (const target of targets) {
        const email = normalizeEmail(target.email);
        const desired = desiredDeliverabilityTag({
            email,
            healthRow: email ? health.get(email) : null,
        });
        if (!byClient.has(target.client)) byClient.set(target.client, []);
        byClient.get(target.client).push({ masterAccountId: target.masterAccountId, desired });
    }

    totals.clients = byClient.size;
    for (const [client, accounts] of byClient) {
        const result = await reconcileClientTags({ client, accounts, source, actor });
        totals.inserted += result.inserted;
        totals.endDated += result.endDated;
        totals.unchanged += result.unchanged;
        for (const [key, count] of Object.entries(result.insertedByKey)) {
            totals.insertedByKey[key] = (totals.insertedByKey[key] || 0) + count;
        }
    }
    return totals;
}
