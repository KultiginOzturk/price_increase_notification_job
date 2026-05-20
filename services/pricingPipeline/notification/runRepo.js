/**
 * Postgres helpers for notification_run + notification_recipient.
 *
 * The notification service (priceIncreaseNotificationService.js) opens a
 * notification_run when a batch starts, inserts a notification_recipient row
 * per intended send (with status='queued'), then updates each row to
 * sent/failed/suppressed as the MailerSend response lands.
 *
 * No transaction wrapping at the helper layer — caller decides. Every
 * mutation here is idempotent-ish (UPDATE by id, INSERT one row at a time)
 * so a partial-batch retry doesn't corrupt state.
 */

import { query as pgQuery } from '../../../lib/postgres.js';

/**
 * @typedef {'pre_push' | 'post_push' | 'manual'} NotificationMode
 * @typedef {'draft' | 'pending' | 'sending' | 'completed' | 'failed' | 'cancelled'} NotificationRunStatus
 * @typedef {'queued' | 'sent' | 'failed' | 'suppressed_unsubscribed' | 'suppressed_no_email' | 'deduped_already_sent' | 'test'} NotificationRecipientStatus
 */

/**
 * Open a new notification_run row.
 *
 * @param {Object} args
 * @param {number} args.planId
 * @param {string} args.officeKey
 * @param {NotificationMode} args.mode
 * @param {string} args.effectivePeriod
 * @param {number} args.configVersionId
 * @param {string} args.triggeredBy
 * @param {number|null} [args.pushRunId]
 * @returns {Promise<{ id: number }>}
 */
export async function openNotificationRun({
    planId,
    officeKey,
    mode,
    effectivePeriod,
    configVersionId,
    triggeredBy,
    pushRunId = null,
}) {
    const result = await pgQuery(
        `INSERT INTO notification_run
            (plan_id, office_key, push_run_id, mode, effective_period,
             triggered_by, config_version_id, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'sending')
         RETURNING id`,
        [planId, officeKey, pushRunId, mode, effectivePeriod, triggeredBy, configVersionId],
    );
    return { id: Number(result.rows[0].id) };
}

/**
 * Flip an app-created notification_run from 'draft'/'pending' to 'sending'.
 *
 * Used when the price-increase-notification job is launched with
 * NOTIFICATION_RUN_ID — the client-ops-pilot /launch/notify trigger creates
 * the run row up front, and the job reuses it rather than opening its own.
 * No-op once the run is past pending (already sending / completed / failed /
 * cancelled), so a retry can't resurrect a finished run.
 *
 * @param {number} id
 */
export async function markNotificationRunSending(id) {
    await pgQuery(
        `UPDATE notification_run
            SET status = 'sending'
          WHERE id = $1 AND status IN ('draft', 'pending')`,
        [id],
    );
}

/**
 * Insert a notification_recipient row (status='queued' by default).
 *
 * Returns the new id so the caller can update it after MailerSend resolves.
 *
 * @param {Object} args
 * @param {number} args.notificationRunId
 * @param {string} args.masterAccountId
 * @param {string} args.recipientEmail
 * @param {NotificationRecipientStatus} [args.status]      defaults 'queued'
 * @param {string} [args.statusNote]
 * @returns {Promise<{ id: number }>}
 */
export async function insertNotificationRecipient({
    notificationRunId,
    masterAccountId,
    recipientEmail,
    status = 'queued',
    statusNote = null,
}) {
    const result = await pgQuery(
        `INSERT INTO notification_recipient
            (notification_run_id, master_account_id, recipient_email, status, status_note)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (notification_run_id, master_account_id) DO UPDATE SET
            recipient_email = EXCLUDED.recipient_email,
            status          = EXCLUDED.status,
            status_note     = EXCLUDED.status_note
         RETURNING id`,
        [notificationRunId, masterAccountId, recipientEmail, status, statusNote],
    );
    return { id: Number(result.rows[0].id) };
}

/**
 * Update an existing notification_recipient row with the post-send outcome.
 *
 * @param {Object} args
 * @param {number} args.id
 * @param {NotificationRecipientStatus} args.status
 * @param {string} [args.statusNote]
 * @param {string} [args.providerMessageId]
 * @param {string} [args.bqPayloadDataset]
 * @param {string} [args.bqPayloadRowId]
 * @param {Date} [args.sentAt]
 */
export async function updateNotificationRecipientOutcome({
    id,
    status,
    statusNote = null,
    providerMessageId = null,
    bqPayloadDataset = null,
    bqPayloadRowId = null,
    sentAt = null,
}) {
    await pgQuery(
        `UPDATE notification_recipient SET
            status               = $2,
            status_note          = COALESCE($3, status_note),
            provider_message_id  = COALESCE($4, provider_message_id),
            bq_payload_dataset   = COALESCE($5, bq_payload_dataset),
            bq_payload_row_id    = COALESCE($6, bq_payload_row_id),
            sent_at              = COALESCE($7, sent_at)
          WHERE id = $1`,
        [id, status, statusNote, providerMessageId, bqPayloadDataset, bqPayloadRowId, sentAt],
    );
}

/**
 * Close a notification_run with final counts. Fold whatever counts the
 * caller has accumulated into the appropriate columns.
 *
 * @param {Object} args
 * @param {number} args.id
 * @param {number} args.totalRecipients
 * @param {number} args.succeededCount
 * @param {number} args.failedCount
 * @param {number} args.suppressedCount
 * @param {NotificationRunStatus} [args.status]    defaults 'completed' when failedCount === 0, else 'failed' if everything failed.
 * @param {string} [args.errorText]
 */
export async function closeNotificationRun({
    id,
    totalRecipients,
    succeededCount,
    failedCount,
    suppressedCount,
    status,
    errorText = null,
}) {
    const finalStatus = status
        ?? (totalRecipients === 0
            ? 'completed'
            : succeededCount === 0 && failedCount > 0
                ? 'failed'
                : 'completed');
    await pgQuery(
        `UPDATE notification_run SET
            total_recipients = $2,
            succeeded_count  = $3,
            failed_count     = $4,
            suppressed_count = $5,
            status           = $6,
            completed_at     = NOW(),
            error_text       = COALESCE($7, error_text)
          WHERE id = $1`,
        [id, totalRecipients, succeededCount, failedCount, suppressedCount, finalStatus, errorText],
    );
}
