/**
 * Capture the rendered email payload to BQ as a notification_send_event row.
 *
 * Different from pricing/scheduling recorders: this writes to the per-office
 * `notification_send_event` table (not `decision_event`). The schema is
 * defined in scripts/bootstrap_office_event_tables.mjs and includes the full
 * HTML body, template variables, provider request/response — so the trace UI
 * can show the operator the *exact email that was sent* alongside the
 * notification_recipient row that pointed at it.
 *
 * Volume: pre-push + post-push batches typically send 50–500 emails each, a
 * few times per month per office. Batching matters less than for pricing —
 * single-row inserts are fine. We expose both single (`emitNotificationSendEvent`)
 * and batched (`emitNotificationSendEventsBatch`) entry points so the
 * notification service can pick whichever fits.
 */

import { bigqueryClient } from '../../../utils/bigquery.js';
import logger from '../../../lib/logger.js';

const NOTIFICATION_SEND_EVENT_TABLE = 'notification_send_event';

/**
 * @typedef {Object} NotificationSendEventArgs
 * @property {number} notification_run_id
 * @property {number|null} notification_recipient_id   null until the PG insert lands; OK to set later via UPDATE
 * @property {string} office_key
 * @property {string} master_account_id
 * @property {string} recipient_email
 * @property {string} provider                          'mailersend' today
 * @property {string|null} provider_message_id
 * @property {string} subject
 * @property {string} html_body
 * @property {string|null} text_body
 * @property {string|null} template_id
 * @property {Object} template_variables
 * @property {Object} provider_request
 * @property {Object} provider_response
 * @property {Date} sent_at
 */

/**
 * Insert one notification_send_event row.
 *
 * @param {NotificationSendEventArgs} args
 * @returns {Promise<void>}
 */
export async function emitNotificationSendEvent(args) {
    const datasetId = `rcp_${args.office_key}`;
    const table = bigqueryClient.dataset(datasetId).table(NOTIFICATION_SEND_EVENT_TABLE);
    const row = toRow(args);

    try {
        await table.insert([row]);
    } catch (err) {
        logger.error({
            err: err?.message || String(err),
            row_errors: err?.errors,
            dataset: datasetId,
            table: NOTIFICATION_SEND_EVENT_TABLE,
            office_key: args.office_key,
            master_account_id: args.master_account_id,
            recipient_email: args.recipient_email,
        }, '[notification-send-event] insert failed');
        throw err;
    }
}

/**
 * Insert many rows in one call. All must share the same office_key.
 *
 * @param {NotificationSendEventArgs[]} eventsByOffice
 * @returns {Promise<void>}
 */
export async function emitNotificationSendEventsBatch(eventsByOffice) {
    if (!Array.isArray(eventsByOffice) || eventsByOffice.length === 0) return;

    const officeKeys = new Set(eventsByOffice.map((e) => e?.office_key));
    if (officeKeys.size !== 1) {
        throw new Error(
            `emitNotificationSendEventsBatch requires a single office per call; got ${officeKeys.size} ` +
            `(office_keys: ${[...officeKeys].join(', ')}). Group by office_key on the caller side.`,
        );
    }
    const officeKey = [...officeKeys][0];

    const rows = eventsByOffice.map(toRow);
    const datasetId = `rcp_${officeKey}`;
    const table = bigqueryClient.dataset(datasetId).table(NOTIFICATION_SEND_EVENT_TABLE);

    try {
        await table.insert(rows);
    } catch (err) {
        logger.error({
            err: err?.message || String(err),
            row_errors: err?.errors,
            dataset: datasetId,
            table: NOTIFICATION_SEND_EVENT_TABLE,
            office_key: officeKey,
            row_count: rows.length,
        }, '[notification-send-event] batch insert failed');
        throw err;
    }
}

function toRow(args) {
    if (!args.office_key) throw new Error('notification_send_event row requires office_key');
    if (!args.master_account_id) throw new Error('notification_send_event row requires master_account_id');
    if (!args.recipient_email) throw new Error('notification_send_event row requires recipient_email');
    return {
        notification_run_id:        args.notification_run_id ?? null,
        notification_recipient_id:  args.notification_recipient_id ?? null,
        master_account_id:          args.master_account_id,
        recipient_email:            args.recipient_email,
        provider:                   args.provider ?? 'mailersend',
        provider_message_id:        args.provider_message_id ?? null,
        subject:                    args.subject ?? null,
        html_body:                  args.html_body ?? null,
        text_body:                  args.text_body ?? null,
        template_id:                args.template_id ?? null,
        template_variables:         args.template_variables ? JSON.stringify(args.template_variables) : null,
        provider_request:           args.provider_request ? JSON.stringify(args.provider_request) : null,
        provider_response:          args.provider_response ? JSON.stringify(args.provider_response) : null,
        sent_at:                    (args.sent_at ?? new Date()).toISOString(),
    };
}
