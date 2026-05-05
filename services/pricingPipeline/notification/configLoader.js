/**
 * Load the active notification config for an office.
 *
 * Mirror of pricing/configLoader.js + scheduling/configLoader.js. Versioned:
 * callers pin the version on every notification_run row + every BQ
 * notification_send_event payload they emit.
 */

import { query as pgQuery } from '../../../lib/postgres.js';

/**
 * @typedef {Object} NotificationConfig
 * @property {number} versionId
 * @property {string|null} sendFromAddress
 * @property {string|null} sendFromName
 * @property {number|null} prePushLeadDays
 * @property {number|null} postPushSendOffsetDays
 * @property {boolean} requiresPrePushEmail
 * @property {string|null} templateChoice
 * @property {Array<Object>} suppressionRules
 * @property {Array<string>} bccAddresses
 */

/**
 * @param {string} officeKey
 * @returns {Promise<NotificationConfig>}
 */
export async function loadActiveNotificationConfig(officeKey) {
    const result = await pgQuery(
        `SELECT
             config_version_id,
             send_from_address, send_from_name,
             pre_push_lead_days, post_push_send_offset_days,
             requires_pre_push_email,
             template_choice, suppression_rules, bcc_addresses
         FROM office_notification_config
         WHERE office_key = $1`,
        [officeKey],
    );
    if (result.rows.length === 0) {
        throw new Error(
            `No notification config found for office '${officeKey}'. ` +
            `Run scripts/backfill_office_notification_config.mjs --office=${officeKey} first.`,
        );
    }
    const row = result.rows[0];
    return {
        versionId:                Number(row.config_version_id),
        sendFromAddress:          row.send_from_address,
        sendFromName:             row.send_from_name,
        prePushLeadDays:          parseIntOrNull(row.pre_push_lead_days),
        postPushSendOffsetDays:   parseIntOrNull(row.post_push_send_offset_days),
        requiresPrePushEmail:     Boolean(row.requires_pre_push_email),
        templateChoice:           row.template_choice,
        suppressionRules:         Array.isArray(row.suppression_rules) ? row.suppression_rules : [],
        bccAddresses:             Array.isArray(row.bcc_addresses)
            ? row.bcc_addresses.map(String)
            : [],
    };
}

function parseIntOrNull(value) {
    if (value === null || value === undefined) return null;
    const n = typeof value === 'number' ? value : parseInt(value, 10);
    return Number.isInteger(n) ? n : null;
}
