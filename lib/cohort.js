/**
 * Cohort resolution — shared by verify-emails.js and sync-email-status.js.
 *
 * Resolves the same recipient set the send job (runDuePrePushNotifications)
 * would process, so verification, status reconciliation, and the actual send
 * all agree on "which accounts are in this batch".
 *
 * Returns one entry per master account: { client, masterAccountId, email }.
 * email is the normalized resolved contact address, or null when the account
 * has no usable email on file.
 */

import { normalizeEmail } from './emailHealth.js';
import {
    findDuePrePushNotificationPeriods,
    buildPrePushNotificationTargets,
} from '../services/priceIncreaseNotificationService.js';
import { buildPlanV2PricePushSource } from '../services/planV2PricePushService.js';

/**
 * @param {Object} args
 * @param {string[]} [args.clients]      client allowlist (empty/undefined → all due)
 * @param {string[]} [args.accountIds]   master_account_id allowlist (app-triggered cohort)
 * @param {string|null} [args.targetDate] YYYY-MM-DD (default: today)
 * @returns {Promise<Array<{client: string, masterAccountId: string, email: string|null}>>}
 */
export async function resolveCohortTargets({ clients = [], accountIds = [], targetDate = null } = {}) {
    const periods = await findDuePrePushNotificationPeriods({
        targetDate,
        clients: clients.length > 0 ? clients : null,
        // An explicit account cohort is an app-triggered batch — bypass the
        // notice/effective-date window, same as the send job does.
        ignoreDueDate: accountIds.length > 0,
    });

    const accountSet = accountIds.length > 0 ? new Set(accountIds.map(String)) : null;
    const targets = [];
    const seen = new Set();

    for (const period of periods) {
        const batch = await buildPlanV2PricePushSource({
            client: period.client,
            effectivePeriod: period.effectivePeriod,
        });
        if (!batch.plan || (batch.accounts || []).length === 0) continue;

        const periodTargets = await buildPrePushNotificationTargets({ client: period.client, batch });
        for (const target of periodTargets) {
            const masterAccountId = String(target.masterAccountId);
            if (accountSet && !accountSet.has(masterAccountId)) continue;
            const dedupeKey = `${period.client}::${masterAccountId}`;
            if (seen.has(dedupeKey)) continue;
            seen.add(dedupeKey);
            targets.push({
                client: period.client,
                masterAccountId,
                email: normalizeEmail(target.email),
            });
        }
    }

    return targets;
}
