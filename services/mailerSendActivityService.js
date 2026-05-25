/**
 * MailerSend Activity ETL — pulls delivery-failure events into email_health.
 *
 * syncEmailStatus() walks every MailerSend domain on the account, pages through
 * the Activity API for the lookback window, and records `hard_bounced` and
 * `spam_complaints` events into email_health.
 *
 * No high-water-mark cursor by design: the email_health upsert keeps only the
 * latest timestamp per event type (GREATEST), so re-scanning an overlapping
 * window is harmless. The operator just re-runs this over the days following a
 * send — bounces and complaints trickle in for a while after delivery.
 */

import { MailerSend } from 'mailersend';
import logger from '../lib/logger.js';
import { recordBounceEvents, normalizeEmail } from '../lib/emailHealth.js';

const MAILERSEND_API_KEY = process.env.MAILERSEND_API_KEY;
const mailerSend = MAILERSEND_API_KEY ? new MailerSend({ apiKey: MAILERSEND_API_KEY }) : null;

// Activity event types that mean "this address can't be reached by email".
const TRACKED_EVENTS = ['hard_bounced', 'spam_complaints'];

const PAGE_LIMIT = 100;          // MailerSend Activity API max page size
const MAX_PAGES = 200;           // safety cap against runaway pagination
const DEFAULT_LOOKBACK_DAYS = Math.max(1, Number(process.env.EMAIL_STATUS_LOOKBACK_DAYS) || 30);

/**
 * List the MailerSend domains to scan. Optional MAILERSEND_SYNC_DOMAINS env
 * (comma-separated ids or names) narrows it; otherwise all domains are scanned.
 */
async function listDomains() {
    const response = await mailerSend.email.domain.list({ limit: 100 });
    const domains = response?.body?.data || [];
    const filter = (process.env.MAILERSEND_SYNC_DOMAINS || '')
        .split(',')
        .map((value) => value.trim().toLowerCase())
        .filter(Boolean);
    if (filter.length === 0) return domains;
    return domains.filter((domain) =>
        filter.includes(String(domain.id).toLowerCase())
        || filter.includes(String(domain.name).toLowerCase()));
}

async function fetchActivityPage(domainId, dateFromSec, dateToSec, page) {
    const response = await mailerSend.email.activity.domain(domainId, {
        date_from: dateFromSec,
        date_to: dateToSec,
        event: TRACKED_EVENTS,
        page,
        limit: PAGE_LIMIT,
    });
    return response?.body?.data || [];
}

/**
 * Pull hard-bounce / spam-complaint activity into email_health.
 *
 * @param {Object} [args]
 * @param {number|null} [args.sinceDays]  lookback window in days (default EMAIL_STATUS_LOOKBACK_DAYS / 30)
 * @returns {Promise<{ domains: Array, totalHardBounced: number, totalSpamComplaints: number }>}
 */
export async function syncEmailStatus({ sinceDays = null } = {}) {
    if (!mailerSend) {
        throw new Error('[sync-email-status] MAILERSEND_API_KEY is not set');
    }

    const lookbackDays = sinceDays != null ? Math.max(1, sinceDays) : DEFAULT_LOOKBACK_DAYS;
    const now = Date.now();
    const fromMs = now - lookbackDays * 86400 * 1000;
    const dateFromSec = Math.floor(fromMs / 1000);
    const dateToSec = Math.floor(now / 1000);

    const domains = await listDomains();
    if (domains.length === 0) {
        logger.warn({}, '[sync-email-status] no MailerSend domains found to scan');
    }

    const summary = { domains: [], totalHardBounced: 0, totalSpamComplaints: 0 };

    for (const domain of domains) {
        const bounceEvents = [];
        let fetched = 0;

        for (let page = 1; page <= MAX_PAGES; page++) {
            let items;
            try {
                items = await fetchActivityPage(String(domain.id), dateFromSec, dateToSec, page);
            } catch (error) {
                const detail = error?.body?.message
                    ?? (typeof error?.body === 'string' ? error.body : null)
                    ?? error?.message
                    ?? String(error);
                logger.warn(
                    { err: detail, domain: domain.name, page },
                    '[sync-email-status] activity fetch failed; stopping this domain',
                );
                break;
            }

            fetched += items.length;
            for (const item of items) {
                const email = normalizeEmail(
                    item?.email?.recipient?.email || item?.recipient?.email,
                );
                if (!email) continue;
                const occurredAt = item?.created_at ? new Date(item.created_at) : new Date();
                if (item?.type === 'hard_bounced') {
                    bounceEvents.push({
                        email,
                        eventType: 'hard_bounced',
                        occurredAt,
                        reason: item?.email?.status || 'hard_bounced',
                    });
                } else if (item?.type === 'spam_complaints') {
                    bounceEvents.push({
                        email,
                        eventType: 'spam_complaints',
                        occurredAt,
                        reason: 'spam_complaint',
                    });
                }
            }

            if (items.length < PAGE_LIMIT) break;
        }

        if (bounceEvents.length > 0) {
            await recordBounceEvents(bounceEvents);
        }

        const hardBounced = bounceEvents.filter((e) => e.eventType === 'hard_bounced').length;
        const spamComplaints = bounceEvents.filter((e) => e.eventType === 'spam_complaints').length;
        summary.totalHardBounced += hardBounced;
        summary.totalSpamComplaints += spamComplaints;
        summary.domains.push({
            id: domain.id,
            name: domain.name,
            from: new Date(fromMs).toISOString(),
            to: new Date(now).toISOString(),
            events: fetched,
            hardBounced,
            spamComplaints,
        });
    }

    return summary;
}
