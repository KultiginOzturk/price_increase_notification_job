/**
 * Email Service - MailerSend Integration
 * 
 * Handles sending emails for scheduled reports and customer notifications.
 * Uses MailerSend as the email provider.
 */

import { MailerSend, EmailParams, Sender, Recipient } from 'mailersend';

// Initialize MailerSend with API key
const MAILERSEND_API_KEY = process.env.MAILERSEND_API_KEY;
const FROM_EMAIL = process.env.MAILERSEND_FROM_EMAIL || 'reports@pestnotifications.com';
const FROM_NAME = process.env.MAILERSEND_FROM_NAME || 'Pest Analytics Reports';
// Domain for price-increase notification "from" address (client@domain). Set for testing to a verified MailerSend test domain.
const NOTIFICATION_FROM_DOMAIN = process.env.MAILERSEND_FROM_DOMAIN || 'pestnotifications.com';
const MAILERSEND_MAX_REQUESTS_PER_MINUTE = Math.max(1, Number(process.env.MAILERSEND_MAX_REQUESTS_PER_MINUTE) || 60);
const MAILERSEND_RATE_LIMIT_WINDOW_MS = 60 * 1000;

let mailerSend = null;
const mailerSendRequestTimestamps = [];

if (MAILERSEND_API_KEY) {
  mailerSend = new MailerSend({ apiKey: MAILERSEND_API_KEY });
  console.log('[EmailService] MailerSend initialized');
} else {
  console.warn('[EmailService] MAILERSEND_API_KEY not set - emails will be logged only');
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitForMailerSendRateLimitSlot() {
  while (true) {
    const now = Date.now();

    while (
      mailerSendRequestTimestamps.length > 0 &&
      now - mailerSendRequestTimestamps[0] >= MAILERSEND_RATE_LIMIT_WINDOW_MS
    ) {
      mailerSendRequestTimestamps.shift();
    }

    if (mailerSendRequestTimestamps.length < MAILERSEND_MAX_REQUESTS_PER_MINUTE) {
      mailerSendRequestTimestamps.push(now);
      return;
    }

    const oldestTimestamp = mailerSendRequestTimestamps[0];
    const waitMs = Math.max(250, MAILERSEND_RATE_LIMIT_WINDOW_MS - (now - oldestTimestamp) + 50);
    console.log(`[EmailService] Rate limit reached, waiting ${waitMs}ms before next MailerSend request`);
    await sleep(waitMs);
  }
}

async function sendMailerSendRequest(emailParams) {
  await waitForMailerSendRateLimitSlot();
  return mailerSend.email.send(emailParams);
}

/**
 * Send a scheduled report email
 * 
 * @param {Object} options
 * @param {string[]} options.recipients - Array of email addresses
 * @param {string} options.reportName - Name of the report
 * @param {string} options.client - Client code
 * @param {string} options.downloadUrl - Signed URL for downloading the report
 * @param {string} options.expiresAt - When the download link expires
 * @returns {Promise<{success: boolean, messageId?: string, error?: string}>}
 */
export async function sendReportEmail({ recipients, reportName, client, downloadUrl, expiresAt }) {
  const subject = `📊 Your Scheduled Report: ${reportName} (${client})`;
  
  const htmlContent = `
    <!DOCTYPE html>
    <html>
    <head>
      <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #1a1a2e; color: white; padding: 20px; border-radius: 8px 8px 0 0; }
        .content { background: #f8f9fa; padding: 20px; border: 1px solid #e9ecef; }
        .button { display: inline-block; background: #2563eb; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; margin: 16px 0; }
        .footer { padding: 16px; font-size: 12px; color: #6b7280; text-align: center; }
        .info { background: #e0f2fe; border: 1px solid #7dd3fc; padding: 12px; border-radius: 6px; margin: 16px 0; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h1 style="margin: 0; font-size: 24px;">Pest Analytics</h1>
          <p style="margin: 8px 0 0 0; opacity: 0.9;">Scheduled Report Ready</p>
        </div>
        <div class="content">
          <h2 style="margin-top: 0;">Your Report is Ready</h2>
          <p>Your scheduled report <strong>${reportName}</strong> for client <strong>${client}</strong> has been generated and is ready for download.</p>
          
          <a href="${downloadUrl}" class="button">Download Report</a>
          
          <div class="info">
            <strong>📋 Report Details:</strong><br>
            • Report: ${reportName}<br>
            • Client: ${client}<br>
            • Generated: ${new Date().toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' })}
          </div>
          
          <p style="font-size: 14px; color: #6b7280;">
            <strong>Note:</strong> This download link will expire on ${expiresAt}. 
            Please download your report before then.
          </p>
        </div>
        <div class="footer">
          <p>This is an automated email from Pest Analytics.</p>
          <p>You received this email because you are subscribed to scheduled reports.</p>
        </div>
      </div>
    </body>
    </html>
  `;

  const textContent = `
Your Scheduled Report is Ready

Report: ${reportName}
Client: ${client}
Generated: ${new Date().toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' })}

Download your report here:
${downloadUrl}

Note: This download link will expire on ${expiresAt}.

---
This is an automated email from Pest Analytics.
  `.trim();

  // If MailerSend is not configured, just log and return success
  if (!MAILERSEND_API_KEY || !mailerSend) {
    console.log('[EmailService] Would send email to:', recipients);
    console.log('[EmailService] Subject:', subject);
    console.log('[EmailService] Download URL:', downloadUrl);
    return { success: true, messageId: 'mock-' + Date.now() };
  }

  try {
    const from = new Sender(FROM_EMAIL, FROM_NAME);
    const to = recipients.map((email) => new Recipient(email));

    const emailParams = new EmailParams()
      .setFrom(from)
      .setTo(to)
      .setSubject(subject)
      .setText(textContent)
      .setHtml(htmlContent);

    const response = await sendMailerSendRequest(emailParams);
    const messageId = response?.messageId || response?.message_id || 'unknown';

    console.log(`[EmailService] Email sent successfully to ${recipients.length} recipients. MessageId: ${messageId}`);
    
    return { success: true, messageId };
  } catch (error) {
    // MailerSend SDK throws { body, statusCode } on API errors (not Error instances)
    const body = error?.response?.body ?? error?.body;
    const msg = body?.message ?? (typeof body === 'string' ? body : JSON.stringify(body ?? error?.message ?? error));
    console.error('[EmailService] Failed to send email:', msg);
    return { success: false, error: msg };
  }
}

/**
 * Send a report failure notification
 * 
 * @param {Object} options
 * @param {string[]} options.recipients - Array of email addresses
 * @param {string} options.reportName - Name of the report
 * @param {string} options.client - Client code
 * @param {string} options.errorMessage - Error description
 */
export async function sendReportFailureEmail({ recipients, reportName, client, errorMessage }) {
  const subject = `⚠️ Report Generation Failed: ${reportName} (${client})`;
  
  const htmlContent = `
    <!DOCTYPE html>
    <html>
    <head>
      <style>
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; line-height: 1.6; color: #333; }
        .container { max-width: 600px; margin: 0 auto; padding: 20px; }
        .header { background: #dc2626; color: white; padding: 20px; border-radius: 8px 8px 0 0; }
        .content { background: #f8f9fa; padding: 20px; border: 1px solid #e9ecef; }
        .error { background: #fef2f2; border: 1px solid #fecaca; padding: 12px; border-radius: 6px; margin: 16px 0; color: #991b1b; }
        .footer { padding: 16px; font-size: 12px; color: #6b7280; text-align: center; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h1 style="margin: 0; font-size: 24px;">Pest Analytics</h1>
          <p style="margin: 8px 0 0 0; opacity: 0.9;">Report Generation Failed</p>
        </div>
        <div class="content">
          <h2 style="margin-top: 0;">Report Generation Failed</h2>
          <p>Unfortunately, your scheduled report <strong>${reportName}</strong> for client <strong>${client}</strong> could not be generated.</p>
          
          <div class="error">
            <strong>Error:</strong><br>
            ${errorMessage}
          </div>
          
          <p>The system will automatically retry on the next scheduled run. If this issue persists, please contact support.</p>
        </div>
        <div class="footer">
          <p>This is an automated email from Pest Analytics.</p>
        </div>
      </div>
    </body>
    </html>
  `;

  if (!MAILERSEND_API_KEY || !mailerSend) {
    console.log('[EmailService] Would send failure email to:', recipients);
    return { success: true };
  }

  try {
    const from = new Sender(FROM_EMAIL, FROM_NAME);
    const to = recipients.map((email) => new Recipient(email));

    const emailParams = new EmailParams()
      .setFrom(from)
      .setTo(to)
      .setSubject(subject)
      .setHtml(htmlContent);

    await sendMailerSendRequest(emailParams);
    return { success: true };
  } catch (error) {
    console.error('[EmailService] Failed to send failure notification:', error.message);
    return { success: false, error: error.message };
  }
}

/**
 * Send a price increase notification email to a customer
 * 
 * @param {Object} options
 * @param {string} options.recipient - Email address
 * @param {string} options.recipientName - Customer display name (e.g., "John Smith")
 * @param {string} [options.customerName] - Preferred customer display name
 * @param {string} options.accountName - Master account display name
 * @param {string} options.clientName - Client code (e.g., "BHB")
 * @param {Array<{serviceTypeName: string, currentPrice: number, newPrice: number, increaseAmount: number, increasePct: number}>} [options.services]
 * @param {Array<{serviceTypeName: string, oldCharge: number, newCharge: number, increasePct: number}>} [options.subscriptions]
 * @param {string} [options.effectiveDate] - Effective date in YYYY-MM-DD format
 * @param {string} options.unsubscribeUrl - Full URL for unsubscribe link
 * @param {string} [options.fromEmail] - Override from email (default: {clientName}@pestanalytics.com)
 * @param {string} [options.fromName] - Override from name (default: clientName)
 * @param {Object} [options.branding] - Optional branding overrides
 * @param {string} [options.branding.headerColor] - Header background color (default: #0f172a)
 * @param {string} [options.branding.accentColor] - Accent color for badges (default: #10b981)
 * @param {string} [options.branding.logoUrl] - URL to company logo image
 * @returns {Promise<{success: boolean, messageId?: string, error?: string}>}
 */
// ============================================
// Template Resolution Engine
// ============================================

/**
 * Replace {variable} placeholders in a template string.
 *
 * A referenced variable that is missing, null, or an empty string renders as
 * an empty string — NEVER the raw {token} text, so customers can never see
 * literal braces. Unknown variables (typos, dropped words) are logged so we
 * can spot templates that need updating.
 */
function resolveTemplateVariables(template, variables) {
  if (!template) return '';
  return template.replace(/\{(\w+)\}/g, (match, key) => {
    const value = variables[key];
    if (value === undefined) {
      console.warn(`[EmailService] Template references unknown variable {${key}} — rendered as empty.`);
      return '';
    }
    return value === null ? '' : String(value);
  });
}

/**
 * Format a pricing_tier code (e.g. "sqft_3k_4k") into human-readable range ("3,001–4,000 sq ft").
 * Pattern: sqft_{lo}{k?}_{hi}{k?} — lower 0 stays 0, otherwise increments by 1.
 * Unknown formats are returned as-is; null/empty → ''.
 */
function formatSquareFootageTier(tier) {
  if (!tier) return '';
  const m = /^sqft_(\d+)(k?)_(\d+)(k?)$/.exec(String(tier));
  if (!m) return String(tier);
  const lo = Number(m[1]) * (m[2] ? 1000 : 1);
  const hi = Number(m[3]) * (m[4] ? 1000 : 1);
  const loStr = lo === 0 ? '0' : (lo + 1).toLocaleString('en-US');
  const hiStr = hi.toLocaleString('en-US');
  return `${loStr}–${hiStr} sq ft`; // en dash
}

/**
 * Map a FieldRoutes billing_frequency (days between charges) to a human label
 * and a charges-per-year count.
 *
 * Returns a NULL sentinel — { label: null, chargesPerYear: null } — when the
 * cadence genuinely cannot be determined. This function NEVER throws and NEVER
 * guesses monthly; callers decide how to degrade (see {service_list}).
 *
 * `isCount: true` marks a label derived from servicesPerYear ("4 times a year")
 * rather than a real cadence — callers render it with different punctuation.
 *
 * @param {number|string|null} freq - billing_frequency in days; -1 = per visit
 * @param {number|null} servicesPerYear
 * @returns {{ label: string|null, chargesPerYear: number|null, isCount?: boolean }}
 */
function billingFrequencyToLabel(freq, servicesPerYear = null) {
  const known = {
    7:   { label: 'every week',        chargesPerYear: 52 },
    14:  { label: 'every other week',  chargesPerYear: 26 },
    28:  { label: 'every 4 weeks',     chargesPerYear: 13 },
    29:  { label: 'every month',       chargesPerYear: 12 },
    30:  { label: 'every month',       chargesPerYear: 12 },
    31:  { label: 'every month',       chargesPerYear: 12 },
    60:  { label: 'every other month', chargesPerYear: 6 },
    90:  { label: 'every quarter',     chargesPerYear: 4 },
    91:  { label: 'every quarter',     chargesPerYear: 4 },
    180: { label: 'every 6 months',    chargesPerYear: 2 },
    360: { label: 'every year',        chargesPerYear: 1 },
  };

  const code = freq == null ? null : Number(freq);
  if (code != null && Number.isFinite(code) && known[code]) {
    return { ...known[code] };
  }

  // -1 is FieldRoutes' explicit per-visit code. 0 means "not configured" — it
  // is NOT per-visit, so it falls through to the servicesPerYear path below.
  if (code === -1) {
    return { label: 'every visit', chargesPerYear: Number(servicesPerYear) || null };
  }

  // Unknown / 0 / null frequency — fall back to the visit count if we have it.
  // We describe the service count rather than inventing a cadence label.
  const spy = Number(servicesPerYear) || 0;
  if (spy > 0) {
    return {
      label: spy === 1 ? 'once a year' : `${spy} times a year`,
      chargesPerYear: spy,
      isCount: true,
    };
  }

  // Genuinely undetermined — never guess.
  return { label: null, chargesPerYear: null };
}

/**
 * Single source of truth for default template text.
 * Frontend DEFAULTS in NotificationSettings.tsx must match these values.
 */
/**
 * Compute the dominant billing frequency across a set of services. If all
 * services share a frequency, that's the dominant. Mixed-frequency batches
 * (rare) fall back to the most common; ties broken by the highest
 * chargesPerYear (the most granular cycle, since communicating in a
 * smaller-than-actual cycle is gentler than the reverse).
 *
 * @param {Array<Object>} services
 * @returns {{ label: string, chargesPerYear: number|null }}
 */
function computeDominantBillingFrequency(services) {
  const counts = new Map();
  for (const svc of services || []) {
    const billing = billingFrequencyToLabel(
      svc.billingFrequency ?? svc.billing_frequency,
      svc.servicesPerYear ?? svc.services_per_year,
    );
    // Skip services whose cadence is undetermined — they can't vote.
    if (billing.chargesPerYear == null) continue;
    const key = String(billing.chargesPerYear);
    const prev = counts.get(key) ?? { label: billing.label, chargesPerYear: billing.chargesPerYear, count: 0 };
    prev.count++;
    counts.set(key, prev);
  }
  // No service had a determinable cadence — return the null sentinel.
  if (counts.size === 0) return { label: null, chargesPerYear: null };
  const sorted = [...counts.values()].sort((a, b) => {
    if (b.count !== a.count) return b.count - a.count;
    return (b.chargesPerYear ?? 0) - (a.chargesPerYear ?? 0);
  });
  return { label: sorted[0].label, chargesPerYear: sorted[0].chargesPerYear };
}

const DEFAULT_TEMPLATE = {
  notification_subject: 'Your Service Summary - {company_name}',
  notification_greeting: 'Hi {first_name},',
  notification_body_single: 'Thank you for trusting {company_name} with your pest management needs. We truly value your business.\n\nWe are writing to let you know about an upcoming price change for {account_name}, effective {effective_date}:\n\n{service_list}\n\nThese updated rates reflect the continued investment in quality products, trained technicians, and reliable scheduling that keep your property protected year-round.',
  notification_body_multi: 'Thank you for trusting {company_name} with your pest management needs. We truly value your business.\n\nWe are writing to let you know about upcoming price changes for {account_name}, effective {effective_date}:\n\n{service_list}\n\nThese updated rates reflect the continued investment in quality products, trained technicians, and reliable scheduling that keep your property protected year-round.',
  notification_closing: "We're always here if you have any questions — feel free to reach out to us at any time.\n\nThank you for being a valued part of the {company_name} family.\n\nWarm regards,\nThe {company_name} Team",
  notification_footer: 'You are receiving this because you are a customer of {company_name}.',
  notification_header_color: '#0f172a',
  notification_accent_color: '#10b981',
};

/**
 * Resolve a customer-facing account name. Falls back to "your account" when the
 * name is missing or is a bare master_account_id (digits only) — a raw numeric
 * id should never appear in a customer email.
 */
function resolveAccountName(accountName) {
  const name = String(accountName ?? '').trim();
  if (!name || /^\d+$/.test(name)) return 'your account';
  return name;
}

/**
 * Compute template variables from services + account metadata.
 *
 * This function is TOTAL — it never throws. Every variable resolves to a string
 * or to null (the renderer turns null into an empty string). Bad billing data
 * on one service can make a cadence-dependent aggregate null, but it never
 * aborts the email or blocks unrelated variables like {service_list}.
 *
 * @returns {{ mode: 'single'|'multi', variables: Object }}
 */
export function computeEmailVariables({
  services,
  customerName,
  firstName,
  accountName,
  companyName,
  effectiveDate,
  effectivePeriod = null,
  templateConfig = {},
}) {
  const formatUSD = (val) => `$${Number(val).toFixed(2)}`;

  const parseDate = (value) => {
    if (!value) return null;
    const parsed = new Date(`${String(value).slice(0, 10)}T00:00:00`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  };
  const formatDate = (value) => {
    const parsed = parseDate(value);
    return parsed
      ? parsed.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })
      : '';
  };
  // effective_date is stored as the day BEFORE rollout, so +1 day lands in the
  // rollout month.
  const formatMonth = (value) => {
    const parsed = parseDate(value);
    if (!parsed) return '';
    parsed.setDate(parsed.getDate() + 1);
    return parsed.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
  };
  // effective_period is YYYY-MM — already the rollout month, no +1 needed.
  const monthFromPeriod = (period) => {
    const m = /^(\d{4})-(\d{2})$/.exec(String(period ?? '').trim());
    if (!m) return '';
    const parsed = new Date(Number(m[1]), Number(m[2]) - 1, 1);
    return Number.isNaN(parsed.getTime())
      ? ''
      : parsed.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
  };

  // Only services with a real price increase appear in customer-facing output.
  const nonZeroServices = (services || []).filter(s => (Number(s.increaseAmount) || 0) > 0);

  // ── {service_list} — the primary compound word ──────────────────────────
  // One line per increasing subscription: "Name — $price[ cadence]". Price is
  // the new recurring charge (matches the customer's invoice). The cadence
  // suffix degrades gracefully — omitted entirely when it can't be determined,
  // so the customer always sees a correct price, never a wrong cadence.
  const serviceList = nonZeroServices.map(svc => {
    const name = svc.serviceTypeName || svc.service_type_name || 'your service';
    const price = formatUSD(Number(svc.newPrice ?? svc.new_price) || 0);
    const billing = billingFrequencyToLabel(
      svc.billingFrequency ?? svc.billing_frequency,
      svc.servicesPerYear ?? svc.services_per_year,
    );
    if (!billing.label) return `${name} — ${price}`;
    // isCount labels ("4 times a year") read as a parenthetical, not a rate,
    // so they take a comma; real cadence labels ("every month") attach directly.
    return billing.isCount
      ? `${name} — ${price}, ${billing.label}`
      : `${name} — ${price} ${billing.label}`;
  }).join('\n');

  // Square-footage tier — unique set across increasing subs. Blank when none.
  const uniqueTiers = [...new Set(
    nonZeroServices.map(s => s.pricingTier ?? s.pricing_tier).filter(Boolean)
  )];
  const squareFootageTier = uniqueTiers.map(formatSquareFootageTier).filter(Boolean).join(', ');

  // ── Annual aggregates ───────────────────────────────────────────────────
  // PREFERRED: authoritative annualCurrent/annualNew/annualIncrease from the
  // pipeline. FALLBACK: reconstruct from per-cycle price × charges-per-year.
  // If a service has neither, the annual totals are unreliable and every
  // cadence-derived aggregate resolves to null (omitted) — we never guess.
  let totalAnnualIncrease = 0;
  let totalAnnualCurrent = 0;
  let totalAnnualNew = 0;
  let annualReliable = nonZeroServices.length > 0;
  for (const svc of nonZeroServices) {
    const annualCurrent = Number(svc.annualCurrent ?? svc.annual_current) || 0;
    const annualNew = Number(svc.annualNew ?? svc.annual_new) || 0;
    const annualIncrease = Number(svc.annualIncrease ?? svc.annual_increase) || 0;

    if (annualCurrent > 0 && annualNew > 0) {
      totalAnnualCurrent += annualCurrent;
      totalAnnualNew += annualNew;
      totalAnnualIncrease += annualIncrease > 0 ? annualIncrease : (annualNew - annualCurrent);
      continue;
    }

    // Fallback needs a charges-per-year multiplier we can trust. If billing
    // frequency is undetermined, we refuse to guess — the aggregates go null.
    const billing = billingFrequencyToLabel(
      svc.billingFrequency ?? svc.billing_frequency,
      svc.servicesPerYear ?? svc.services_per_year,
    );
    if (!billing.chargesPerYear || billing.chargesPerYear <= 0) {
      annualReliable = false;
      continue;
    }
    totalAnnualIncrease += (Number(svc.increaseAmount) || 0) * billing.chargesPerYear;
    totalAnnualCurrent += (Number(svc.currentPrice ?? svc.current_price) || 0) * billing.chargesPerYear;
    totalAnnualNew += (Number(svc.newPrice ?? svc.new_price) || 0) * billing.chargesPerYear;
  }

  const serviceCount = nonZeroServices.length;

  // Cadence-blind aggregates (annual ÷ a fixed divisor) — null when unreliable.
  const aggMonthly = (annual) => (annualReliable ? formatUSD(annual / 12) : null);
  const aggQuarterly = (annual) => (annualReliable ? formatUSD(annual / 4) : null);

  // Cadence-aware per-cycle aggregates, using the dominant billing frequency.
  const dominant = computeDominantBillingFrequency(nonZeroServices);
  const perCycleOk = annualReliable && dominant.chargesPerYear != null && dominant.chargesPerYear > 0;
  const perCycle = (annual) => (perCycleOk ? formatUSD(annual / dominant.chargesPerYear) : null);

  // Base variables available in single + multi mode.
  const baseVars = {
    first_name: firstName || (customerName ? String(customerName).split(' ')[0] : '') || 'there',
    customer_name: customerName || '',
    account_name: resolveAccountName(accountName),
    company_name: companyName || templateConfig.notification_from_name || '',
    effective_date: formatDate(effectiveDate),
    effective_month: effectiveDate ? formatMonth(effectiveDate) : monthFromPeriod(effectivePeriod),
    service_list: serviceList,
    square_footage_tier: squareFootageTier,
    service_count: String(serviceCount),
    // Cadence-blind aggregates — kept for power users, null when unreliable.
    monthly_increase: aggMonthly(totalAnnualIncrease),
    quarterly_increase: aggQuarterly(totalAnnualIncrease),
    current_monthly_total: aggMonthly(totalAnnualCurrent),
    current_quarterly_total: aggQuarterly(totalAnnualCurrent),
    new_monthly_total: aggMonthly(totalAnnualNew),
    new_quarterly_total: aggQuarterly(totalAnnualNew),
    average_quarterly_increase: (annualReliable && serviceCount > 0)
      ? formatUSD(totalAnnualIncrease / 4 / serviceCount)
      : null,
    // Cadence-aware aggregates — preferred over the monthly/quarterly pair.
    per_cycle_increase: perCycle(totalAnnualIncrease),
    per_cycle_label: perCycleOk ? dominant.label : null,
    current_per_cycle_total: perCycle(totalAnnualCurrent),
    new_per_cycle_total: perCycle(totalAnnualNew),
    // Client business contact (from settings).
    client_email: templateConfig.notification_client_email || '',
    client_phone: templateConfig.notification_client_phone || '',
    client_address: templateConfig.notification_client_address || '',
  };

  if (nonZeroServices.length === 1) {
    const svc = nonZeroServices[0];
    return {
      mode: 'single',
      variables: {
        ...baseVars,
        increase: formatUSD(Number(svc.increaseAmount) || 0),
        new_price: formatUSD(Number(svc.newPrice ?? svc.new_price) || 0),
      },
    };
  }
  // Multi mode — single-sub-only words ({increase}, {new_price}) are absent, so
  // the renderer omits them instead of leaking literal {tokens}.
  return { mode: 'multi', variables: { ...baseVars } };
}

export async function sendPriceIncreaseEmail({
  recipient,
  recipientName,
  customerName,
  accountName,
  clientName,
  services,
  subscriptions,
  effectiveDate,
  effectivePeriod,
  unsubscribeUrl,
  fromEmail,
  fromName,
  replyTo,
  branding = {},
  templateConfig = null,
  dryRun = false,
}) {
  const senderEmail = fromEmail || `${clientName.toLowerCase()}@${NOTIFICATION_FROM_DOMAIN}`;
  const senderName = fromName || clientName;
  const resolvedCustomerName = customerName || recipientName || '';
  const firstName = resolvedCustomerName ? resolvedCustomerName.split(' ')[0] : '';

  // Build normalized services list
  const normalizedServices = (services && services.length > 0
    ? services
    : (subscriptions || []).map((subscription) => {
        const currentPrice = Number(subscription.oldCharge) || 0;
        const newPrice = Number(subscription.newCharge) || 0;
        return {
          serviceTypeName: subscription.serviceTypeName || 'Service',
          currentPrice,
          newPrice,
          increaseAmount: newPrice - currentPrice,
          increasePct: Number(subscription.increasePct) || (currentPrice > 0 ? ((newPrice - currentPrice) / currentPrice) * 100 : 0),
          billingFrequency: subscription.billingFrequency ?? subscription.billing_frequency ?? null,
          servicesPerYear: subscription.servicesPerYear ?? subscription.services_per_year ?? null,
          recurringPriceCharge: subscription.recurringPriceCharge ?? subscription.recurring_price_charge ?? null,
        };
      })
  );

  // Always delegate to sendTemplatedEmail — it merges with DEFAULT_TEMPLATE
  return sendTemplatedEmail({
    recipient, recipientName: resolvedCustomerName, firstName, accountName,
    clientName, senderEmail, senderName, services: normalizedServices,
    effectiveDate, effectivePeriod, unsubscribeUrl, branding, templateConfig: templateConfig || {},
    replyTo, dryRun,
  });
}

// ============================================
// Templated Email Sender
// ============================================
async function sendTemplatedEmail({
  recipient, recipientName, firstName, accountName,
  clientName, senderEmail, senderName, services,
  effectiveDate, effectivePeriod, unsubscribeUrl, branding, templateConfig,
  replyTo, dryRun = false,
}) {
  // Merge incoming config with defaults — configured values win
  const cfg = { ...DEFAULT_TEMPLATE, ...Object.fromEntries(
    Object.entries(templateConfig).filter(([, v]) => v != null && v !== '')
  )};

  const { mode, variables } = computeEmailVariables({
    services, customerName: recipientName, firstName, accountName,
    companyName: senderName, effectiveDate, effectivePeriod, templateConfig,
  });

  // Resolve all template sections from the merged config
  const subject = resolveTemplateVariables(cfg.notification_subject, variables);
  const greeting = resolveTemplateVariables(cfg.notification_greeting, variables);
  const body = resolveTemplateVariables(
    mode === 'single' ? cfg.notification_body_single : cfg.notification_body_multi,
    variables
  );
  const closing = resolveTemplateVariables(cfg.notification_closing, variables);
  const footer = resolveTemplateVariables(cfg.notification_footer, variables);

  // Branding
  const headerColor = cfg.notification_header_color;
  const accentColor = cfg.notification_accent_color;
  const logoUrl = templateConfig.notification_logo_url || branding.logoUrl || '';
  const initial = senderName.charAt(0).toUpperCase();
  const logoHtml = logoUrl
    ? `<img src="${logoUrl}" alt="${senderName}" style="max-height: 40px; max-width: 180px; display: block;" />`
    : '';

  // Convert body newlines to <br> for HTML, and closing newlines to <br>
  const bodyHtml = body.replace(/\n/g, '<br>');
  const closingHtml = closing.replace(/\n/g, '<br>');
  const footerHtml = footer.replace(/\n/g, '<br>');

  const htmlContent = `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <meta name="color-scheme" content="light">
      <title>${subject}</title>
      <!--[if mso]><noscript><xml><o:OfficeDocumentSettings><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript><![endif]-->
    </head>
    <body style="margin: 0; padding: 0; background-color: #f8fafc; -webkit-font-smoothing: antialiased;">
      <div style="display: none; max-height: 0; overflow: hidden; font-size: 1px; line-height: 1px; color: #f8fafc;">Here is a summary of your updated service pricing with ${senderName}.</div>
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color: #f8fafc;">
        <tr>
          <td align="center" style="padding: 32px 16px;">
            <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width: 600px; width: 100%;">

              <!-- HEADER -->
              <tr>
                <td style="background: ${headerColor}; padding: 32px 32px 28px 32px; border-radius: 12px 12px 0 0;">
                  ${logoUrl ? logoHtml : `
                  <table role="presentation" cellpadding="0" cellspacing="0">
                    <tr>
                      <td style="width: 44px; height: 44px; background: rgba(255,255,255,0.12); border-radius: 10px; text-align: center; vertical-align: middle; font-size: 20px; font-weight: 700; color: #ffffff; font-family: Georgia, 'Times New Roman', serif;">
                        ${initial}
                      </td>
                      <td style="padding-left: 14px;">
                        <div style="font-family: Georgia, 'Times New Roman', serif; font-size: 20px; font-weight: 700; color: #ffffff; letter-spacing: -0.3px;">${senderName}</div>
                      </td>
                    </tr>
                  </table>`}
                </td>
              </tr>

              <!-- BODY -->
              <tr>
                <td style="background: #ffffff; padding: 32px; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.65; color: #334155; border-left: 1px solid #e2e8f0; border-right: 1px solid #e2e8f0;">
                  <p style="margin: 0 0 16px 0; font-size: 15px; color: #334155;">${greeting}</p>
                  <p style="margin: 0 0 24px 0; font-size: 15px; color: #475569;">${bodyHtml}</p>

                  <!-- Sign-off -->
                  <div style="margin-top: 28px; padding-top: 20px; border-top: 1px solid #f1f5f9;">
                    <p style="margin: 0; font-size: 15px; color: #334155;">${closingHtml}</p>
                  </div>
                </td>
              </tr>

              <!-- FOOTER -->
              <tr>
                <td style="background: #f8fafc; padding: 24px 32px; border: 1px solid #e2e8f0; border-top: none; border-radius: 0 0 12px 12px;">
                  <p style="margin: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; font-size: 12px; color: #94a3b8; text-align: center;">
                    ${footerHtml}
                  </p>
                </td>
              </tr>

            </table>
          </td>
        </tr>
      </table>
    </body>
    </html>
  `;

  const textContent = `
${senderName}

${greeting}

${body}

${closing}

---
${footer}
  `.trim();

  // Caller (priceIncreaseNotificationService) needs the rendered payload to
  // persist a notification_send_event in BQ. Bundle everything we produced
  // here so the audit trail can show the operator the exact email that went
  // out.
  const ccListEmails = (raw) => {
    if (!raw) return [];
    return String(raw)
      .split(',')
      .map((e) => e.trim())
      .filter((e) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e));
  };
  const renderedPayload = {
    subject,
    htmlBody: htmlContent,
    textBody: textContent,
    mode,
    templateVariables: variables,
    senderEmail,
    senderName,
    recipient,
    recipientName: recipientName || null,
    cc: ccListEmails(templateConfig.notification_cc),
    bcc: ccListEmails(templateConfig.notification_bcc),
    replyToEmail: templateConfig.notification_client_email?.trim() || replyTo || null,
  };

  // Dry-run: return the rendered payload without sending.
  if (dryRun) {
    return {
      success: true,
      dryRun: true,
      rendered: renderedPayload,
    };
  }

  // Send via MailerSend (or log)
  if (!MAILERSEND_API_KEY || !mailerSend) {
    console.log('[EmailService] Would send templated price increase email to:', recipient);
    console.log('[EmailService] Subject:', subject);
    console.log('[EmailService] Mode:', mode);
    return {
      success: true,
      messageId: 'mock-' + Date.now(),
      mock: true,
      rendered: renderedPayload,
    };
  }

  // Parse CC/BCC
  const parseCcList = (raw) => {
    if (!raw) return [];
    return raw.split(',').map(e => e.trim()).filter(e => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e)).map(e => new Recipient(e));
  };
  const ccList = parseCcList(templateConfig.notification_cc);
  const bccList = parseCcList(templateConfig.notification_bcc);

  const sendWithParams = async (params) => {
    const response = await sendMailerSendRequest(params);
    const messageId = response?.body?.['x-message-id'] || response?.headers?.['x-message-id'] || response?.messageId || 'unknown';
    return messageId;
  };

  try {
    const from = new Sender(senderEmail, senderName);
    const to = [new Recipient(recipient, recipientName || undefined)];

    const emailParams = new EmailParams()
      .setFrom(from)
      .setTo(to)
      .setSubject(subject)
      .setText(textContent)
      .setHtml(htmlContent);

    if (replyTo) emailParams.setReplyTo(new Recipient(replyTo));
    if (ccList.length > 0) emailParams.setCc(ccList);
    if (bccList.length > 0) emailParams.setBcc(bccList);

    const messageId = await sendWithParams(emailParams);
    console.log(`[EmailService] Templated price increase email sent to ${recipient}. MessageId: ${messageId}`);
    return { success: true, messageId, rendered: renderedPayload };
  } catch (error) {
    // CC/BCC resilience: if send fails and we had CC/BCC, retry without them
    if (ccList.length > 0 || bccList.length > 0) {
      console.warn(`[EmailService] Send failed with CC/BCC, retrying without CC/BCC for ${recipient}:`, error?.message);
      try {
        const from = new Sender(senderEmail, senderName);
        const to = [new Recipient(recipient, recipientName || undefined)];
        const retryParams = new EmailParams()
          .setFrom(from)
          .setTo(to)
          .setSubject(subject)
          .setText(textContent)
          .setHtml(htmlContent);

        if (replyTo) retryParams.setReplyTo(new Recipient(replyTo));

        const messageId = await sendWithParams(retryParams);
        console.log(`[EmailService] Retry without CC/BCC succeeded for ${recipient}. MessageId: ${messageId}`);
        return { success: true, messageId, ccBccFailed: true, rendered: renderedPayload };
      } catch (retryError) {
        const body = retryError?.response?.body ?? retryError?.body;
        const msg = body?.message ?? (typeof body === 'string' ? body : JSON.stringify(body ?? retryError?.message ?? retryError));
        console.error(`[EmailService] Retry also failed for ${recipient}:`, msg);
        return { success: false, error: msg, rendered: renderedPayload };
      }
    }

    const body = error?.response?.body ?? error?.body;
    const msg = body?.message ?? (typeof body === 'string' ? body : JSON.stringify(body ?? error?.message ?? error));
    console.error(`[EmailService] Failed to send templated email to ${recipient}:`, msg);
    return { success: false, error: msg, rendered: renderedPayload };
  }
}

/**
 * Validate email addresses
 * 
 * @param {string[]} emails - Array of email addresses
 * @returns {{valid: string[], invalid: string[]}}
 */
export function validateEmails(emails) {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  const valid = [];
  const invalid = [];

  for (const email of emails) {
    const trimmed = email.trim().toLowerCase();
    if (emailRegex.test(trimmed)) {
      valid.push(trimmed);
    } else {
      invalid.push(email);
    }
  }

  return { valid, invalid };
}

export default {
  sendReportEmail,
  sendReportFailureEmail,
  sendPriceIncreaseEmail,
  computeEmailVariables,
  validateEmails,
};

