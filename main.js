import { mkdirSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import readline from 'readline';
import XLSX from 'xlsx';
import { closePool } from './lib/postgres.js';
import { runDuePrePushNotifications } from './services/priceIncreaseNotificationService.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

const EXCEL_COLUMNS = [
    ['accountId', 'Account ID'],
    ['accountName', 'Account Name'],
    ['customerName', 'Customer Name'],
    ['email', 'Email'],
    ['eligibility', 'Eligibility for email'],
    ['effectivePeriod', 'Effective Period'],
    ['effectiveDate', 'Effective Date'],
    ['serviceCount', 'Service Count'],
    ['totalIncrease', 'Total increase $'],
    ['services', 'Services'],
    ['sendStatus', 'Send Status'],
];

function writeSendReport(records, clients) {
    if (!records || records.length === 0) return;
    const rows = records.map((r) => {
        const out = {};
        for (const [key, header] of EXCEL_COLUMNS) out[header] = r[key] ?? '';
        return out;
    });
    const ws = XLSX.utils.json_to_sheet(rows, { header: EXCEL_COLUMNS.map(([, h]) => h) });
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'Send Report');
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const suffix = clients ? clients.join('_') : 'all';
    const outDir = join(__dirname, 'send-reports');
    mkdirSync(outDir, { recursive: true });
    const outPath = join(outDir, `send-report-${suffix}-${stamp}.xlsx`);
    XLSX.writeFile(wb, outPath);
    console.log(`[price-increase-notification-job] Wrote send report: ${outPath}`);
}

function parseClientList(rawValue) {
    if (!rawValue) return null;
    const clients = String(rawValue)
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean);
    return clients.length > 0 ? [...new Set(clients)] : null;
}

function logPeriodSummary(period) {
    const detailParts = [
        `status=${period.status}`,
        `client=${period.client}`,
        `effectivePeriod=${period.effectivePeriod}`,
        `noticeDate=${period.noticeDate || 'n/a'}`,
        `effectiveDate=${period.effectiveDate || 'n/a'}`,
        `eligible=${period.eligible ?? 0}`,
        `noEmail=${period.noEmail ?? 0}`,
        `unsubscribed=${period.unsubscribed ?? 0}`,
        `alreadySent=${period.alreadySent ?? 0}`,
        `excludedTag=${period.excludedTag ?? 0}`,
        `sent=${period.sent ?? 0}`,
        `failed=${period.failed ?? 0}`,
    ];
    console.log(`[price-increase-notification-job] ${detailParts.join(' ')}`);
}

function promptYesNo(message) {
    return new Promise((resolve) => {
        const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
        rl.question(`${message} [y/N] `, (answer) => {
            rl.close();
            resolve(/^y(es)?$/i.test(String(answer).trim()));
        });
    });
}

function formatPreflightSection(title, body) {
    const bar = '─'.repeat(60);
    return `\n${bar}\n${title}\n${bar}\n${body}\n`;
}

async function interactivePreflight({ period, senderConfig, counts, excluded, sample, sanity }) {
    const periodHeader = `Client: ${period.client}  |  Effective period: ${period.effectivePeriod}  |  Effective date: ${period.effectiveDate || 'n/a'}`;
    console.log(`\n\n════════════════════════════════════════════════════════════`);
    console.log(`PREFLIGHT — ${periodHeader}`);
    console.log(`════════════════════════════════════════════════════════════`);

    const hasExcludedSection = excluded && excluded.keys?.length > 0;
    const total = hasExcludedSection ? 5 : 4;
    let step = 1;
    const label = (title) => `${step++}/${total}  ${title}`;

    // Sender identity
    console.log(formatPreflightSection(label('SENDER IDENTITY'), [
        `From email:  ${senderConfig.fromEmail || '(fallback to env default)'}`,
        `From name:   ${senderConfig.fromName || '(fallback to client name)'}`,
        `Reply-to:    ${senderConfig.replyTo || '(none)'}`,
    ].join('\n')));
    if (!(await promptYesNo('Sender identity looks correct — proceed?'))) return false;

    // Counts
    console.log(formatPreflightSection(label('RECIPIENT COUNTS'), [
        `Will send to:      ${counts.toSend}`,
        `Eligible total:    ${counts.eligible}`,
        `No email on file:  ${counts.noEmail}`,
        `Unsubscribed:      ${counts.unsubscribed}`,
        `Already sent:      ${counts.alreadySent}`,
        `Excluded by tag:   ${counts.excludedTag ?? 0}`,
    ].join('\n')));
    if (!(await promptYesNo('Counts look correct — proceed?'))) return false;

    // Excluded-tag validation (whenever tags are configured, even if 0 match this cohort)
    if (hasExcludedSection) {
        const breakdown = excluded.breakdown || {};
        const currentPeriod = excluded.currentPeriod || '(this cohort)';
        const isPeriod = (loc) => /^\d{4}-\d{2}$/.test(loc);
        const formatLocation = (loc, n) => (isPeriod(loc) ? `${n} in ${loc}` : `${n} ${String(loc).replace(/_/g, ' ')}`);

        const tagLines = [];
        for (const tag of excluded.keys) {
            const inCohort = excluded.countsByTag[tag] ?? 0;
            const tagBreakdown = breakdown[tag] || { total: 0, byLocation: {} };
            const total = tagBreakdown.total;
            const elsewhere = Object.entries(tagBreakdown.byLocation)
                .filter(([loc]) => loc !== currentPeriod)
                .sort(([a], [b]) => {
                    const ap = isPeriod(a); const bp = isPeriod(b);
                    if (ap && bp) return a.localeCompare(b);
                    if (ap) return -1;
                    if (bp) return 1;
                    return a.localeCompare(b);
                });

            const head = `  ${tag.padEnd(30)} ${String(inCohort).padStart(4)} in ${currentPeriod} / ${String(total).padStart(4)} total`;

            if (total === 0) {
                tagLines.push(head + '  ← no accounts carry this tag yet');
                continue;
            }
            if (elsewhere.length === 0) {
                tagLines.push(head); // all tagged accounts are in this cohort — nothing more to say
                continue;
            }
            tagLines.push(head);
            const prefix = inCohort === 0 ? 'none in this cohort; elsewhere' : 'elsewhere';
            const text = elsewhere.map(([loc, n]) => formatLocation(loc, n)).join(', ');
            tagLines.push(`     ← ${prefix}: ${text}`);
        }

        const sampleLines = excluded.samples.length === 0
            ? '  (none in this cohort)'
            : excluded.samples
                .map((s) => `  ${String(s.accountId).padEnd(14)} ${(s.accountName || '').slice(0, 40).padEnd(42)} [${s.matchedTags.join(', ')}]`)
                .join('\n');
        const body = [
            `Configured tag keys:    ${excluded.keys.join(', ')}`,
            `Total excluded from this send:  ${counts.excludedTag}`,
            '',
            `By tag (in this cohort / total tagged for this client on the current plan):`,
            tagLines.join('\n'),
            '',
            `Sample excluded accounts (first ${excluded.samples.length} of ${counts.excludedTag}):`,
            sampleLines,
        ].join('\n');
        console.log(formatPreflightSection(label('EXCLUDED-TAG VALIDATION'), body));
        if (!(await promptYesNo('Excluded accounts look correct — proceed?'))) return false;
    }

    // Sample rendered email
    const r = sample?.rendered;
    const sampleBody = r
        ? [
              `Recipient:  ${r.recipient}${r.recipientName ? ` (${r.recipientName})` : ''}`,
              `From:       ${r.senderName} <${r.senderEmail}>`,
              `Reply-to:   ${r.replyTo || '(none)'}`,
              `Subject:    ${r.subject}`,
              '',
              '--- TEXT BODY ---',
              r.textContent,
              '--- END BODY ---',
          ].join('\n')
        : '(no sample rendered — nothing to show)';
    console.log(formatPreflightSection(label(`SAMPLE EMAIL (acct ${sample?.target?.masterAccountId ?? 'n/a'})`), sampleBody));
    if (!(await promptYesNo('Sample email looks correct — proceed?'))) return false;

    // Sanity checks
    const sanityBody = sanity.warnings.length === 0
        ? 'All automated checks passed.'
        : ['Warnings:', ...sanity.warnings.map((w) => `  - ${w}`)].join('\n');
    console.log(formatPreflightSection(label('AUTOMATED SANITY CHECKS'), sanityBody));
    if (!(await promptYesNo('Sanity checks acceptable — proceed with send?'))) return false;

    return true;
}

async function main() {
    const targetDate = process.env.NOTIFICATION_TARGET_DATE || null;
    const clients = parseClientList(process.env.NOTIFICATION_CLIENTS || process.env.CLIENT || null);
    const baseUrl = process.env.APP_URL || '';
    const sentBy = process.env.NOTIFICATION_SENT_BY || 'cloud_run_job';
    const testRecipient = process.env.NOTIFICATION_TEST_RECIPIENT || null;
    const sendLimit = process.env.NOTIFICATION_LIMIT ? parseInt(process.env.NOTIFICATION_LIMIT, 10) : null;
    const autoConfirm = /^(1|true|yes)$/i.test(String(process.env.NOTIFICATION_AUTO_CONFIRM || ''));

    console.log(
        `[price-increase-notification-job] Starting targetDate=${targetDate || 'today'} ` +
        `clients=${clients ? clients.join(',') : 'all'} testRecipient=${testRecipient || 'none'} ` +
        `sendLimit=${sendLimit ?? 'none'} autoConfirm=${autoConfirm}`
    );

    const summary = await runDuePrePushNotifications({
        targetDate,
        clients,
        baseUrl,
        sentBy,
        testRecipient,
        sendLimit,
        preflight: autoConfirm ? null : interactivePreflight,
    });

    for (const period of summary.periods) {
        logPeriodSummary(period);
    }

    writeSendReport(summary.records, clients);

    console.log(
        `[price-increase-notification-job] Complete targetDate=${summary.targetDate} ` +
        `duePeriods=${summary.duePeriodCount} processedPeriods=${summary.processedPeriodCount} ` +
        `eligible=${summary.eligible} sent=${summary.sent} failed=${summary.failed}`
    );

    if (summary.failed > 0) {
        throw new Error(`Price increase notification job completed with ${summary.failed} failed send(s)`);
    }
}

try {
    await main();
} catch (error) {
    console.error('[price-increase-notification-job] Job failed:', error?.message || error);
    process.exitCode = 1;
} finally {
    try {
        await closePool();
    } catch (closeError) {
        console.error('[price-increase-notification-job] Failed to close PostgreSQL pool:', closeError?.message || closeError);
        process.exitCode = process.exitCode || 1;
    }
}
