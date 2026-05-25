import { config } from 'dotenv';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const envPath = join(__dirname, '.env');
if (existsSync(envPath)) config({ path: envPath });

const { sendPriceIncreaseEmail } = await import('./services/emailService.js');
const { fetchNotificationConfig } = await import('./services/priceIncreaseNotificationService.js');
const { closePool } = await import('./lib/postgres.js');

const recipient = process.env.TEST_TO;
const client = process.env.TEST_CLIENT || 'SHERILPEST';
const fromOverride = process.env.TEST_FROM || 'cs@pestnotifications.com';
const fromNameOverride = process.env.TEST_FROM_NAME || null;
if (!recipient) {
    console.error('Set TEST_TO=you@example.com');
    process.exit(1);
}

// Multiple non-zero-increase services → forces emailService into "multi" mode
// (computeEmailVariables: nonZeroServices.length !== 1). This exercises
// notification_body_multi and the multi-service template variables, which
// test-send-one.js (single service) never reaches.
const services = [
    {
        serviceTypeName: 'General Pest Control',
        currentPrice: 45,
        newPrice: 49,
        increaseAmount: 4,
        increasePct: 8.89,
        billingFrequency: 30,
        servicesPerYear: 12,
    },
    {
        serviceTypeName: 'Termite Monitoring',
        currentPrice: 120,
        newPrice: 132,
        increaseAmount: 12,
        increasePct: 10,
        billingFrequency: 90,
        servicesPerYear: 4,
    },
    {
        serviceTypeName: 'Mosquito & Tick Service',
        currentPrice: 60,
        newPrice: 65,
        increaseAmount: 5,
        increasePct: 8.33,
        billingFrequency: 30,
        servicesPerYear: 12,
    },
];

try {
    const senderConfig = await fetchNotificationConfig(client);
    const effectiveFromEmail = fromOverride || senderConfig.fromEmail || undefined;
    const effectiveFromName = fromNameOverride || senderConfig.fromName || undefined;
    console.log('[test-send-multi] senderConfig:', {
        fromEmail: effectiveFromEmail,
        fromName: effectiveFromName,
        originalFromEmail: senderConfig.fromEmail,
        replyTo: senderConfig.replyTo,
    });
    console.log(`[test-send-multi] sending ${services.length}-service sample (multi mode) to ${recipient}`);

    const result = await sendPriceIncreaseEmail({
        recipient,
        recipientName: 'Test Customer',
        customerName: 'Test Customer',
        accountName: 'TEST-ACCOUNT',
        clientName: client,
        effectiveDate: '2026-05-01',
        services,
        unsubscribeUrl: 'https://example.com/unsub?token=test',
        fromEmail: effectiveFromEmail,
        fromName: effectiveFromName,
        replyTo: senderConfig.replyTo,
        templateConfig: senderConfig.templateConfig || {},
    });

    console.log('[test-send-multi] result:', result);
} catch (err) {
    console.error('[test-send-multi] failed:', err?.message || err);
    process.exitCode = 1;
} finally {
    try { await closePool(); } catch {}
}
