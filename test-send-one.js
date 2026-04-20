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

try {
    const senderConfig = await fetchNotificationConfig(client);
    const effectiveFromEmail = fromOverride || senderConfig.fromEmail || undefined;
    const effectiveFromName = fromNameOverride || senderConfig.fromName || undefined;
    console.log('[test-send-one] senderConfig:', {
        fromEmail: effectiveFromEmail,
        fromName: effectiveFromName,
        originalFromEmail: senderConfig.fromEmail,
        replyTo: senderConfig.replyTo,
    });

    const result = await sendPriceIncreaseEmail({
        recipient,
        recipientName: 'Test Customer',
        customerName: 'Test Customer',
        accountName: 'TEST-ACCOUNT',
        clientName: client,
        effectiveDate: '2026-05-01',
        services: [{
            serviceTypeName: 'Pest Control- PPC',
            currentPrice: 29,
            newPrice: 30.75,
            increaseAmount: 1.75,
            increasePct: 6.03,
            billingFrequency: 30,
            servicesPerYear: 3,
        }],
        unsubscribeUrl: 'https://example.com/unsub?token=test',
        fromEmail: effectiveFromEmail,
        fromName: effectiveFromName,
        replyTo: senderConfig.replyTo,
        templateConfig: senderConfig.templateConfig || {},
    });

    console.log('[test-send-one] result:', result);
} catch (err) {
    console.error('[test-send-one] failed:', err?.message || err);
    process.exitCode = 1;
} finally {
    try { await closePool(); } catch {}
}
