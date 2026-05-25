import { config } from 'dotenv';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const envPath = join(__dirname, '.env');

if (existsSync(envPath)) {
    config({ path: envPath });
    console.log('[price-increase-notification-job] Loaded .env from project root');
} else {
    console.log('[price-increase-notification-job] No .env file found (using injected environment variables)');
}

// JOB_TASK selects which entry point this container execution runs. One image,
// one Cloud Run Job; the app passes JOB_TASK as a per-execution env override
// alongside the existing NOTIFICATION_* vars.
//   send         → main.js (default; runs the send pipeline)
//   verify       → verify-emails.js (cohort email verification + tagging)
//   sync-status  → sync-email-status.js (MailerSend Activity ETL + tagging)
const JOB_TASKS = {
    send: './main.js',
    verify: './verify-emails.js',
    'sync-status': './sync-email-status.js',
};
const task = String(process.env.JOB_TASK || 'send').toLowerCase();
const entryModule = JOB_TASKS[task];
if (!entryModule) {
    console.error(`[price-increase-notification-job] Unknown JOB_TASK=${task}. Valid: ${Object.keys(JOB_TASKS).join(', ')}`);
    process.exit(1);
}
console.log(`[price-increase-notification-job] Dispatching JOB_TASK=${task} → ${entryModule}`);
await import(entryModule);
