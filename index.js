import { config } from 'dotenv';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const envPath = join(__dirname, '..', '..', '..', '.env');

if (existsSync(envPath)) {
    config({ path: envPath });
    console.log('[price-increase-notification-job] Loaded .env from project root');
} else {
    console.log('[price-increase-notification-job] No .env file found (using injected environment variables)');
}

await import('./main.js');
