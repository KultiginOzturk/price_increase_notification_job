import { config } from 'dotenv';
import { existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const envPath = join(__dirname, '..', '.env');
if (existsSync(envPath)) config({ path: envPath });

const { runQuery } = await import('../utils/bigquery.js');
const { closePool } = await import('../lib/postgres.js');

try {
    const rows = await runQuery(`
        SELECT client, status, COUNT(*) AS cnt
        FROM \`pco-prod.APP_INPUTS.inp_price_increase_notification_events\`
        WHERE LOWER(client) LIKE '%greensboro%'
           OR LOWER(client) LIKE '%greenville%'
           OR LOWER(client) LIKE '%holly%'
        GROUP BY client, status
        ORDER BY client, status
    `, {}, 'office-counts');
    console.table(rows);
} finally {
    try { await closePool(); } catch {}
}
