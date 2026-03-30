/**
 * Cloud SQL PostgreSQL Connection Module
 * 
 * Provides connection pooling for the warm layer database.
 * Instance is determined by BQ_PROJECT environment variable:
 *   - pco-qa   → pco-qa:us-central1:client-ops-warm-layer
 *   - pco-prod → pco-prod:us-central1:client-ops-warm-layer
 *   - pco-dev3 → pco-dev3:us-central1:client-ops-warm-layer
 * 
 * Usage:
 *   import { query } from './postgres.js';
 *   const result = await query('SELECT * FROM account_summaries WHERE client = $1', ['BHB']);
 * 
 * Database: client_ops
 * Tables: account_summaries, inp_margin_adjustments, inp_account_tags, cfg_tag_definitions, etc.
 */

import pg from 'pg';
const { Pool } = pg;

// Return DATE columns as plain 'YYYY-MM-DD' strings instead of JavaScript Date objects.
// Without this, pg converts DATE → JS Date → JSON serializes as "2026-03-17T04:00:00.000Z",
// which pollutes a timezone-agnostic calendar date with the server's UTC offset.
pg.types.setTypeParser(1082, (val) => val);

// Determine if we're connecting via Unix socket (Cloud Run) or TCP (local dev)
const isCloudRun = process.env.CLOUDSQL_HOST?.startsWith('/cloudsql/');

const poolConfig = {
    database: process.env.CLOUDSQL_DATABASE || 'client_ops',
    user: process.env.CLOUDSQL_USER || 'postgres',
    password: process.env.CLOUDSQL_PASSWORD,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    allowExitOnIdle: false,  // Prevent Node from exiting while pool exists
};

// Cloud Run uses Unix socket, local dev uses TCP
if (isCloudRun) {
    poolConfig.host = process.env.CLOUDSQL_HOST; // '/cloudsql/pco-qa:us-central1:client-ops-warm-layer'
} else {
    poolConfig.host = process.env.CLOUDSQL_HOST || '127.0.0.1';
    poolConfig.port = parseInt(process.env.CLOUDSQL_PORT || '5432', 10);
}

const pool = new Pool(poolConfig);

// Log connection events
pool.on('connect', () => {
    if (process.env.NODE_ENV !== 'production') {
        console.log('[postgres] Connected to Cloud SQL');
    }
});

pool.on('error', (err) => {
    console.error('[postgres] Unexpected error on Cloud SQL connection:', err.message);
});

/**
 * Execute a query against the Cloud SQL database
 * @param text - SQL query with $1, $2... placeholders  
 * @param params - Array of parameter values
 * @returns Query result with rows array
 */
export async function query(text, params = []) {
    const start = Date.now();
    try {
        const result = await pool.query(text, params);
        const duration = Date.now() - start;

        if (process.env.NODE_ENV !== 'production') {
            console.log('[postgres] Query executed', {
                query: text.substring(0, 60).replace(/\s+/g, ' '),
                duration: `${duration}ms`,
                rows: result.rowCount,
            });
        }

        return result;
    } catch (error) {
        console.error('[postgres] Query error:', {
            query: text.substring(0, 100),
            error: error.message,
        });
        throw error;
    }
}

/**
 * Get a client from the pool for transactions
 * Remember to call client.release() when done!
 */
export async function getClient() {
    return pool.connect();
}

/**
 * Execute a transaction with automatic commit/rollback.
 * Sets server-side timeouts to prevent zombie transactions if the client dies:
 *   - statement_timeout: max time for any single query (default 30s)
 *   - idle_in_transaction_session_timeout: max idle time between queries (60s)
 * @param callback - Async function that receives the client
 * @param options - { statementTimeoutMs: number } override default timeout
 */
export async function withTransaction(callback, options = {}) {
    const statementTimeout = options.statementTimeoutMs || 30000;
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        await client.query(`SET LOCAL statement_timeout = '${statementTimeout}'`);
        await client.query(`SET LOCAL idle_in_transaction_session_timeout = '60000'`);
        const result = await callback(client);
        await client.query('COMMIT');
        return result;
    } catch (error) {
        try {
            await client.query('ROLLBACK');
        } catch (rollbackError) {
            console.error('[postgres] Rollback failed:', rollbackError.message);
        }
        throw error;
    } finally {
        client.release();
    }
}

/**
 * Close all connections (for graceful shutdown)
 */
export async function closePool() {
    await pool.end();
}

/**
 * Verify Cloud SQL connection at startup
 * Fails loudly if Cloud SQL is unreachable
 */
export async function verifyConnection() {
    try {
        const result = await pool.query('SELECT 1 as connected');
        console.log('[postgres] ✅ Cloud SQL connection verified');
        return true;
    } catch (error) {
        console.error('\n' + '='.repeat(60));
        console.error('❌ CLOUD SQL CONNECTION FAILED');
        console.error('='.repeat(60));
        console.error(`Error: ${error.message}`);
        console.error('');
        console.error('The Cloud SQL Auth Proxy is probably not running.');
        console.error('');
        console.error('Fix: Start the proxy in a separate terminal:');
        console.error('  ./bin/cloud-sql-proxy pco-qa:us-central1:client-ops-warm-layer --port=5432');
        console.error('');
        console.error('Or use npm run dev:all which starts it automatically.');
        console.error('='.repeat(60) + '\n');
        throw error;
    }
}

export default pool;
