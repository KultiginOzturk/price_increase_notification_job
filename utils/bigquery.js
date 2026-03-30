import { BigQuery } from '@google-cloud/bigquery';
import { resolveCredentialsPath } from '../credentials.js';
import { execSync } from 'child_process';

// Check if Application Default Credentials are available
function hasADC() {
  try {
    execSync('gcloud auth application-default print-access-token 2>/dev/null', { encoding: 'utf-8', stdio: 'pipe' });
    return true;
  } catch {
    return false;
  }
}

function isCloudRunRuntime() {
  return Boolean(process.env.K_SERVICE || process.env.CLOUD_RUN_JOB || process.env.CLOUD_RUN_EXECUTION);
}

// Initialize BigQuery client
const { keyFilename, candidates: credentialCandidates, defaultFilename } = resolveCredentialsPath();

// USE_ADC=true forces using user credentials (from gcloud auth application-default login)
// This is preferred for local dev as it works across all projects without managing keys
const preferADC = process.env.USE_ADC === 'true';
const useADC = preferADC || isCloudRunRuntime() || (!keyFilename && hasADC());

// Store credentials status for error handling
export const hasCredentials = !!keyFilename || useADC;

if (useADC) {
  console.log('[BigQuery] Using Application Default Credentials (ADC)');
  if (preferADC && keyFilename) {
    console.log(`[BigQuery] (Ignoring keyfile ${keyFilename} because USE_ADC=true)`);
  }
} else if (keyFilename) {
  console.log(`BigQuery credentials loaded from ${keyFilename}`);
} else {
  console.warn('[BigQuery] No credentials file found.');
  console.warn('           Searched paths:');
  credentialCandidates.forEach((candidate) => console.warn(`             - ${candidate}`));
  console.warn(`           Set BIGQUERY_CREDENTIALS_FILE in .env file or place a key at ./secrets/${defaultFilename}.`);
  console.warn('           Alternatively, run: gcloud auth application-default login');
}

// Use ADC (no keyFilename) when preferred, otherwise use keyfile if available
// BQ_PROJECT is required - no fallback to prevent cross-project accidents
const projectId = process.env.BQ_PROJECT;
if (!projectId) {
  throw new Error('BQ_PROJECT environment variable is required. Set it in .env (e.g., BQ_PROJECT=pco-prod)');
}
export const bigqueryClient = new BigQuery({
  projectId,
  ...(!useADC && keyFilename ? { keyFilename } : {}),
});

/**
 * Normalize BigQuery row values (convert Big/Int/Timestamp to primitives)
 * @param {Object} row - Raw BigQuery row object
 * @returns {Object} Normalized row with primitive values
 */
export function normalizeRow(row) {
  const out = {};
  for (const [key, value] of Object.entries(row)) {
    let v = value;
    if (v !== null && typeof v === 'object') {
      // BigQuery Timestamp wrapper
      if (Object.prototype.hasOwnProperty.call(v, 'value') && typeof v.value === 'string') {
        v = v.value;
      } else if (v.constructor && v.constructor.name === 'Big') {
        // Numeric (Big.js)
        try { v = Number(v.toString()); } catch { v = Number(String(v)); }
      } else if (v.constructor && v.constructor.name === 'BigQueryInt') {
        v = Number(v.value);
      }
    }
    // ⚠️ REMOVED Feb 2026: Automatic numeric string conversion
    // This caused type mismatches when IDs like '44840' were converted to numbers,
    // breaking Map.get() lookups where keys were strings from other sources.
    // If you need numeric values, explicitly convert them in your code.
    // See: ClustingUpdate_2026_02_01_v2.md for details.
    out[key] = v;
  }
  return out;
}

/**
 * Run a BigQuery query with parameter substitution
 * @param {string} query - SQL query with @param placeholders
 * @param {Object} params - Parameters to substitute in the query
 * @param {string} queryName - Optional name/identifier for the query (for logging). Prefix with '_' to suppress logging.
 * @returns {Promise<Array>} Normalized query results
 */
export async function runQuery(query, params = {}, queryName = 'unnamed') {
  // Query names starting with '_' are silent (no logging) - useful for polling queries
  const silent = queryName.startsWith('_');
  // Validate inputs
  if (!query || typeof query !== 'string') {
    throw new Error('Query must be a non-empty string');
  }

  // Check for credentials before attempting query
  if (!hasCredentials) {
    const envPath = process.env.GOOGLE_APPLICATION_CREDENTIALS
      ? `(from .env: ${process.env.GOOGLE_APPLICATION_CREDENTIALS})`
      : '(not set in .env)';
    const errorMessage = `BigQuery credentials not found. Please set up authentication by either:

1. Adding GOOGLE_APPLICATION_CREDENTIALS to your .env file:
   GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account-key.json

2. Or placing your credentials file at: ./secrets/${defaultFilename}

Current GOOGLE_APPLICATION_CREDENTIALS: ${envPath}

Searched paths:
${credentialCandidates.map(c => `  - ${c}`).join('\n')}

For more information, visit: https://cloud.google.com/docs/authentication/getting-started`;
    throw new Error(errorMessage);
  }

  try {
    // Replace parameters in query string
    // Use word boundary to ensure we only match complete parameter names
    let processedQuery = query;
    for (const [key, value] of Object.entries(params)) {
      // Escape special regex characters in the parameter name
      const escapedKey = key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      // Match @paramName with word boundary to avoid partial matches
      const paramPlaceholder = new RegExp(`@${escapedKey}(?![a-zA-Z0-9_])`, 'g');
      let replacementValue;

      if (typeof value === 'string') {
        // Escape single quotes in string values
        const escapedValue = value.replace(/'/g, "''");
        replacementValue = `'${escapedValue}'`;
      } else if (typeof value === 'number') {
        replacementValue = value.toString();
      } else if (value === null || value === undefined) {
        replacementValue = 'NULL';
      } else if (Array.isArray(value)) {
        // Handle arrays - convert to BigQuery array literal format
        const arrayElements = value.map(item => {
          if (typeof item === 'string') {
            return `'${item.replace(/'/g, "''")}'`;
          } else if (typeof item === 'number') {
            return item.toString();
          } else if (item === null || item === undefined) {
            return 'NULL';
          } else {
            return `'${String(item).replace(/'/g, "''")}'`;
          }
        });
        replacementValue = `[${arrayElements.join(', ')}]`;
      } else if (typeof value === 'boolean') {
        // Handle booleans - convert to SQL boolean literals
        replacementValue = value ? 'TRUE' : 'FALSE';
      } else {
        replacementValue = value.toString();
      }

      processedQuery = processedQuery.replace(paramPlaceholder, replacementValue);
    }

    // Log the query name for debugging (skip if silent/polling query)
    if (!silent) {
      console.log(`[BigQuery] Executing query: ${queryName}`);
    }

    const options = {
      query: processedQuery,
      // location: 'us-central1', // Let BigQuery auto-detect location based on dataset
    };

    const [job] = await bigqueryClient.createQueryJob(options);
    const [rows] = await job.getQueryResults();
    return rows.map(normalizeRow);
  } catch (error) {
    console.error(`[BigQuery] Error in query "${queryName}":`, error.message);
    console.error('Query parameters:', params);

    // Note: Query details are not logged to keep console output clean
    // If you need to debug, check the query in the source code for the queryName

    // Provide better error message for authentication errors
    if (error.message && error.message.includes('Could not load the default credentials')) {
      const envPath = process.env.GOOGLE_APPLICATION_CREDENTIALS
        ? `(from .env: ${process.env.GOOGLE_APPLICATION_CREDENTIALS})`
        : '(not set in .env)';
      const authError = new Error(`BigQuery authentication failed. Please set up credentials by either:

1. Adding GOOGLE_APPLICATION_CREDENTIALS to your .env file:
   GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/service-account-key.json

2. Or placing your credentials file at: ./secrets/${defaultFilename}

Current GOOGLE_APPLICATION_CREDENTIALS: ${envPath}

Searched paths:
${credentialCandidates.map(c => `  - ${c}`).join('\n')}

For more information, visit: https://cloud.google.com/docs/authentication/getting-started`);
      authError.cause = error;
      throw authError;
    }

    throw error;
  }
}
