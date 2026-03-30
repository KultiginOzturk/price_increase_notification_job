import fs from 'fs';
import path from 'path';

const DEFAULT_SECRET_FILENAME = 'pco-qa-credentials.json';
const LEGACY_WINDOWS_PATH = 'C\\Users\\Kültigin\\PycharmProjects\\pco_analytics\\pco-qa-9a3d854dcb14.json';

export function resolveCredentialsPath() {
  const candidates = [];

  // Prefer BIGQUERY_CREDENTIALS_FILE for explicit BigQuery credentials
  // This allows Cloud SQL Proxy to use separate (user) credentials
  if (process.env.BIGQUERY_CREDENTIALS_FILE) {
    candidates.push(process.env.BIGQUERY_CREDENTIALS_FILE);
  }

  // Fall back to GOOGLE_APPLICATION_CREDENTIALS for backwards compatibility
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    candidates.push(process.env.GOOGLE_APPLICATION_CREDENTIALS);
  }

  candidates.push(path.join(process.cwd(), 'secrets', DEFAULT_SECRET_FILENAME));
  candidates.push(path.join(process.cwd(), 'BQ Key', 'pco-qa-9a3d854dcb14.json'));
  candidates.push(LEGACY_WINDOWS_PATH);

  const resolvedCandidates = candidates
    .filter(Boolean)
    .map((candidatePath) => path.resolve(candidatePath));

  const existingPath = resolvedCandidates.find((candidatePath) => fs.existsSync(candidatePath));

  return {
    keyFilename: existingPath ?? null,
    candidates: resolvedCandidates,
    defaultFilename: DEFAULT_SECRET_FILENAME,
  };
}


