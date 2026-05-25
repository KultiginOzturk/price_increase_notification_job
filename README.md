# Price Increase Notification Job

Standalone Cloud Run Job that sends pre-push customer price increase emails. Runs on demand — invoked by the client-ops-pilot app via the Cloud Run Jobs API, not on a schedule.

## What it does

On each invocation, the job:

1. Finds the latest published PlanV2 plan for each client (or just the clients passed in `NOTIFICATION_CLIENTS`).
2. Finds rollout periods whose derived `noticeDate` is due on the target date.
3. Builds pre-push notification recipients from the published pricing workflow.
4. Sends emails only to eligible targets.
5. Skips already-sent accounts using the existing notification event tables.

Due rule:

- `noticeDate <= targetDate <= effectiveDate`
- `targetDate` defaults to the current UTC date; pass `NOTIFICATION_TARGET_DATE` to override

## Repo layout

- `index.js` loads `.env` from this repo root, then dispatches to one of `main.js` / `verify-emails.js` / `sync-email-status.js` based on the `JOB_TASK` env var (default `send`)
- `main.js` runs the send pipeline (the default `JOB_TASK=send`)
- `verify-emails.js` verifies a cohort's recipient emails via MailerSend, then tags undeliverable accounts
- `sync-email-status.js` pulls hard bounces / spam complaints from the MailerSend Activity API, then tags affected accounts
- `lib/`, `services/`, `config/`, `utils/`, `routes/` contain the minimal runtime copied from the main app

## Email deliverability jobs

Two extra entry points feed the channel-routing system. Both are triggered on
demand (same Cloud Run Jobs `:run` pattern as the send job) and write to
`email_health` (address-level deliverability store, auto-created in Cloud SQL)
and to `inp_account_tags` (reason-coded tags).

- **`verify-emails.js`** — resolves the same recipient cohort the send job
  builds, verifies each email through MailerSend (skipping addresses with a
  cached verdict still inside `EMAIL_VERIFICATION_TTL_DAYS`), then reconciles
  deliverability tags: `no_email`, `email_invalid`, `email_hard_bounced`,
  `email_spam_complaint`.
- **`sync-email-status.js`** — pulls `hard_bounced` / `spam_complaints` activity
  into `email_health`, then (if a cohort is given) reconciles the same tags.
  Re-run safe; bounces trickle in for days after a send, so re-run accordingly.

Tag keys are registered via `migrations/002_deliverability_tag_definitions.sql`
(apply manually — `cfg_tag_definitions` is app-owned config).

## Required environment

- `BQ_PROJECT`
- `APP_URL`
- `MAILERSEND_API_KEY`
- `CLOUDSQL_HOST`
- `CLOUDSQL_DATABASE`
- `CLOUDSQL_USER`
- `CLOUDSQL_PASSWORD`

Optional environment:

- `JOB_TASK`
  - Selects which entry point this container execution runs:
    - `send` (default) — `main.js`, the send pipeline
    - `verify` — `verify-emails.js`, cohort email verification + tagging
    - `sync-status` — `sync-email-status.js`, MailerSend Activity ETL + tagging
  - Pass it as a per-execution `containerOverrides.env` entry when invoking the Cloud Run Job, same shape as the existing `NOTIFICATION_*` overrides.
- `NOTIFICATION_CLIENTS`
  - Comma-separated client list. If omitted, the job scans all clients with a published PlanV2 plan.
- `NOTIFICATION_TARGET_DATE`
  - Override job date in `YYYY-MM-DD` format for backfills/testing.
- `NOTIFICATION_SENT_BY`
  - Audit label written to notification events. Default: `cloud_run_job`
- `NOTIFICATION_TEST_RECIPIENT`
  - Sends all due emails to one inbox and records them as `test_pre_push`, so real sends are not blocked later.
- `NOTIFICATION_LIMIT`
  - Cap the number of sends in a single invocation.
- `NOTIFICATION_AUTO_CONFIRM`
  - Set `true` to skip the interactive preflight. The deployed Cloud Run Job sets this. Local runs leave it unset to walk through the prompts.
- `BIGQUERY_CREDENTIALS_FILE`
  - Optional for local runs only if you are not using ADC.
- `USE_ADC`
  - Optional. Set `true` locally to use `gcloud auth application-default login`.

Optional environment for `verify-emails.js` / `sync-email-status.js`:

- `EMAIL_VERIFICATION_TTL_DAYS`
  - How long a MailerSend verification verdict is trusted before re-verifying. Default `90`.
- `MAILERSEND_VERIFY_MAX_PER_MINUTE`
  - In-process rate limit for the verification endpoint. Default `60`.
- `EMAIL_STATUS_LOOKBACK_DAYS`
  - Default lookback window for the Activity ETL. Default `30`.
- `NOTIFICATION_STATUS_SINCE_DAYS`
  - Per-run override of the Activity ETL lookback window.
- `MAILERSEND_SYNC_DOMAINS`
  - Comma-separated MailerSend domain ids/names to limit the Activity ETL to. Default: all domains.
- `VERIFY_EMAILS`
  - Ad-hoc: comma-separated addresses to verify directly, bypassing cohort resolution and tagging.
- `EMAIL_VERIFY_FORCE`
  - Set `true` to re-verify addresses even when a fresh cached verdict exists.

## Local run

Install dependencies:

```bash
npm install
```

Run:

```bash
node index.js
```

Example backfill for one client:

```bash
NOTIFICATION_CLIENTS=MODERN NOTIFICATION_TARGET_DATE=2026-12-01 node index.js
```

## Build image

```bash
docker build \
  -t us-central1-docker.pkg.dev/PROJECT_ID/client-ops-pilot/price-increase-notification:latest \
  .
```

## Push image

```bash
docker push us-central1-docker.pkg.dev/PROJECT_ID/client-ops-pilot/price-increase-notification:latest
```

## Deploy Cloud Run Job

Use `scripts/deploy_to_gcp.ps1`, or run the equivalent command:

```bash
gcloud run jobs deploy price-increase-notification \
  --project PROJECT_ID \
  --region us-central1 \
  --image us-central1-docker.pkg.dev/PROJECT_ID/client-ops-pilot/price-increase-notification:latest \
  --service-account SERVICE_ACCOUNT_EMAIL \
  --set-env-vars BQ_PROJECT=PROJECT_ID,APP_URL=APP_URL_VALUE,NOTIFICATION_SENT_BY=cloud_run_job,NOTIFICATION_AUTO_CONFIRM=true \
  --set-env-vars CLOUDSQL_HOST=/cloudsql/PROJECT_ID:us-central1:CLOUDSQL_INSTANCE,CLOUDSQL_DATABASE=client_ops,CLOUDSQL_USER=postgres \
  --set-secrets CLOUDSQL_PASSWORD=CLOUDSQL_PASSWORD:latest,MAILERSEND_API_KEY=MAILERSEND_API_KEY:latest \
  --set-cloudsql-instances PROJECT_ID:us-central1:CLOUDSQL_INSTANCE \
  --task-timeout 15m \
  --max-retries 1
```

`NOTIFICATION_AUTO_CONFIRM=true` is required on the deployed Job — without it the interactive preflight in [main.js](main.js) will hang on `readline` because Cloud Run has no stdin.

## Manually execute (smoke test)

```bash
gcloud run jobs execute price-increase-notification \
  --project PROJECT_ID \
  --region us-central1
```

## Invoke from another service

The client-ops-pilot app triggers a run by POSTing to the Cloud Run Jobs `:run` endpoint. Per-execution inputs (target date, client list, test recipient, send limit) are passed as env-var overrides — they don't change the deployed Job spec.

Endpoint:

```
POST https://run.googleapis.com/v2/projects/PROJECT_ID/locations/us-central1/jobs/price-increase-notification:run
```

Auth: the calling service account needs `roles/run.invoker` on the Job. With Google Auth libraries this is usually a `GoogleAuth` client requesting an access token for `https://www.googleapis.com/auth/cloud-platform`.

Body — overrides apply only to this one execution:

```json
{
  "overrides": {
    "containerOverrides": [
      {
        "env": [
          { "name": "NOTIFICATION_CLIENTS",       "value": "MODERN" },
          { "name": "NOTIFICATION_TARGET_DATE",   "value": "2026-12-01" },
          { "name": "NOTIFICATION_SENT_BY",       "value": "client_ops_pilot:user@example.com" },
          { "name": "NOTIFICATION_TEST_RECIPIENT","value": "qa@example.com" }
        ]
      }
    ]
  }
}
```

Response shape (abbreviated):

```json
{
  "name": "projects/PROJECT_NUMBER/locations/us-central1/jobs/price-increase-notification/executions/price-increase-notification-abcde",
  "metadata": { "@type": "type.googleapis.com/google.cloud.run.v2.Execution", "...": "..." }
}
```

The last segment of `name` (e.g. `price-increase-notification-abcde`) is the **execution name** — keep it; that's how you scope the logs for this particular run.

### Tracking progress via logs

The job emits structured progress lines on stdout, all prefixed with `[price-increase-notification-job]`:

- start: `Starting targetDate=… clients=… testRecipient=… sendLimit=… autoConfirm=…`
- per period: `status=… client=… effectivePeriod=… eligible=… sent=… failed=… …`
- end: `Complete targetDate=… duePeriods=… processedPeriods=… eligible=… sent=… failed=…`

Filter Cloud Logging for one execution:

```
resource.type="cloud_run_job"
resource.labels.job_name="price-increase-notification"
labels."run.googleapis.com/execution_name"="EXECUTION_NAME_FROM_RESPONSE"
```

Or via gcloud:

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND labels."run.googleapis.com/execution_name"="EXECUTION_NAME_FROM_RESPONSE"' \
  --project PROJECT_ID \
  --order=asc \
  --format='value(textPayload)'
```

The execution's overall completion + exit code are also visible via the Executions API (`projects/.../executions/EXECUTION_NAME`) — `status.completionTime`, `status.failedCount`, `status.succeededCount`. The job exits non-zero if any send failed, so a failed execution surfaces there too.

## Required IAM

Runtime service account (the one the Job runs as):

- `roles/cloudsql.client`
- `roles/bigquery.jobUser`
- `roles/bigquery.dataViewer`
- `roles/secretmanager.secretAccessor`

Caller service account (the one client-ops-pilot uses to invoke the Job):

- `roles/run.invoker` on the Job
- `roles/logging.viewer` (or `roles/logging.privateLogViewer`) if it also reads the execution logs back

## Failure behavior

- exits `0` when nothing is due
- exits non-zero if any email send fails

## Helper scripts

- `scripts/deploy_to_gcp.ps1`
  - builds the image, pushes it, and deploys the Cloud Run Job
