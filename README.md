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

- `index.js` loads `.env` from this repo root, then starts the job
- `main.js` runs the job
- `lib/`, `services/`, `config/`, `utils/`, `routes/` contain the minimal runtime copied from the main app

## Required environment

- `BQ_PROJECT`
- `APP_URL`
- `MAILERSEND_API_KEY`
- `CLOUDSQL_HOST`
- `CLOUDSQL_DATABASE`
- `CLOUDSQL_USER`
- `CLOUDSQL_PASSWORD`

Optional environment:

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
