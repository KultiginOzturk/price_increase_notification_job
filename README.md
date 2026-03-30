# Price Increase Notification Job

Standalone Cloud Run Job for daily pre-push customer price increase emails.

## What it does

On each run, the job:

1. Finds the latest published PlanV2 plan for each client.
2. Finds rollout periods whose derived `noticeDate` is due on the job date.
3. Builds pre-push notification recipients from the published pricing workflow.
4. Sends emails only to eligible targets.
5. Skips already-sent accounts using the existing notification event tables.

Due rule:

- `noticeDate <= targetDate <= effectiveDate`
- `targetDate` defaults to the current UTC date

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

Replace the placeholders before running:

- `PROJECT_ID`
- `CLOUDSQL_INSTANCE`
- `APP_URL_VALUE`
- `SERVICE_ACCOUNT_EMAIL` if you want a dedicated runtime service account

```bash
gcloud run jobs deploy price-increase-notification \
  --project PROJECT_ID \
  --region us-central1 \
  --image us-central1-docker.pkg.dev/PROJECT_ID/client-ops-pilot/price-increase-notification:latest \
  --service-account SERVICE_ACCOUNT_EMAIL \
  --set-env-vars BQ_PROJECT=PROJECT_ID,APP_URL=APP_URL_VALUE,NOTIFICATION_SENT_BY=cloud_run_job \
  --set-env-vars CLOUDSQL_HOST=/cloudsql/PROJECT_ID:us-central1:CLOUDSQL_INSTANCE,CLOUDSQL_DATABASE=client_ops,CLOUDSQL_USER=postgres \
  --set-secrets CLOUDSQL_PASSWORD=CLOUDSQL_PASSWORD:latest,MAILERSEND_API_KEY=MAILERSEND_API_KEY:latest \
  --set-cloudsql-instances PROJECT_ID:us-central1:CLOUDSQL_INSTANCE \
  --task-timeout 15m \
  --max-retries 1
```

## Test execute

```bash
gcloud run jobs execute price-increase-notification \
  --project PROJECT_ID \
  --region us-central1
```

Tail logs:

```bash
gcloud logging read "resource.type=cloud_run_job" \
  --project PROJECT_ID \
  --limit 50 \
  --freshness 6h
```

## Schedule daily

Create a Cloud Scheduler job that calls the Cloud Run Jobs API once per day.

```bash
gcloud scheduler jobs create http price-increase-notification-daily \
  --project PROJECT_ID \
  --location us-central1 \
  --schedule "0 9 * * *" \
  --time-zone "America/Chicago" \
  --http-method POST \
  --uri "https://run.googleapis.com/v2/projects/PROJECT_ID/locations/us-central1/jobs/price-increase-notification:run" \
  --oauth-service-account-email SCHEDULER_SERVICE_ACCOUNT_EMAIL
```

## Required IAM

Runtime service account:

- `roles/cloudsql.client`
- `roles/bigquery.jobUser`
- `roles/bigquery.dataViewer`
- `roles/secretmanager.secretAccessor`

Scheduler service account:

- permission to run the Cloud Run Job

## Failure behavior

- exits `0` when nothing is due
- exits non-zero if any email send fails

## Helper scripts

- `scripts/deploy_to_gcp.ps1`
  - builds the image, pushes it, deploys the Cloud Run Job, and executes it once
- `scripts/create_daily_scheduler.ps1`
  - creates the daily Cloud Scheduler trigger
