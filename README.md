# Price Increase Notification Job

Daily Cloud Run Job entrypoint for automated pre-push customer notifications.

## What it does

On each run, the job:

1. Finds the latest published PlanV2 plan for each client.
2. Finds rollout periods whose derived `noticeDate` is due on the job date.
3. Builds pre-push notification recipients from the existing pricing workflow.
4. Sends emails only to `eligible` targets.
5. Relies on existing duplicate suppression so already-sent accounts are skipped safely.

Current due rule:

- `noticeDate <= targetDate <= effectiveDate`
- `targetDate` defaults to the current UTC date.
- If the job misses a day, it can still catch up before the effective date.

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

## Local run

```bash
node server/jobs/price_increase_notification_job/index.js
```

Example backfill for one client:

```bash
NOTIFICATION_CLIENTS=MODERN NOTIFICATION_TARGET_DATE=2026-12-01 node server/jobs/price_increase_notification_job/index.js
```

## Build and deploy

Build and push an image with the dedicated Dockerfile:

```bash
docker build -f server/jobs/price_increase_notification_job/Dockerfile -t us-central1-docker.pkg.dev/PROJECT_ID/client-ops-pilot/price-increase-notification:latest .
docker push us-central1-docker.pkg.dev/PROJECT_ID/client-ops-pilot/price-increase-notification:latest
```

Deploy it as a Cloud Run Job:

```bash
gcloud run jobs deploy price-increase-notification \
  --image us-central1-docker.pkg.dev/PROJECT_ID/client-ops-pilot/price-increase-notification:latest \
  --region us-central1
```

Manual execution:

```bash
gcloud run jobs execute price-increase-notification --region us-central1
```

Then wire Cloud Scheduler to execute the Cloud Run Job once per day at your preferred business time.

## Failure behavior

- The job exits successfully when nothing is due.
- The job exits non-zero if any email send fails, so Cloud Run Job retries/alerting can catch real delivery issues.
