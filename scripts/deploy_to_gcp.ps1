#
# Deploy the price-increase-notification Cloud Run Job.
#
# Usage:
#   ./scripts/deploy_to_gcp.ps1                       # deploys to pco-dev3 (default)
#   ./scripts/deploy_to_gcp.ps1 -Project pco-cleanup  # deploys to cleanup
#
# Per-project settings (service account, Artifact Registry repo, APP_URL)
# live in $PROJECT_CONFIG below — add an entry to target a new project.
#
param(
    [ValidateSet("pco-dev3", "pco-cleanup")]
    [string]$Project = "pco-dev3"
)

$ErrorActionPreference = "Stop"

$REGION            = "us-central1"
$IMAGE_NAME        = "price-increase-notification"
$JOB_NAME          = "price-increase-notification"
$CLOUDSQL_INSTANCE = "client-ops-warm-layer"

# ── Per-project configuration ────────────────────────────────────────────
# repo    — Artifact Registry repository the image is pushed to
# sa      — runtime service account for the Cloud Run Job
# appUrl  — base URL for customer-facing unsubscribe links
$PROJECT_CONFIG = @{
    "pco-dev3" = @{
        repo   = "cloud-run-jobs"
        sa     = "price-push-api-sa@pco-dev3.iam.gserviceaccount.com"
        appUrl = "https://client-portal-dev3-mkvc5sdqiq-uc.a.run.app"
    }
    "pco-cleanup" = @{
        repo   = "price-increase-notification-job"
        sa     = "price-push-api-sa@pco-cleanup.iam.gserviceaccount.com"
        appUrl = "https://clientportal.pestanalytics.com"
    }
}

$cfg = $PROJECT_CONFIG[$Project]
if (-not $cfg) { throw "No deploy config for project '$Project'" }

$PROJECT_ID              = $Project
$REPOSITORY              = $cfg.repo
$RUNTIME_SERVICE_ACCOUNT = $cfg.sa
$APP_URL                 = $cfg.appUrl

$IMAGE_URI          = "${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:latest"
$CLOUDSQL_CONNECTION = "${PROJECT_ID}:${REGION}:${CLOUDSQL_INSTANCE}"

Write-Host "Deploying price-increase-notification job"
Write-Host "  project = $PROJECT_ID"
Write-Host "  image   = $IMAGE_URI"
Write-Host "  sa      = $RUNTIME_SERVICE_ACCOUNT"
Write-Host "  appUrl  = $APP_URL"

gcloud config set project $PROJECT_ID

Write-Host "Building container image in Cloud Build: $IMAGE_URI"
gcloud builds submit `
  --project $PROJECT_ID `
  --tag $IMAGE_URI `
  .

# PLAN_SCHEMA=renamed: the pco-dev3 / pco-cleanup warm layers use the renamed
# plan_* tables (plan_run, plan_account_decision, ...); the job's config/tables.js
# otherwise defaults to the legacy planv2_* names.
Write-Host "Deploying Cloud Run Job $JOB_NAME"
gcloud run jobs deploy $JOB_NAME `
  --project $PROJECT_ID `
  --region $REGION `
  --image $IMAGE_URI `
  --service-account $RUNTIME_SERVICE_ACCOUNT `
  --set-env-vars "BQ_PROJECT=$PROJECT_ID,APP_URL=$APP_URL,NOTIFICATION_SENT_BY=cloud_run_job,NOTIFICATION_AUTO_CONFIRM=true,PLAN_SCHEMA=renamed" `
  --set-env-vars "CLOUDSQL_HOST=/cloudsql/${CLOUDSQL_CONNECTION},CLOUDSQL_DATABASE=client_ops,CLOUDSQL_USER=postgres" `
  --set-secrets "CLOUDSQL_PASSWORD=CLOUDSQL_PASSWORD:latest,MAILERSEND_API_KEY=MAILERSEND_API_KEY:latest" `
  --set-cloudsql-instances $CLOUDSQL_CONNECTION `
  --task-timeout 15m `
  --max-retries 1

Write-Host "Done. Check logs with:"
Write-Host "gcloud logging read `"resource.type=cloud_run_job`" --project $PROJECT_ID --limit 50 --freshness 6h"
