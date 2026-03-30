$ErrorActionPreference = "Stop"

# Fill these in before running.
$PROJECT_ID = "pco-prod"
$REGION = "us-central1"
$REPOSITORY = "price-increase-notification-job"
$IMAGE_NAME = "price-increase-notification"
$JOB_NAME = "price-increase-notification"
$CLOUDSQL_INSTANCE = "client-ops-warm-layer"
$APP_URL = "https://clientportal.pestanalytics.com"
$RUNTIME_SERVICE_ACCOUNT = "github-deploy@pco-prod.iam.gserviceaccount.com"

$IMAGE_URI = "${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/${IMAGE_NAME}:latest"
$CLOUDSQL_CONNECTION = "${PROJECT_ID}:${REGION}:${CLOUDSQL_INSTANCE}"

Write-Host "Setting gcloud project to $PROJECT_ID"
gcloud config set project $PROJECT_ID

Write-Host "Using existing Artifact Registry repository $REPOSITORY"


Write-Host "Building container image in Cloud Build: $IMAGE_URI"
gcloud builds submit `
  --project $PROJECT_ID `
  --tag $IMAGE_URI `
  .

Write-Host "Deploying Cloud Run Job $JOB_NAME"
gcloud run jobs deploy $JOB_NAME `
  --project $PROJECT_ID `
  --region $REGION `
  --image $IMAGE_URI `
  --service-account $RUNTIME_SERVICE_ACCOUNT `
  --set-env-vars "BQ_PROJECT=$PROJECT_ID,APP_URL=$APP_URL,NOTIFICATION_SENT_BY=cloud_run_job" `
  --set-env-vars "CLOUDSQL_HOST=/cloudsql/${CLOUDSQL_CONNECTION},CLOUDSQL_DATABASE=client_ops,CLOUDSQL_USER=postgres" `
  --set-secrets "CLOUDSQL_PASSWORD=CLOUDSQL_PASSWORD:latest,MAILERSEND_API_KEY=MAILERSEND_API_KEY:latest" `
  --set-cloudsql-instances $CLOUDSQL_CONNECTION `
  --task-timeout 15m `
  --max-retries 1

Write-Host "Executing Cloud Run Job once for validation"
gcloud run jobs execute $JOB_NAME --project $PROJECT_ID --region $REGION

Write-Host "Done. Check logs with:"
Write-Host "gcloud logging read `"resource.type=cloud_run_job`" --project $PROJECT_ID --limit 50 --freshness 6h"
