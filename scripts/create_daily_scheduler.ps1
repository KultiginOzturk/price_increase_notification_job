$ErrorActionPreference = "Stop"

# Fill these in before running.
$PROJECT_ID = "YOUR_PROJECT_ID"
$REGION = "us-central1"
$JOB_NAME = "price-increase-notification"
$SCHEDULER_JOB_NAME = "price-increase-notification-daily"
$SCHEDULE = "0 9 * * *"
$TIME_ZONE = "America/Chicago"
$SCHEDULER_SERVICE_ACCOUNT = "YOUR_SCHEDULER_SERVICE_ACCOUNT@${PROJECT_ID}.iam.gserviceaccount.com"

$RUN_URI = "https://run.googleapis.com/v2/projects/${PROJECT_ID}/locations/${REGION}/jobs/${JOB_NAME}:run"

gcloud scheduler jobs create http $SCHEDULER_JOB_NAME `
  --project $PROJECT_ID `
  --location $REGION `
  --schedule $SCHEDULE `
  --time-zone $TIME_ZONE `
  --http-method POST `
  --uri $RUN_URI `
  --oauth-service-account-email $SCHEDULER_SERVICE_ACCOUNT

Write-Host "Created scheduler job $SCHEDULER_JOB_NAME"
