# CLAUDE.md — agent orientation

Operational docs (deploy, env vars, scheduling, IAM) live in [README.md](README.md). This file is the architecture map and gotcha list for code work.

## What this repo is

A standalone Cloud Run Job that runs once per day and sends pre-push price-increase emails to customers whose rollout is "due today". It is a slimmed-down extraction of the main Client-Ops app — it shares table contracts, services, and a few utilities with that app, but runs independently.

The runtime is **Node 20, ESM** (`"type": "module"` in [package.json](package.json)). Top-level `await` is used in entry files and dynamic-import scripts.

## Entry points

| File | Purpose |
| --- | --- |
| [index.js](index.js) | Loads `.env` from project root, then imports `main.js`. The Docker `CMD` runs this. |
| [main.js](main.js) | The job. Reads env, calls `runDuePrePushNotifications`, runs interactive preflight, writes `send-report-*.xlsx`, exits non-zero on any send failure. |
| [test-send-one.js](test-send-one.js) | Sends one hardcoded sample email to `TEST_TO`. Used to verify MailerSend / sender config end-to-end. |
| [export-eligible.js](export-eligible.js) | Dry-run: dumps the eligible-target list for a client to xlsx without sending. |
| [send-correction.js](send-correction.js) | One-off correction-email sender (a previous incident remediation script). Not part of the daily job. |

The "real work" entry into the service layer is [`runDuePrePushNotifications`](services/priceIncreaseNotificationService.js#L941) — every entry point above ultimately funnels through it (or its sub-functions).

## Module layout

```
config/  tables.js                          — central BQ/Postgres table-name constants
lib/     postgres.js                        — Cloud SQL pg.Pool + helpers
services/
         priceIncreaseNotificationService.js — orchestration: find due → build targets → eligibility → send → audit
         planV2PricePushService.js          — builds the per-(client, effectivePeriod) batch from the latest published plan
         emailService.js                    — MailerSend client + price-increase template (supports dryRun for preflight)
routes/  planV2/timing.js                   — converts effective_period (YYYY-MM) → noticeDate / effectiveDate
utils/   bigquery.js                        — runQuery wrapper around @google-cloud/bigquery
         pricePushMath.js, repricingScheduling.js — pricing math used by planV2PricePushService
scripts/                                    — ad-hoc operator/maintenance scripts (NOT run in the job container)
```

`lib/`, `services/`, `config/`, `utils/`, `routes/` are intentionally minimal copies of the main app's modules. When fixing a bug here, **check whether the same code exists in the main app** — divergence has bitten this repo before.

## How a run actually flows

1. **Find due periods** — `findDuePrePushNotificationPeriods` queries Cloud SQL Postgres for the latest `published` plan per client, joins `*_account_decision`, filters to periods where `noticeDate <= targetDate <= effectiveDate`. Timing is derived in [routes/planV2/timing.js](routes/planV2/timing.js) from the `YYYY-MM` `effective_period` string.
2. **Build the batch** — `buildPlanV2PricePushSource` (from `planV2PricePushService.js`) reconstructs accounts + subscriptions + new prices for that (client, period). This is the same logic the UI uses; do not invent a parallel path.
3. **Build targets** — `buildPrePushNotificationTargets` joins to BigQuery `cur_customer` (+ `norm_customer_fieldroutes` + raw `FR_CUSTOMER`) to pick the contact email. Email priority: `billing_email` → `email`. Name priority: `billing_first_name` (raw) → `first_name`.
4. **Annotate eligibility** — `annotateNotificationEligibility` assigns each target one of: `eligible`, `excluded_tag`, `no_email`, `unsubscribed`, `already_sent`. **Order matters** — `excluded_tag` wins over `no_email`, and `already_sent` is checked last. See [`annotateNotificationEligibility`](services/priceIncreaseNotificationService.js#L519).
5. **Preflight** — when `NOTIFICATION_AUTO_CONFIRM` is unset, `interactivePreflight` in `main.js` walks the operator through 4–5 y/N gates (sender identity, counts, excluded tags, sample email, sanity warnings). The Cloud Run Job sets `NOTIFICATION_AUTO_CONFIRM=true` to skip this.
6. **Send** — `sendNotificationTargets` calls MailerSend per target (rate-limited to `MAILERSEND_MAX_REQUESTS_PER_MINUTE`, default 10/min) and writes audit rows.
7. **Audit** — every target outcome (sent, failed, skipped_*) is inserted into BigQuery `inp_price_increase_notification_events`. Post-push sends additionally write to the legacy `inp_price_increase_notifications` table.

## Non-obvious things

- **Two plan-table naming schemes.** [config/tables.js](config/tables.js#L269-L286) reads `PLAN_SCHEMA`. Default `legacy` → `planv2_*`; `renamed` → `plan_run` / `plan_*`. If a query against plan tables fails with "relation does not exist", check this env var.
- **Per-client BQ datasets.** RCP tables live in `rcp_${CLIENT}` datasets — always go through `getRCP(client)`. The bare `RCP` proxy throws on access by design (see [config/tables.js](config/tables.js#L138-L145)).
- **`BQ_PROJECT` is required at import time.** [config/tables.js](config/tables.js#L13-L16) throws if it's missing — the job won't even start without it. This is intentional cross-project safety.
- **Dedup keys differ by mode.** Pre-push: `pre_push:{planId}:{effectivePeriod}:{masterAccountId}`. Post-push: `post_push:{queueId}`. See [`buildDedupKey`](services/priceIncreaseNotificationService.js#L82). The `already_sent` check reads from both `inp_price_increase_notification_events` and the legacy `inp_price_increase_notifications` table.
- **Test sends do not block real sends.** When `NOTIFICATION_TEST_RECIPIENT` is set, the audit row is written with `mode = test_pre_push` so the dedup query (which filters `mode = 'pre_push'`) won't treat it as already sent.
- **Postgres DATE typecast.** [lib/postgres.js](lib/postgres.js#L24) overrides pg's DATE parser to return `'YYYY-MM-DD'` strings instead of JS `Date` objects — preserves timezone-agnostic calendar dates. Don't remove this.
- **Cloud SQL host detection.** [lib/postgres.js](lib/postgres.js#L27) treats `CLOUDSQL_HOST` starting with `/cloudsql/` as a Unix socket (Cloud Run); anything else is TCP (local dev via cloud-sql-proxy).
- **MailerSend rate limit is in-process.** Multiple parallel job runs would not coordinate; the job is single-process by design.
- **Effective period → year inference.** `getEffectiveYear` in [routes/planV2/timing.js](routes/planV2/timing.js#L49) chooses next-year for past months relative to the reference date. Don't "fix" this without checking the late-year planning behavior.
- **Send reports.** Every run writes `send-reports/send-report-{CLIENT}-{ISO}.xlsx` (the directory is created on demand). These are gitignored (`*.xlsx` in [.gitignore](.gitignore)) but accumulate locally — feel free to delete.

## Eligibility states (exhaustive)

Set on `target.eligibility`, mirrored to per-row `sendStatus` in the xlsx and to the `status` column in the audit table:

| State | Meaning |
| --- | --- |
| `eligible` | Will be sent (subject to `NOTIFICATION_LIMIT`) |
| `excluded_tag` | Account carries one of the tags in `notification_excluded_tags` client setting |
| `no_email` | No valid email after `billing_email`/`email` resolution |
| `unsubscribed` | Email is in `cfg_email_unsubscribes` for `price_increase` or `all` |
| `already_sent` | Audit table shows a prior `sent` row matching the dedup key |

Plus runtime-only outcomes appended to `sendStatus`: `skipped_send_limit`, `failed: <reason>`, `test`, `sent`.

## Sender config

`fetchNotificationConfig(client)` in [services/priceIncreaseNotificationService.js](services/priceIncreaseNotificationService.js#L556) reads `inp_client_settings` rows where `setting_key LIKE 'notification_%'`. Keys consumed:

- `notification_from_address` — full email or local-part (combined with `notification_from_domain` or `MAILERSEND_FROM_DOMAIN`)
- `notification_from_domain`
- `notification_from_name`
- `notification_excluded_tags` — comma-separated list of tag keys; matched accounts get `excluded_tag`
- All other `notification_*` keys are passed through as `templateConfig` to `emailService` for template substitution

Reply-to falls back to `NOTIFICATION_REPLY_TO_FALLBACK` (or `nate@pestnotifications.com`) when the client hasn't configured a verified address.

## Scripts directory

Operator-only — not bundled into the Cloud Run image's entry point. Each loads `.env` from the repo root via `existsSync`/`config({ path: ... })` then dynamic-imports the service modules.

- [scripts/backfillEvents.js](scripts/backfillEvents.js) — replays historical periods to populate `inp_price_increase_notification_events`. See `scripts/backfill-log.txt` for past runs.
- [scripts/countOfficeSends.js](scripts/countOfficeSends.js) — quick send-count breakdown by office for spot checks.
- [scripts/enrichBouncedCustomers.js](scripts/enrichBouncedCustomers.js) — joins MailerSend bounce activity exports against customer records.
- [scripts/deploy_to_gcp.ps1](scripts/deploy_to_gcp.ps1), [scripts/create_daily_scheduler.ps1](scripts/create_daily_scheduler.ps1) — deploy/schedule helpers (Windows PowerShell).

## Local development

```bash
npm install
node index.js                                                # full run, today's date, all clients
NOTIFICATION_CLIENTS=MODERN NOTIFICATION_TARGET_DATE=2026-12-01 node index.js   # backfill one client
NOTIFICATION_TEST_RECIPIENT=you@example.com node index.js    # redirect all sends to a test inbox
NOTIFICATION_AUTO_CONFIRM=true node index.js                 # skip the interactive preflight (what Cloud Run sets)
node test-send-one.js                                        # one hardcoded sample (TEST_TO required)
node export-eligible.js MODERN                               # dry run → xlsx, no sends
npm run check                                                # syntax-checks every entry/module
```

Cloud SQL needs cloud-sql-proxy running locally:

```bash
./bin/cloud-sql-proxy pco-prod:us-central1:client-ops-warm-layer --port=5432
```

BigQuery uses ADC locally — set `USE_ADC=true` and run `gcloud auth application-default login` once.

## When making changes

- **Touching SQL?** Mind the per-client RCP rule, the `PLAN_SCHEMA` legacy/renamed split, and the SHARED curation/normalization tables.
- **Touching eligibility?** Update both the state enum in `annotateNotificationEligibility` *and* `buildNotificationSummary` (counts) *and* the skip branches in `sendNotificationTargets` (audit rows). All three list the same set of states.
- **Touching the audit insert?** Two tables: `inp_price_increase_notification_events` (always) and `inp_price_increase_notifications` (post-push only, legacy). The legacy table uses `subscription_count`; the new one uses `service_count`.
- **Adding env vars?** Document them in [README.md](README.md) under "Required" or "Optional" *and* update the deploy commands in `scripts/deploy_to_gcp.ps1`.
