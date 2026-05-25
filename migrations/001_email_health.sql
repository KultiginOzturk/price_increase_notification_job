-- email_health — address-level email deliverability store (Cloud SQL Postgres, client_ops db).
--
-- Keyed by email address (NOT client) — verification verdicts and bounces are
-- properties of the address itself, shared across every account that uses it.
--
-- Populated by:
--   verify-emails.js      → verification_status / verification_checked_at
--   sync-email-status.js  → last_hard_bounce_at / last_spam_complaint_at
--
-- Consumed by tag reconciliation (Phase B) and send-time routing.
--
-- This table is auto-created at runtime by ensureEmailHealthSchema() in
-- lib/emailHealth.js; this file is the canonical reference / for manual DBA setup.

CREATE TABLE IF NOT EXISTS email_health (
    email                   text PRIMARY KEY,
    verification_status     varchar(32),   -- MailerSend verdict: valid / catch_all / mailbox_not_found / ...
    verification_checked_at timestamptz,   -- when verification last ran (drives the TTL cache)
    last_hard_bounce_at     timestamptz,   -- most recent hard bounce observed via the Activity API
    last_spam_complaint_at  timestamptz,   -- most recent spam complaint observed
    last_bounce_reason      text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);
