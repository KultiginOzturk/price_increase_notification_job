-- Canonical deliverability tag definitions (Cloud SQL Postgres, client_ops db).
--
-- Registers the reason-coded tag keys written by lib/accountTags.js in
-- cfg_tag_definitions so the app's tag UI / no-email page can display them.
--
-- Unlike email_health, this is app-owned config — it is NOT auto-created by
-- the job. Apply it manually (or via the app's migration tooling).
--
-- Legacy client-created tags (No Email, missing_email, invalid_email,
-- physical_mail_2026, ...) are intentionally left in place; consolidating them
-- into these canonical keys is a separate one-time migration.

INSERT INTO cfg_tag_definitions (tag_type_key, display_name, description, is_global, tag_source, created_by)
SELECT v.tag_type_key, v.display_name, v.description, v.is_global, v.tag_source, v.created_by
FROM (VALUES
    ('no_email',
     'No Email',
     'Account has no email address on file. Routes to physical mail.',
     TRUE, 'system', 'email_health'),
    ('email_invalid',
     'Email Invalid',
     'Email address failed MailerSend verification (hard fail: syntax_error / mailbox_not_found / failed).',
     TRUE, 'system', 'email_health'),
    ('email_hard_bounced',
     'Email Hard Bounced',
     'Email address hard-bounced, observed via the MailerSend Activity API.',
     TRUE, 'system', 'email_health'),
    ('email_spam_complaint',
     'Email Spam Complaint',
     'Recipient marked a message from this address as spam.',
     TRUE, 'system', 'email_health')
) AS v(tag_type_key, display_name, description, is_global, tag_source, created_by)
WHERE NOT EXISTS (
    SELECT 1 FROM cfg_tag_definitions d WHERE d.tag_type_key = v.tag_type_key
);
