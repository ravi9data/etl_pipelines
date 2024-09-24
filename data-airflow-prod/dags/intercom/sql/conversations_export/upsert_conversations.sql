BEGIN;

DELETE FROM ods_intercom.conversations
USING ods_intercom.conversations_dl
WHERE ods_intercom.conversations.conversation_id = ods_intercom.conversations_dl.conversation_id;

INSERT INTO ods_intercom.conversations(
    conversation_id,
    created_at,
    updated_at,
    attachments,
    author,
    body,
    delivered_as,
    source_id,
    subject,
    url,
    redacted,
    contacts,
    teammates,
    title,
    admin_assignee_id,
    team_assignee_id,
    custom_attributes,
    "open",
    "state",
    "read",
    waiting_since,
    snoozed_until,
    tags,
    first_contact_reply,
    priority,
    sla_applied,
    conversation_rating,
    "statistics",
    conversation_parts,
    total_count
)
SELECT
    conversation_id,
    created_at,
    updated_at,
    attachments,
    author,
    body,
    delivered_as,
    source_id,
    subject,
    url,
    redacted,
    contacts,
    teammates,
    title,
    admin_assignee_id,
    team_assignee_id,
    custom_attributes,
    "open",
    "state",
    "read",
    waiting_since,
    snoozed_until,
    tags,
    first_contact_reply,
    priority,
    sla_applied,
    conversation_rating,
    "statistics",
    conversation_parts,
    total_count
FROM ods_intercom.conversations_dl;

COMMIT;
