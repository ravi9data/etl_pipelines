BEGIN;

DELETE FROM ods_intercom.conversations_parts
WHERE conversation_id IN (SELECT DISTINCT conversation_id FROM ods_intercom.conversations_parts_dl);

INSERT INTO ods_intercom.conversations_parts(
    row_id,
    conversation_id,
    conversation_part_number,
    conversation_part_id,
    part_type,
    body,
    created_at,
    updated_at,
    notified_at,
    assigned_to,
    author,
    attachments,
    external_id,
    redacted
)
SELECT
    row_id,
    conversation_id,
    conversation_part_number,
    conversation_part_id,
    part_type,
    body,
    created_at,
    updated_at,
    notified_at,
    assigned_to,
    author,
    attachments,
    external_id,
    redacted
FROM ods_intercom.conversations_parts_dl;

COMMIT;
