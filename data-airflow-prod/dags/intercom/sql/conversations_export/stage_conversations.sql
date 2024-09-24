DROP TABLE IF EXISTS ods_intercom.conversations_dl;

CREATE TABLE ods_intercom.conversations_dl
DISTKEY(conversation_id)
SORTKEY(conversation_id)
AS
WITH cte AS (
SELECT id::VARCHAR(100),
       created_at::INTEGER,
       updated_at::INTEGER,
       attachments::VARCHAR(65535),
       author::VARCHAR(65535),
       body::VARCHAR(65535),
       delivered_as::VARCHAR(100),
       source_id::VARCHAR(100),
       subject::VARCHAR(65535),
       url::VARCHAR(65535),
       NULLIF(redacted, '<NA>')::VARCHAR(10),
       contacts::VARCHAR(5000),
       teammates::VARCHAR(5000),
       NULLIF(title, 'null')::VARCHAR(65535),
       NULLIF(admin_assignee_id, '<NA>')::VARCHAR(100),
       NULLIF(team_assignee_id, '<NA>')::VARCHAR(100),
       NULLIF(custom_attributes, 'null')::VARCHAR(65535),
       "open"::VARCHAR(1),
       "state"::VARCHAR(20),
       "read"::VARCHAR(1),
       NULLIF(waiting_since, '<NA>')::INTEGER,
       NULLIF(snoozed_until, '<NA>')::VARCHAR(50),
       tags::VARCHAR(65535),
       NULLIF(first_contact_reply, 'null')::VARCHAR(65535),
       priority::VARCHAR(50),
       NULLIF(sla_applied, 'null')::VARCHAR(65535),
       NULLIF(conversation_rating, 'null')::VARCHAR(65535),
       "statistics"::VARCHAR(65535),
       conversation_parts::VARCHAR(65535),
       total_count::VARCHAR(10),
       row_number() OVER (PARTITION BY id ORDER BY extracted_at DESC) AS rn
FROM s3_spectrum_intercom.conversations
  WHERE year = '{{ti.xcom_pull(key='year')}}'
  AND month = '{{ti.xcom_pull(key='month')}}'
  AND day = '{{ti.xcom_pull(key='day')}}'
  AND batch_id = '{{ti.xcom_pull(key='batch_id')}}')
SELECT
    id AS conversation_id,
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
FROM cte WHERE rn=1;
