DROP TABLE IF EXISTS ods_intercom.conversations_parts_dl;

CREATE TABLE ods_intercom.conversations_parts_dl
DISTKEY(conversation_id)
SORTKEY(conversation_id, conversation_part_id)
AS
SELECT
      md5(conversation_id || conversation_part_id) as row_id,
      conversation_id::VARCHAR(100),
      conversation_part_number::INTEGER,
      conversation_part_id::VARCHAR(100),
      part_type::VARCHAR(200),
      body::VARCHAR(65535),
      NULLIF(created_at, '<NA>')::INTEGER,
      NULLIF(updated_at, '<NA>')::INTEGER,
      NULLIF(notified_at, '<NA>')::INTEGER,
      NULLIF(assigned_to, 'null')::VARCHAR(5000),
      author::VARCHAR(20000),
      attachments::VARCHAR(65535),
      NULLIF(external_id, 'null')::VARCHAR(20000),
      CASE WHEN redacted = 'false' THEN false ELSE true END AS redacted
FROM s3_spectrum_intercom.conversations_parts
  WHERE year = '{{ti.xcom_pull(key='year')}}'
  AND month = '{{ti.xcom_pull(key='month')}}'
  AND day = '{{ti.xcom_pull(key='day')}}'
  AND batch_id = '{{ti.xcom_pull(key='batch_id')}}';
