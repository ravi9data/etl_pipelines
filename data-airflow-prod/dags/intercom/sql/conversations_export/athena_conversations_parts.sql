WITH ordered AS (
    SELECT id,
           extracted_at,
           batch_id,
           conversation_parts,
           year,
           month,
           day,
           row_number() OVER (PARTITION BY id ORDER BY extracted_at DESC) AS rn
    FROM :table_conversations;
    WHERE year = :year;
    AND month=:month;
    AND day=:day;
    AND batch_id=:batch_id;
),
deduped AS (
SELECT
    id,
    extracted_at,
    batch_id,
    conversation_parts,
    year,
    month,
    day
FROM ordered
WHERE rn=1
)
SELECT id as conversation_id,
       ROW_NUMBER() OVER (PARTITION BY id)                               AS conversation_part_number,
       CAST(json_extract(conversation_part, '$.id') AS VARCHAR)          AS conversation_part_id,
       CAST(json_extract(conversation_part, '$.part_type') AS VARCHAR)   AS part_type,
       CAST(json_extract(conversation_part, '$.body') AS VARCHAR)        as body,
       CAST(json_extract(conversation_part, '$.created_at') AS VARCHAR)  AS created_at,
       CAST(json_extract(conversation_part, '$.updated_at') AS VARCHAR)  AS updated_at,
       CAST(json_extract(conversation_part, '$.notified_at') AS VARCHAR) AS notified_at,
       json_format(json_extract(conversation_part, '$.assigned_to'))     AS assigned_to,
       json_format(json_extract(conversation_part, '$.author'))          AS author,
       json_format(json_extract(conversation_part, '$.attachments'))     AS attachments,
       json_format(json_extract(conversation_part, '$.external_id'))     AS external_id,
       CAST(json_extract(conversation_part, '$.redacted') AS VARCHAR)    AS redacted,
       batch_id,
       extracted_at,
       year,
       month,
       day
FROM deduped
         CROSS JOIN UNNEST(CAST(json_parse(conversation_parts) AS array(JSON))) AS t(conversation_part)
ORDER BY id, conversation_part_number;
