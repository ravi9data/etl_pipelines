DROP TABLE IF EXISTS segment.cookie_consent;
CREATE TABLE segment.cookie_consent AS
WITH sessions_with_consent AS (
    SELECT
        session_id,
        CASE WHEN IS_VALID_JSON(traits) THEN JSON_EXTRACT_PATH_TEXT(traits, 'cookie_consent') ELSE NULL END AS cookie_consent,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC) AS rn
    FROM segment.identify_events
    WHERE loaded_at >= '2023-01-01'
      AND traits ILIKE '%cookie_consent%'
)

SELECT session_id,
       CASE WHEN IS_VALID_JSON(cookie_consent) THEN JSON_EXTRACT_PATH_text(cookie_consent, 'profiling') ELSE NULL END marketing_consent,
       CASE WHEN IS_VALID_JSON(cookie_consent) THEN JSON_EXTRACT_PATH_text(cookie_consent, 'performance') ELSE NULL END performance_consent,
       CASE WHEN IS_VALID_JSON(cookie_consent) THEN JSON_EXTRACT_PATH_text(cookie_consent, 'functional') ELSE NULL END functional_consent
FROM sessions_with_consent
WHERE rn = 1;

GRANT SELECT ON segment.cookie_consent TO hams;