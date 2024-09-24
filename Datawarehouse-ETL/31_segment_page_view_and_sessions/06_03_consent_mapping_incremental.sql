
CREATE TEMP TABLE tmp_cookie_consent AS
WITH last_3_days_sessions AS (
    SELECT DISTINCT
        session_id
    FROM segment.identify_events
    WHERE loaded_at >= CURRENT_DATE - 3
),

sessions_with_consent AS (
    SELECT
        session_id,
        CASE WHEN IS_VALID_JSON(traits) THEN JSON_EXTRACT_PATH_TEXT(traits, 'cookie_consent') ELSE NULL END AS cookie_consent,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC) AS rn
    FROM segment.identify_events
        INNER JOIN last_3_days_sessions USING (session_id)
    WHERE traits ILIKE '%cookie_consent%'
)

SELECT 
    session_id,
    CASE WHEN IS_VALID_JSON(cookie_consent) THEN JSON_EXTRACT_PATH_text(cookie_consent, 'profiling') ELSE NULL END marketing_consent,
    CASE WHEN IS_VALID_JSON(cookie_consent) THEN JSON_EXTRACT_PATH_text(cookie_consent, 'performance') ELSE NULL END performance_consent,
    CASE WHEN IS_VALID_JSON(cookie_consent) THEN JSON_EXTRACT_PATH_text(cookie_consent, 'functional') ELSE NULL END functional_consent
FROM sessions_with_consent
WHERE rn = 1;

BEGIN transaction;

DELETE FROM segment.cookie_consent
    USING tmp_cookie_consent b
WHERE cookie_consent.session_id = b.session_id;

INSERT INTO segment.cookie_consent
SELECT *
FROM tmp_cookie_consent;

END transaction;
