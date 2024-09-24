--as we don't use snowplow anymore, mapping will stay in this table web.snowplow_user_mapping for period before May as it is
--we will process only segment data now for faster execution time
DROP TABLE IF EXISTS segment_user_mapping; 
CREATE TEMP TABLE segment_user_mapping AS
WITH first_session_date_segment_web AS (
	SELECT 
		anonymous_id,
		min(session_date::date) AS first_session_date
	FROM segment.sessions_web
	WHERE session_index = 1
	GROUP BY 1
),

first_session_date_segment_app AS (
	SELECT 
		a.anonymous_id,
		min(a.session_date::date) AS first_session_date
	FROM segment.sessions_app a
		LEFT JOIN first_session_date_segment_web b USING (anonymous_id)
	WHERE a.session_index = 1
		AND b.anonymous_id IS NULL
	GROUP BY 1
)

SELECT DISTINCT 
	sm.anonymous_id AS anonymous_id,
	sm.anonymous_id AS anonymous_id_new,
	sm.customer_id::VARCHAR AS customer_id_new,
	sm.session_id,
	CASE WHEN fsd.anonymous_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_new_visitor
FROM segment.sessions_web sm
LEFT JOIN first_session_date_segment_web fsd 
	ON sm.anonymous_id = fsd.anonymous_id
	AND sm.session_date::date = fsd.first_session_date
WHERE session_start >= '2023-05-01'

UNION ALL 

SELECT DISTINCT 
	sm.anonymous_id AS anonymous_id,
	sm.anonymous_id AS anonymous_id_new,
	sm.customer_id::VARCHAR AS customer_id_new,
	sm.session_id,
	CASE WHEN fsd.anonymous_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_new_visitor
FROM segment.sessions_app sm
LEFT JOIN first_session_date_segment_app fsd 
	ON sm.anonymous_id = fsd.anonymous_id
	AND sm.session_date::date = fsd.first_session_date
WHERE session_start >= '2023-05-01';

BEGIN transaction;

DELETE FROM traffic.snowplow_user_mapping
    USING segment_user_mapping b
WHERE snowplow_user_mapping.session_id = b.session_id;

INSERT INTO traffic.snowplow_user_mapping
SELECT *
FROM segment_user_mapping;

END transaction;

GRANT SELECT ON traffic.snowplow_user_mapping TO redash_growth;
GRANT SELECT ON traffic.snowplow_user_mapping TO ceyda_peker;
