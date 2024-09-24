DROP TABLE IF EXISTS web.snowplow_user_mapping;
CREATE TABLE web.snowplow_user_mapping AS
WITH sessions_data AS (
    SELECT
        snowplow_user_id,
        COALESCE(first_value(customer_id) ignore nulls over
            (partition by snowplow_user_id, session_start::DATE order by session_start rows between unbounded preceding and unbounded following),
                 first_value(customer_id) ignore nulls over
                     (partition by snowplow_user_id order by session_start rows between unbounded preceding and unbounded following)) as customer_id,
        ip_address,
        session_id,
        session_start,
        session_index 
    FROM web.sessions_snowplow --_snowplow
    WHERE session_start < '2023-05-01'
)
,ip_address_level_data AS (
    SELECT
        ip_address,
        count(distinct snowplow_user_id) AS total_snowplow_user_ids,
        MAX(snowplow_user_id) AS snowplow_user_id
    FROM sessions_data
    GROUP BY 1
    HAVING total_snowplow_user_ids <=5
)
,prepare_snowplow_users_match_data AS (
    SELECT
        a.snowplow_user_id,
        a.session_id,
        a.customer_id,
        a.ip_address,
        b.total_snowplow_user_ids,
        b.ip_address AS matching_ip_address,
        b.snowplow_user_id AS matching_snowplow_user_id,
        min(a.session_start)::date AS session_date,
        min(a.session_index) AS session_index
    FROM sessions_data a
    LEFT JOIN ip_address_level_data b ON a.ip_address = b.ip_address AND a.customer_id IS NULL
    GROUP BY 1,2,3,4,5,6,7
)
,snowplow_user_id_customer_id_mappping AS (
    SELECT snowplow_user_id,
           MAX(customer_id) AS customer_id
    FROM sessions_data
    WHERE customer_id IS NOT NULL
    GROUP BY 1
)
,snowplow_filtering AS (
    SELECT snowplow_user_id,
           matching_snowplow_user_id,
           ROW_NUMBER() OVER (PARTITION BY snowplow_user_id ORDER BY total_snowplow_user_ids ASC) AS rn
    FROM prepare_snowplow_users_match_data
    WHERE matching_ip_address IS NOT NULL
)
, snowplow_mapping AS (
	SELECT DISTINCT
	    a.snowplow_user_id,
	    COALESCE(b.matching_snowplow_user_id, a.snowplow_user_id) AS snowplow_user_id_new,
	    COALESCE(a.customer_id, c.customer_id) AS customer_id_new,
	    a.session_id,
	    a.session_date,
	    a.session_index
	FROM prepare_snowplow_users_match_data a
	    LEFT JOIN snowplow_filtering b
	        ON a.snowplow_user_id = b.snowplow_user_id AND b.rn = 1
	    LEFT JOIN snowplow_user_id_customer_id_mappping c
	        ON b.matching_snowplow_user_id = c.snowplow_user_id
)
, first_session_date_snowplow AS (
	SELECT 
		snowplow_user_id_new,
		min(session_date) AS first_session_date
	FROM snowplow_mapping
	WHERE session_index = 1
	GROUP BY 1
)
, first_session_date_segment AS (
	SELECT 
		anonymous_id,
		min(session_date::date) AS first_session_date
	FROM segment.sessions_web
	WHERE session_index = 1
	GROUP BY 1
)
SELECT DISTINCT 
	sm.snowplow_user_id as anonymous_id,
	sm.snowplow_user_id_new as anonymous_id_new,
	sm.customer_id_new,
	sm.session_id,
	CASE WHEN fsd.snowplow_user_id_new IS NOT NULL THEN TRUE ELSE FALSE END AS is_new_visitor
FROM snowplow_mapping sm
LEFT JOIN first_session_date_snowplow fsd 
	ON sm.snowplow_user_id_new = fsd.snowplow_user_id_new
	AND sm.session_date = fsd.first_session_date
UNION 
SELECT DISTINCT 
	sm.anonymous_id as anonymous_id,
	sm.anonymous_id as anonymous_id_new,
	sm.customer_id::varchar AS customer_id_new,
	sm.session_id,
	CASE WHEN fsd.anonymous_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_new_visitor
FROM segment.sessions_web sm
LEFT JOIN first_session_date_segment fsd 
	ON sm.anonymous_id = fsd.anonymous_id
	AND sm.session_date::date = fsd.first_session_date
WHERE session_start >= '2023-05-01';
	