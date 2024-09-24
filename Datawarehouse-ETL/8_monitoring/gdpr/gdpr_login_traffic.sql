--we will skip the execution if the customer_id did not change
DELETE FROM hightouch_sources.gdpr_login_traffic;

INSERT INTO hightouch_sources.gdpr_login_traffic
	(customer_id
	,ip_address
	,last_visited
	,type_of_data
	)
WITH staging AS (
	SELECT 
		user_id as customer_id,
		ip_address, 
		max(creation_time)::timestamp as last_visited_at,
		'Login' as type_of_data
	WHERE user_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	GROUP BY 1,2,4
	UNION ALL
	SELECT 
		user_id as customer_id,
		ip_address, 
		max(creation_time)::timestamp as last_visited_at,
		'Registration' as type_of_data
	FROM stg_events.registration_view
	WHERE user_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	GROUP BY 1,2,4
	UNION ALL
	SELECT 
		customer_id,
		ip_address_order, 
		max(created_date)::timestamp as last_visited_at,
		'Order' as type_of_data
	FROM ods_production.order 
	WHERE customer_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	  AND submitted_date is not null
	GROUP BY 1,2,4
)
	SELECT 
		user_id::int AS customer_id,
		ip AS ip_address,
		max(event_time)::timestamp AS last_visited_at,
		CASE WHEN event_name IN ('Log IN Successful','Login Success', 'Login')
			 THEN 'Login'
			 WHEN event_name IN ('Signup Completed')
			 THEN 'Registration'
		END AS type_of_data
	FROM segment.track_events
	WHERE event_name IN ('Signup Completed','Log IN Successful','Login Success', 'Login')
	  AND user_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	GROUP BY 1,2,4
)
, react_native AS (
	SELECT 
		user_id::int AS customer_id,
		context_ip AS ip_address,
		max("timestamp")::timestamp AS last_visited_at,
		'Login' AS type_of_data
	FROM react_native.login_success
	WHERE user_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	GROUP BY 1,2,4
	UNION 
	SELECT 
		user_id::int AS customer_id,
		context_ip AS ip_address,
		max("timestamp")::timestamp AS last_visited_at,
		'Login' AS type_of_data
	FROM react_native.log_in_successful
	WHERE user_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	GROUP BY 1,2,4
	UNION 
	SELECT 
		user_id AS customer_id,
		context_ip AS ip_address,
		max("timestamp")::timestamp AS last_visited_at,
		'Login' AS type_of_data
	FROM react_native.login
	WHERE user_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	GROUP BY 1,2,4
	UNION 
	SELECT 
		user_id::int AS customer_id,
		context_ip AS ip_address,
		max("timestamp")::timestamp AS last_visited_at,
		'Registration' AS type_of_data
	FROM react_native.register
	WHERE user_id IN (SELECT customer_id FROM staging_google_sheet.gdpr_input)
	GROUP BY 1,2,4
)
SELECT 
	customer_id,
	ip_address,
	DATE_TRUNC('minute', last_visited_at) AS last_visited,
	type_of_data
FROM staging
UNION
SELECT 
	customer_id,
	ip_address,
	DATE_TRUNC('minute', last_visited_at) AS last_visited,
	type_of_data
UNION
SELECT 
	customer_id,
	ip_address,
	DATE_TRUNC('minute', last_visited_at) AS last_visited,
	type_of_data
FROM react_native
;