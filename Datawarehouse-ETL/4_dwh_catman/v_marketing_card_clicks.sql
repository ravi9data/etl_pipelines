CREATE OR REPLACE VIEW dm_commercial.v_marketing_card_clicks AS 
WITH marketing_card_events AS ( 
	SELECT 
		event_id,
		collector_tstamp::date AS click_date,
		platform AS platform,
		se_category AS category,
		se_label AS label,
		user_id,
		customer_id,
		domain_userid,
		se_property,
		substring(json_extract_path_text(se_property,'destination_url'),2,3) AS store_def,
		CASE
			WHEN store_def = 'de-' THEN 'Germany'
			WHEN store_def = 'es-' THEN 'Spain'
			WHEN store_def = 'nl-' THEN 'Netherlands'
			WHEN store_def = 'us-' THEN 'United States'
			WHEN store_def = 'at-' THEN 'Austria'
		END AS store_session,
		CASE WHEN c.signup_country <> 'never_add_to_cart' THEN c.signup_country END sign_up_country ,
		c.shipping_country ,
		c.billing_country ,
		CASE WHEN geo_country = 'DE' THEN 'Germany'
		     WHEN geo_country = 'AT' THEN 'Austria'
		     WHEN geo_country = 'ES' THEN 'Spain'
		     WHEN geo_country = 'NL' THEN 'Netherlands'
		     WHEN geo_country = 'US' THEN 'United States'
		END AS store_browser
	FROM scratch.se_events_flat f
	LEFT JOIN master.customer c 
	  ON f.user_id = c.customer_id 
	  WHERE collector_tstamp::date BETWEEN DATEADD('month', -3, date_trunc('week',current_date)) AND current_date
	  AND se_action = 'widgetClick'
	  AND se_category IN ('MC1', 'MC2','MC3')
	  AND collector_tstamp < '2023-05-01'
	UNION 
	SELECT 
		f.event_id,
		f.event_time::date AS click_date,
		s.platform,
		JSON_EXTRACT_PATH_TEXT(f.properties,'widget') AS category,
		JSON_EXTRACT_PATH_TEXT(f.properties,'widget_name') AS label,
		f.user_id,
		c.customer_id,
		f.anonymous_id AS domain_userid,
		f.properties AS se_property,
		split_part(split_part(f.page_path,'/',2),'-',1) AS store_def,
		CASE
				WHEN store_def = 'de' THEN 'Germany'
				WHEN store_def = 'es' THEN 'Spain'
				WHEN store_def = 'nl' THEN 'Netherlands'
				WHEN store_def = 'us' THEN 'United States'
				WHEN store_def = 'at' THEN 'Austria'
			END AS store_session,
		CASE WHEN c.signup_country <> 'never_add_to_cart' THEN c.signup_country END sign_up_country ,
		c.shipping_country ,
		c.billing_country ,
		NULL AS store_browser
	--	CASE WHEN geo_country = 'DE' THEN 'Germany'
	--		     WHEN geo_country = 'AT' THEN 'Austria'
	--		     WHEN geo_country = 'ES' THEN 'Spain'
	--		     WHEN geo_country = 'NL' THEN 'Netherlands'
	--		     WHEN geo_country = 'US' THEN 'United States'
	--		END AS store_browser
	FROM segment.track_events f
	LEFT JOIN master.customer c 
	  ON f.user_id = c.customer_id 
	LEFT JOIN segment.sessions_web s
	  ON s.session_id = f.session_id
	WHERE f.event_time::date BETWEEN DATEADD('month', -3, date_trunc('week',current_date)) AND current_date
	  AND f.event_name = 'Widget Clicked'
	  AND JSON_EXTRACT_PATH_TEXT(f.properties,'widget') IN ('MC1', 'MC2','MC3')
	  AND f.event_time >= '2023-05-01'
)
SELECT 
	e.click_date,
	e.category,
	e.label,
	COALESCE(store_session,shipping_country,sign_up_country,store_browser) AS store,
	count(event_id) AS clicks_count,
	count(DISTINCT domain_userid) AS unique_users
FROM marketing_card_events e
WHERE store IS NOT NULL
GROUP BY 1,2,3,4
WITH NO SCHEMA BINDING
;
