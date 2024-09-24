CREATE OR REPLACE VIEW dm_commercial.v_add_on_engagement_metrics AS 
WITH submitted_add_on AS (
	SELECT DISTINCT
		order_id
	FROM stg_curated.checkout_addons_submitted_v1 casv
)
, add_on_metrics AS (
SELECT 
	event_time::date AS fact_date,
	NULLIF(
			CASE 
				WHEN is_valid_json(te.properties) 
					THEN json_extract_path_text(te.properties, 'sku') 
			END,'') AS add_on_sku,
	CASE
		WHEN te.store_code IN ('de','business') THEN 'Germany'
		WHEN te.store_code IN ('us','business_us') THEN 'United States'
		WHEN te.store_code IN ('nl','business_nl') THEN 'Netherlands'
		WHEN te.store_code IN ('es','business_es') THEN 'Spain'
		WHEN te.store_code IN ('at','business_at') THEN 'Austria'
	END AS country_name,
	count(DISTINCT s.order_id) AS submitted_orders,
	count(DISTINCT CASE WHEN event_name = 'Add-On Viewed' THEN event_id END) AS add_on_viewed,
	count(DISTINCT CASE WHEN event_name = 'Add-On Details Clicked' THEN event_id END) AS add_on_clicked,
	count(DISTINCT CASE WHEN event_name = 'Add-On Added' THEN event_id END) AS add_on_added
FROM segment.track_events te 
LEFT JOIN submitted_add_on s
	ON s.order_id = te.order_id
WHERE properties LIKE '%DP00%'
	AND event_name <> 'Subscription Action Shown'
	GROUP BY 1,2,3
)
SELECT 
	date_trunc('week',fact_date)::date AS week_of_fact_date,
	add_on_sku,
	country_name,
	sum(submitted_orders) AS submitted_orders,
	sum(add_on_viewed) AS add_on_viewed,
	sum(add_on_clicked) AS add_on_clicked,
	sum(add_on_added) AS add_on_added
FROM add_on_metrics a 
WHERE add_on_sku IS NOT NULL 
GROUP BY 1,2,3
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_commercial.v_add_on_engagement_metrics TO hightouch;
