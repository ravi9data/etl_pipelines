CREATE OR REPLACE VIEW dm_catman.v_weekly_performance_report_traffic_metrics AS 
WITH product_changed_and_product_click AS (
	SELECT
		te.event_time::date AS fact_day,
		te.event_id, 
		te.session_id,
		te.anonymous_id,
		split_part(page_path, '/', 2) AS url_split,
		REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(url_split, '-nl', ''), '-es', ''), '-en', ''), '-de', ''), '-', ''), '.[0-9]{3}') AS parsed_store_name,
		CASE
			WHEN parsed_store_name IN ('at', 'at_', 'at_at', 'at_lp', 'business_at') THEN 'Austria'
			WHEN parsed_store_name IN ('de', 'de_', 'de_lp', 'business', 'business_en', 'business_de') THEN 'Germany'
			WHEN parsed_store_name IN ('nl', 'nl_', 'nl_lp', 'business_nl') THEN 'Netherlands'
			WHEN parsed_store_name IN ('es', 'es_', 'es_lp', 'business_es') THEN 'Spain'
			WHEN parsed_store_name IN ('us', 'us_', 'us_us', 'us_lp', 'business_us') THEN 'United_States'
			WHEN te.store_id <> '-1' THEN st.country_name
			WHEN te.store_id = '-1' THEN 'Germany'
		END AS country_name,
		CASE
			WHEN parsed_store_name LIKE '%business%' THEN 'business_customer'
			ELSE 'normal_customer'
		END AS customer_type,
		CASE
			WHEN cu.is_new_visitor IS TRUE THEN 'NEW'
			ELSE 'RECURRING'
		END AS new_recurring,
		s.is_paid, 
		CASE
			WHEN te.event_name = 'Product Changed' THEN
				CASE
					WHEN is_valid_json(te.properties)
						THEN COALESCE(NULLIF(json_extract_path_text(te.properties, 'product_sku'), ''), '')
					ELSE NULL
				END
			WHEN te.event_name = 'Product Clicked' THEN
				CASE
					WHEN is_valid_json(te.properties)
						THEN COALESCE(NULLIF(json_extract_path_text(te.properties, 'product_sku'), ''),
									  NULLIF(json_extract_path_text(te.properties, 'productSKU'), ''),
									  NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties, 'eventProperty', 'product_sku'), ''),
									  NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties, 'eventProperty', 'productData', 'product_sku'), ''),
									  NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties, 'eventProperty', 'productData', 'productSku'), ''))
						ELSE NULL
				END
		END AS product_sku,
		CASE 
			WHEN is_valid_json(te.properties)
				THEN COALESCE(NULLIF(json_extract_path_text(te.properties, 'product_variant'), ''),
							  NULLIF(json_extract_path_text(te.properties, 'productVariant'), ''),
							  NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties, 'variant'), ''),
							  NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties, 'eventProperty', 'product_variant'), ''),
							  NULLIF(JSON_EXTRACT_PATH_TEXT(te.properties, 'eventProperty', 'productData', 'product_variant'), ''))
				ELSE NULL
		END AS variant_id,
		CASE
			WHEN te.event_name = 'Product Changed' 
				THEN
					CASE
						WHEN is_valid_json(te.properties)
							THEN COALESCE(NULLIF(json_extract_path_text(te.properties, 'variant', 'sku'), ''), '')
					END
			WHEN te.event_name = 'Product Clicked' 
				THEN product_sku || 'V' || variant_id
		END AS variant_sku
	FROM segment.track_events te
	LEFT JOIN traffic.sessions s
		ON s.session_id = te.session_id
	LEFT JOIN ods_production.store st
		ON st.id = s.store_id
	LEFT JOIN traffic.snowplow_user_mapping cu
		ON te.anonymous_id = cu.anonymous_id
			AND te.session_id = cu.session_id
	WHERE te.event_name IN ('Product Changed', 'Product Clicked')
		AND te.event_time::date >= dateadd('week',-4,date_trunc('week', current_date))::date
)
SELECT
	date_trunc('week', fact_day)::date AS week_of_fact_day,
	country_name,
	customer_type,
	new_recurring,
	product_sku,
	variant_sku,
	count(DISTINCT event_id ) AS pageviews,
	count(DISTINCT session_id) AS pageview_unique_sessions,
	count(DISTINCT CASE WHEN is_paid THEN session_id END) paid_traffic_sessions,
	count(DISTINCT anonymous_id) AS unique_users
FROM product_changed_and_product_click
WHERE country_name <> 'United_States'
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA BINDING
;
