CREATE OR REPLACE VIEW dm_commercial.v_35UP_orders_funnel AS
WITH product_added_to_cart AS (
	SELECT
		session_id,
		event_time,
		event_id,
		COALESCE(s.country_name,
			CASE
				WHEN te.store_code IN ('de','business') THEN 'Germany'
				WHEN te.store_code IN ('us','business_us') THEN 'United States'
				WHEN te.store_code IN ('nl','business_nl') THEN 'Netherlands'
				WHEN te.store_code IN ('es','business_es') THEN 'Spain'
				WHEN te.store_code IN ('at','business_at') THEN 'Austria'
			END) AS country_name, 
		order_id,
		NULLIF(CASE WHEN is_valid_json(te.properties) THEN json_extract_path_text(te.properties, 'product_sku') END,'') AS main_product_sku
	FROM segment.track_events te 
	LEFT JOIN ods_production.store s 
		ON s.id = te.store_id
	WHERE event_name = 'Product Added to Cart'
)
, addon_list_35UP AS (
	SELECT 
		DISTINCT accessory_sku
	FROM dm_finance.addon_35up_tracking
	WHERE accessory_sku LIKE '%35UP%'
)
, drawer_opened_raw AS (
	SELECT 
		session_id,
		event_time,
		CASE
			WHEN te.store_code IN ('de','business') THEN 'Germany'
			WHEN te.store_code IN ('us','business_us') THEN 'United States'
			WHEN te.store_code IN ('nl','business_nl') THEN 'Netherlands'
			WHEN te.store_code IN ('es','business_es') THEN 'Spain'
			WHEN te.store_code IN ('at','business_at') THEN 'Austria'
		END AS country_name,
		order_id,
		NULLIF(CASE WHEN is_valid_json(te.properties) THEN json_extract_path_text(te.properties, 'main_product_sku') END,'') AS main_product_sku,
		NULLIF(CASE WHEN is_valid_json(te.properties) THEN json_extract_path_text(te.properties, 'skus') END,'') AS addon_sku,
		event_id
	FROM segment.track_events te 
	WHERE properties LIKE '%35UP%' 
		AND te.event_name = 'Accessory Drawer Opened'
)
, added_to_cart_to_drawer_opened_raw AS (
	SELECT 
		pa.session_id,
		pa.event_id AS product_added_event_id,
		pa.event_time AS product_added_event_time,
		COALESCE(pa.country_name,da.country_name) AS country_name,
		pa.order_id,
		pa.main_product_sku,
		da.event_id AS drawer_opened_event_id,
		da.event_time AS drawer_opened_event_time,
		da.addon_sku AS addon_sku_in_drawer,
		ROW_NUMBER () OVER (PARTITION BY pa.event_id ORDER BY da.event_time) AS rowno
	FROM product_added_to_cart pa
	LEFT JOIN drawer_opened_raw da 
		ON da.session_id = pa.session_id
			AND da.main_product_sku = pa.main_product_sku
			AND da.order_id = pa.order_id
			AND da.event_time >= pa.event_time - interval '3 seconds' --included INTERVAL to account for edge cases in which the follow up event is actually recorded few moments earlier  
)
--SELECT * FROM added_to_cart_to_drawer_opened_raw LIMIT 100;
, added_to_cart_to_drawer_opened AS (
	SELECT 
		da.session_id,
		da.product_added_event_id,
		da.product_added_event_time,
		da.country_name,
		da.order_id,
		da.main_product_sku,
		da.drawer_opened_event_id,
		da.drawer_opened_event_time,
		al.accessory_sku AS addon_sku
	FROM added_to_cart_to_drawer_opened_raw da
	LEFT JOIN addon_list_35UP al
		ON POSITION(al.accessory_sku IN da.addon_sku_in_drawer) > 0
	WHERE rowno = 1	
)
--SELECT * FROM added_to_cart_to_drawer_opened LIMIT 100;
, accessory_added AS (
	SELECT 
		session_id,
		event_time,
		CASE
			WHEN te.store_code IN ('de','business') THEN 'Germany'
			WHEN te.store_code IN ('us','business_us') THEN 'United States'
			WHEN te.store_code IN ('nl','business_nl') THEN 'Netherlands'
			WHEN te.store_code IN ('es','business_es') THEN 'Spain'
			WHEN te.store_code IN ('at','business_at') THEN 'Austria'
		END AS country_name,
		order_id,
		NULLIF(CASE WHEN is_valid_json(te.properties) THEN json_extract_path_text(te.properties, 'main_product_sku') END,'') AS main_product_sku,
		NULLIF(CASE WHEN is_valid_json(te.properties) THEN json_extract_path_text(te.properties, 'sku') END,'') AS addon_sku,
		event_id
	FROM segment.track_events te 
	WHERE properties LIKE '%35UP%' 
		AND te.event_name = 'Accessory Added'
)
, drawer_opened_to_accessory_added AS (
	SELECT 
		dw.*,
		ROW_NUMBER () OVER (PARTITION BY dw.drawer_opened_event_id,dw.addon_sku ORDER BY ad.event_time) AS row_no,	
		ROW_NUMBER () OVER (PARTITION BY ad.event_id ORDER BY dw.drawer_opened_event_time DESC) AS rowno,
		CASE WHEN rowno = 1 THEN ad.event_id END AS accessory_added_event_id,
		CASE WHEN rowno = 1 THEN ad.event_time END AS accessory_added_event_time,
		CASE WHEN rowno = 1 THEN ad.addon_sku END AS addon_sku_added,
		o.submitted_date
	FROM added_to_cart_to_drawer_opened dw
	LEFT JOIN accessory_added ad
		ON ad.session_id = dw.session_id 
			AND ad.main_product_sku = dw.main_product_sku
			AND ad.order_id = dw.order_id
			AND ad.addon_sku = dw.addon_sku
			AND ad.event_time >= dw.drawer_opened_event_time - interval '3 seconds' --same as above 
	LEFT JOIN master.ORDER o 
		ON dw.order_id = o.order_id
)
SELECT 
	date_trunc('week',a.product_added_event_time)::date AS reporting_week,
	p.category_name,
	p.subcategory_name,
	p.product_name, 
	a.main_product_sku,
	count(DISTINCT product_added_event_id) AS unique_product_added_to_cart,
	count(DISTINCT drawer_opened_event_id) AS unique_accessory_drawer_opened,
	count(DISTINCT accessory_added_event_id) AS unique_accessory_added,
	count(DISTINCT u.external_id) AS addon_sku_submitted_in_order,
	count(DISTINCT CASE WHEN u.submitted_date IS NOT NULL THEN a.order_id END) AS submitted_orders_with_35UP_accessory,
	count(DISTINCT CASE WHEN u.paid_date IS NOT NULL THEN a.order_id END) AS paid_orders_with_35UP_accessory,
	sum(CASE WHEN u.status = 'PAID' THEN addon_price END) AS addon_amount
FROM drawer_opened_to_accessory_added a
LEFT JOIN ods_production.product p 
	ON p.product_sku = a.main_product_sku
LEFT JOIN ods_production.addon_35up u     --this ensures that the order was actually submitted with the addon   
	ON u.order_id = a.order_id
		AND u.external_id = a.addon_sku
WHERE row_no = 1
GROUP BY 1,2,3,4,5
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_commercial.v_35UP_orders_funnel TO hightouch;
