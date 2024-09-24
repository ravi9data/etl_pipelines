DROP TABLE IF EXISTS dm_product.conversion_add_on_report;
CREATE TABLE dm_product.conversion_add_on_report AS
WITH add_on_data AS (
	SELECT
		event_time,
		json_extract_path_text(properties, 'order_id') AS order_id,
		COALESCE(NULLIF(json_extract_path_text(properties, 'product_sku'), ''),
				NULLIF(json_extract_path_text(properties, 'sku'), ''),
				NULLIF(json_extract_path_text(properties, 'main_product_sku'), '')
				) AS product_sku,
		event_name,
		session_id,
		event_id AS message_id,
		CASE WHEN page_path ILIKE '/business%' THEN 'B2B' ELSE 'B2C' END AS customer_type
	FROM segment.track_events
	WHERE event_type='track'
	  AND event_name IN ('Add On Product Added', 'Add On Drawer Shown', 'Add On Product Viewed',
	  						'Add-On Added', 'Add-On Viewed', 'Add-Ons Drawer Opened')
	  AND context NOT ILIKE '%datadog%'
	  AND event_time >=  DATE_TRUNC('month',dateadd('month',-2,current_date))	
)
, ds_add_on_data AS (
	SELECT 
		event_time,
		order_id,
		product_sku,
		message_id,
		session_id,
		LAG(event_time) OVER (PARTITION BY order_id ORDER BY event_time DESC) AS event_time_end
	FROM add_on_data
	WHERE event_name IN ('Add On Drawer Shown', 'Add-Ons Drawer Opened')
	  AND order_id IS NOT NULL
)
, pd_add_on_data AS (
	SELECT 
		ds.event_time AS event_time_drawer_shown,
		ds.order_id,
		ds.session_id,
		ds.product_sku AS product_sku_drawer_shown,
		ds.message_id AS message_id_drawer_shown,
		ad.event_time AS event_time_product_added,
		ad.product_sku AS product_sku_product_added,
		ad.message_id AS message_id_product_added,
		vp.event_time AS event_time_drawer_viewed,
		vp.product_sku AS product_sku_drawer_viewed,
		vp.message_id AS message_id_drawer_viewed,
		ROW_NUMBER() OVER (PARTITION BY ds.order_id, ds.event_time,ds.product_sku,ds.message_id,vp.product_sku
			ORDER BY vp.event_time) AS row_num -- we can have multiple 'Add On Product Viewed' for the same Add ON
					-- if the customer scroll down the page and up. So, in order to take just one
	FROM ds_add_on_data ds
	LEFT JOIN add_on_data vp 
		ON vp.order_id = ds.order_id 
		AND vp.event_time BETWEEN ds.event_time AND COALESCE(ds.event_time_end, '9999-01-01')
		AND vp.event_name IN ('Add On Product Viewed', 'Add-On Viewed')
		AND ds.product_sku <> vp.product_sku
	LEFT JOIN add_on_data ad
		ON ds.order_id = ad.order_id
		AND ad.event_time BETWEEN ds.event_time AND COALESCE(ds.event_time_end, '9999-01-01')
		AND ad.event_name IN ('Add On Product Added', 'Add-On Added')
		AND ad.product_sku = vp.product_sku
)
, add_to_cart_data AS (	
	SELECT 
		te.event_time AS event_time_add_to_cart,
		LAG(te.event_time) OVER (PARTITION BY order_id, product_sku ORDER BY te.event_time DESC) AS event_time_add_to_cart_end,
		te.event_id,
		COALESCE(te.device_type, 'Other') AS device_type,
		sw.os,
		sw.browser,
		te.order_id,
		json_extract_path_text(te.properties ,'product_sku') AS product_sku,
		json_extract_path_text(te.properties,'variant') AS product_variant,
		e.country_name,
		CASE WHEN page_path ILIKE '/business%' THEN 'B2B' ELSE 'B2C' END AS customer_type,
		te.session_id 
	FROM segment.track_events te --segment data
	LEFT JOIN segment.sessions_web sw 
		ON sw.session_id = te.session_id
	LEFT JOIN ods_production.store e
		ON sw.store_id = e.id
	WHERE te.event_name IN ('Product Added', 'Product Added to Cart')
		AND te.event_time::date >= DATE_TRUNC('month',dateadd('month',-2,current_date))
		AND e.country_name IS NOT NULL	
)
SELECT DISTINCT
	s.event_time_add_to_cart::date,
	s.device_type,
    s.os,
    s.browser,
    s.country_name,
    s.product_sku,
    s.customer_type,
    p.product_name AS product_name_add_to_cart,
    p.category_name AS category_name_add_to_cart,
    p.subcategory_name  AS subcategory_name_add_to_cart,
    pd.product_sku_product_added,
    pd.product_sku_drawer_viewed,
    pp.product_name AS product_name_drawer_viewed,
    pd.message_id_drawer_shown,
    s.order_id,
    s.event_id AS event_id_add_to_cart,
    pd.message_id_product_added AS message_id_product_added,
    pd.message_id_drawer_viewed AS message_id_drawer_viewed
FROM add_to_cart_data  s
LEFT JOIN pd_add_on_data pd
	ON s.session_id = pd.session_id
	AND s.product_sku = pd.product_sku_drawer_shown
	AND pd.event_time_drawer_shown BETWEEN s.event_time_add_to_cart AND COALESCE(s.event_time_add_to_cart_end, '9999-01-01')	
	AND pd.row_num = 1	
LEFT JOIN ods_production.product p  
	ON s.product_sku = p.product_sku  
LEFT JOIN ods_production.product pp 
	ON pp.product_sku = pd.product_sku_drawer_viewed
WHERE p.category_name IS NOT NULL 
;

GRANT SELECT ON dm_product.conversion_add_on_report TO tableau;
