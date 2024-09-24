DROP TABLE IF EXISTS dm_product.conversion_bundle_report;
CREATE TABLE dm_product.conversion_bundle_report AS
WITH prep AS (
	SELECT
	event_time,
	session_id,
	JSON_EXTRACT_PATH_text(properties,'product_sku') AS product_sku,
	json_extract_path_text(properties, 'store_id') AS store_id,
	json_extract_path_text(properties, 'bundleID') AS bundle_id,
	event_name,
	event_id AS message_id,
	CASE WHEN page_path ILIKE '/business%' THEN 'B2B' ELSE 'B2C' END AS customer_type
FROM segment.track_events
WHERE event_name IN ('Product Bundle Added To Cart', 'Bundle Impression', 'Product Viewed')
		AND context NOT ILIKE '%datadog%'
		AND event_time >=  DATE_TRUNC('month',dateadd('month',-2,current_date))
)
, product_viewed_lag_data AS (
	SELECT 
		event_time AS event_time_product_viewed_start,
		product_sku,
		session_id,
		LAG(event_time) OVER (PARTITION BY session_id, product_sku ORDER BY event_time DESC) AS event_time_product_viewed_end
	FROM prep 
	WHERE event_name = 'Product Viewed'	
)
, filter_bundle_impression AS (
	SELECT 
		event_time_product_viewed_start,
		p.product_sku,
		p.session_id,
		p.event_time AS event_time_bundle_impression,
		p.message_id AS message_id_bundle_impression,
		p.store_id,
		p.bundle_id,
		p.customer_type,
		ROW_NUMBER() OVER (PARTITION BY pv.session_id, pv.product_sku, event_time_product_viewed_start 
			ORDER BY event_time_bundle_impression) AS row_num
	FROM product_viewed_lag_data pv
	LEFT JOIN prep p
		ON pv.session_id = p.session_id 
		AND p.event_time BETWEEN pv.event_time_product_viewed_start AND COALESCE(pv.event_time_product_viewed_end, '9999-01-01')
		AND p.product_sku = pv.product_sku
		AND p.event_name = 'Bundle Impression'
	WHERE p.session_id IS NOT NULL  -- TO ONLY have sessions that have Bundles
)
, bundle_end AS (
	SELECT 
		event_time_product_viewed_start,
		product_sku,
		session_id,
		event_time_bundle_impression,
		LAG(event_time_bundle_impression) OVER (PARTITION BY session_id, product_sku 
			ORDER BY event_time_bundle_impression DESC) AS event_time_bundle_impression_end,
		message_id_bundle_impression,
		store_id,
		bundle_id,
		customer_type
	FROM filter_bundle_impression
	WHERE row_num = 1 -- we have multiple events Bundle Impression per Product Viewed. 
		--So, IN ORDER TO NOT inflate this number, we are only taking one per Product Viewed
)
, secondary_skus AS (
	SELECT      
		b.id,
	    json_serialize(b.secondaryskus) AS secondary_skus,
	    b.title
	FROM staging_airbyte_bi.bundle b
)
SELECT 
	be.event_time_bundle_impression::date,
	s.country_name,
	be.product_sku,
	p2.product_name,
	p2.subcategory_name ,
	p2.category_name ,
	be.bundle_id,
	REPLACE(REPLACE(REPLACE(b.secondary_skus,'"',''),'[',''),']','') AS secondary_skus,
	b.title,
	be.store_id,
	be.customer_type,
	count(DISTINCT be.session_id) AS session_id,
	count(DISTINCT be.message_id_bundle_impression) AS message_id_bundle_impression,
	count(DISTINCT p.message_id) AS message_id_bundle_add_to_cart
FROM bundle_end be
LEFT JOIN prep p 
	ON be.session_id = p.session_id
	AND be.product_sku = p.product_sku
	AND be.bundle_id = p.bundle_id
	AND p.event_name = 'Product Bundle Added To Cart'
	AND p.event_time BETWEEN be.event_time_product_viewed_start 
				AND COALESCE(be.event_time_bundle_impression_end, '9999-01-01')
LEFT JOIN ods_production.store s  
	ON be.store_id = s.id
LEFT JOIN ods_production.product p2 
	ON p2.product_sku = be.product_sku
LEFT JOIN secondary_skus b
	ON b.id = be.bundle_id
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
;

GRANT SELECT ON dm_product.conversion_bundle_report TO tableau;
