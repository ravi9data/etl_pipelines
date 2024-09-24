DROP TABLE IF EXISTS tmp_page_views;
CREATE TEMP TABLE tmp_page_views AS 
	SELECT 
		pv.traffic_source,
		pv.device_type, 
		pv.anonymous_id,
		pv.session_id,
		pv.page_view_id,
		pv.page_view_start,
		pv.page_type,
		pv.page_type_detail,
		CASE 
			WHEN pv.page_type = 'category' THEN cs.category_name
			WHEN pv.page_type IN ('sub-category','subcategory') THEN cs.subcategory_name
			ELSE page_type_detail 
		END AS category_subcategory_page_type_name,	
		pv.page_url,
		pv.page_urlpath,
		split_part(pv.page_urlpath,'/',2) AS url_split,
		REGEXP_REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(url_split,'-nl',''),'-es',''),'-en',''),'-de',''),'-','_'),'_.[0-9]{3}') AS parsed_store_name,
		CASE
            WHEN parsed_store_name IN ('at','at_','at_at','at_lp','business_at') THEN 'Austria'
            WHEN parsed_store_name IN ('de', 'de_','de_lp','business','business_en','business_de') THEN 'Germany'
            WHEN parsed_store_name IN ('nl','nl_','nl_lp','business_nl') THEN 'Netherlands'
            WHEN parsed_store_name IN ('es','es_','es_lp','business_es') THEN 'Spain'
            WHEN parsed_store_name IN ('us','us_', 'us_us','us_lp','business_us') THEN 'United_States'
            WHEN pv.store_id = '-1' THEN 'Germany'
            ELSE st.country_name 
		END AS store_country,
        CASE 
        	WHEN parsed_store_name LIKE '%business%' THEN 'B2B' 
        	ELSE 'B2C'
        END AS customer_type,
        s.is_paid,
        CASE 
	        WHEN cu.is_new_visitor IS TRUE 
	        	THEN 'NEW'
	        ELSE 'RECURRING' 
	    END AS new_recurring
    FROM traffic.page_views pv	
	LEFT JOIN traffic.sessions s
	  ON pv.session_id = s.session_id
	LEFT JOIN ods_production.store st
		ON st.id = pv.store_id
	LEFT JOIN traffic.snowplow_user_mapping cu
		ON pv.anonymous_id = cu.anonymous_id
			AND pv.session_id = cu.session_id
	LEFT JOIN staging_airbyte_bi.traffic_list_cat_subcat cs
		ON pv.page_type_detail = cs.traffic_list		
	WHERE pv.page_view_start::date >= current_date -3
	  AND pv.page_type IN (
			'category',
			'sub-category',
			'subcategory',
			'search',
			'home',
			'trending',
			'deals',
			'pdp'
			)
	 AND page_type_detail NOT IN ('pos','POS','Pos')
	 AND page_type_detail IS NOT NULL --missing info AND cannot be derived from page_url_path
;

	
DROP TABLE IF EXISTS tmp_product_added_removed;
CREATE TEMP TABLE tmp_product_added_removed AS 
WITH product_added_removed_ AS (
	SELECT 
		session_id,
		event_id, 
		event_time,
		CASE WHEN is_valid_json(properties) THEN NULLIF(json_extract_path_text(properties,'product_sku'),'')  END AS product_sku,
		order_id,
		event_name
	FROM segment.track_events te
	WHERE event_time::date >= current_date -3
	  AND event_name IN ('Product Added','Product Added to Cart', 'Removed From Cart')
)
SELECT 
	r.session_id,
	r.event_id,
	r.event_time,
	COALESCE(p.product_sku,r.product_sku) AS product_sku,
	r.order_id,
	r.event_name
FROM product_added_removed_ r
LEFT JOIN ods_production.variant v 
		ON v.variant_id = r.product_sku
	LEFT JOIN ods_production.product p 
		ON p.product_id = v.product_id
;

DROP TABLE IF EXISTS tmp_merchandising_kpi_report;
CREATE TEMP TABLE tmp_merchandising_kpi_report AS
WITH traffic_source AS (
	SELECT 
		ts.traffic_source,
		ts.device_type,
		ts.anonymous_id,
		ts.session_id,
		ts.page_view_id,
		ts.page_view_start,
		ts.store_country,
		ts.customer_type,
		ts.page_type,
		ts.page_type_detail,
		ts.category_subcategory_page_type_name,
		ts.page_url,
		ts.is_paid,
		ts.new_recurring,
		ROW_NUMBER() OVER (PARTITION BY ts.page_view_id ORDER BY pv.page_view_start) AS rowno_pdp, --only 1st pdp after the pageview (low conversion but no referrer)
		pv.page_view_start AS pdp_event_time,
		pv.page_view_id AS pdp_event_id,
		pv.page_type_detail AS product_sku
	FROM tmp_page_views ts
	LEFT JOIN tmp_page_views pv
		ON pv.session_id = ts.session_id
		AND pv.page_type = 'pdp'
		AND pv.page_view_start > ts.page_view_start
	WHERE ts.page_type <> 'pdp'
)
, pdp_attribution AS (
	SELECT 
		traffic_source,
		device_type,
		anonymous_id,
		session_id,
		page_view_id,
		page_view_start,
		store_country,
		customer_type,
		page_type,
		page_type_detail,
		category_subcategory_page_type_name,
		page_url,
		is_paid,
		new_recurring,
		ROW_NUMBER() OVER (PARTITION BY pdp_event_id ORDER BY page_view_start DESC) AS rowno_pdp2,
		CASE WHEN rowno_pdp2 = 1 THEN pdp_event_id END AS pdp_event_id,
		CASE WHEN rowno_pdp2 = 1 THEN pdp_event_time END AS pdp_event_time,
		CASE WHEN rowno_pdp2 = 1 THEN product_sku END AS product_sku
	FROM traffic_source
	WHERE rowno_pdp = 1 
)
, product_added AS ( 
	SELECT  
		te.session_id,
		te.event_id, 
		te.event_time AS product_added_event_time,
        te.product_sku,
		te.order_id AS order_id,
		o.created_date,
		o.submitted_date,
		o.paid_date,
		o.new_recurring AS new_recurring_order, 
		count(*) OVER (PARTITION BY te.order_id) AS product_added_to_cart
	FROM tmp_product_added_removed te 
	LEFT JOIN master."order" o 
		ON o.order_id = te.order_id 	
	WHERE event_name IN ('Product Added','Product Added to Cart')
)
, prep2 AS (
	SELECT 
		pdp.traffic_source,
		pdp.device_type,
		pdp.anonymous_id,
		pdp.session_id,
		pdp.page_view_id,
		pdp.page_view_start,
		pdp.store_country,
		pdp.customer_type,
		pdp.page_type,
		pdp.category_subcategory_page_type_name,
		pdp.is_paid,
		pdp.new_recurring AS new_recurring_traffic,
		pdp.pdp_event_id,
		pdp.pdp_event_time,
		p.category_name,
		p.subcategory_name,
		p.brand,
		pdp.product_sku,
		p.product_name,
		pa.order_id,
		pa.created_date,
		pa.submitted_date,
		pa.paid_date,
		pa.new_recurring_order,
		pa.product_added_to_cart,
		ROW_NUMBER() OVER (PARTITION BY pdp.page_view_id ORDER BY pa.product_added_event_time) AS rowno_product_added
	FROM pdp_attribution pdp
	LEFT JOIN ods_production.product p
		ON p.product_sku = pdp.product_sku
	LEFT JOIN product_added pa 
		ON pa.session_id = pdp.session_id
		 AND pa.product_sku = pdp.product_sku
		 AND pa.product_added_event_time > pdp.pdp_event_time
)
, prep AS (
	SELECT p.*
	FROM prep2 p
	WHERE rowno_product_added = 1
)
, cart_info AS ( -- subject TO CHANGE due TO same order_id FOR multiple page VIEW ids 
	SELECT 
		oi.order_id,
		sum(quantity) AS order_products,
		sum(total_price) AS order_value
	FROM ods_production.order_item oi
	WHERE EXISTS (SELECT NULL FROM prep p WHERE oi.order_id = p.order_id)
	GROUP BY 1
)
, product_removed_from_cart AS (
	SELECT 
		te.order_id, 
		count(DISTINCT te.event_id) AS products_removed_from_cart
	FROM tmp_product_added_removed te
	WHERE event_name = 'Removed From Cart'
		AND EXISTS (SELECT NULL FROM prep p WHERE te.order_id = p.order_id)
	GROUP BY 1
)
SELECT 
	p.anonymous_id,
	p.session_id,
	p.page_view_id,
	p.page_view_start,
	p.store_country,
	p.customer_type,
	p.page_type,
	p.is_paid,
	p.category_name,
	p.subcategory_name,
	p.brand,
	p.product_sku,
	p.product_name,
	p.pdp_event_id,
	p.pdp_event_time,
	p.order_id,
	p.created_date,
	p.submitted_date,
	p.paid_date,
	p.product_added_to_cart,
	r.products_removed_from_cart,
	count(p.page_view_id) OVER (PARTITION BY p.order_id) AS page_views_per_order,
	sum(c.order_value)::float/page_views_per_order::float AS avg_cart_value_base,
	sum(c.order_products)::float/page_views_per_order::float AS avg_cart_size_base,
	CASE 
		WHEN p.product_added_to_cart = r.products_removed_from_cart
			THEN TRUE 
		ELSE FALSE 
	END AS is_abandoned,
	p.new_recurring_traffic,
	p.new_recurring_order,
	p.category_subcategory_page_type_name,
	p.traffic_source,
	p.device_type
FROM prep p
LEFT JOIN product_removed_from_cart r
	ON r.order_id = p.order_id
LEFT JOIN cart_info c
	ON c.order_id = p.order_id
LEFT JOIN master."order" o
	ON o.order_id = p.order_id
		AND datediff('day',o.created_date::timestamp,o.updated_date::timestamp) > 3
		AND o.submitted_date IS NULL 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,12,13,14,15,16,17,18,19,20,21,25,26,27,28,29,30
;

BEGIN TRANSACTION;

DELETE FROM dm_commercial.merchandising_kpi_report_source_table
    USING tmp_merchandising_kpi_report tmp
WHERE merchandising_kpi_report_source_table.page_view_id = tmp.page_view_id;

DELETE FROM dm_commercial.merchandising_kpi_report_source_table
WHERE (traffic_source IS NULL AND page_view_start <= current_date - 14) OR (page_view_start::date < DATEADD('month', -6,date_trunc('month',current_date)))
;

INSERT INTO dm_commercial.merchandising_kpi_report_source_table
SELECT *
FROM tmp_merchandising_kpi_report;

END TRANSACTION;

DROP TABLE IF EXISTS tmp_merchandising_kpi_report;

DROP TABLE IF EXISTS tmp_page_views;

DROP TABLE IF EXISTS tmp_product_added_removed;

GRANT SELECT ON dm_commercial.merchandising_kpi_report_source_table TO tableau;
