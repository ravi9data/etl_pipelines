--Navi bar analysis
--Session with product_added: 1716200805621 (test)
--Session without product_added: 1716152584238 (test)
DROP TABLE IF EXISTS tmp_page_views_navigation_bar;
CREATE TEMP TABLE tmp_page_views_navigation_bar AS 
	SELECT 
		pv.traffic_source,
		pv.device_type, 
		pv.anonymous_id,
		pv.session_id,
		pv.page_view_id,
		pv.page_view_start,
		pv.page_type,
		pv.page_type_detail,
		pv.page_url,
		pv.page_urlpath
    FROM traffic.page_views pv
	WHERE pv.page_view_start::date >= current_date - 3
	  AND pv.page_type IN 
	  		(
			'category', 
			'sub-category',
			'subcategory',
			'brand', --added due LP page type, might need to hardcode more
			'pdp'
			)
	 --removed previous filters as it was filtering out the brand page_type 
;


DROP TABLE IF EXISTS tmp_navigation_bar_performance;
CREATE TEMP TABLE tmp_navigation_bar_performance AS
WITH navigation_clicked AS (
	SELECT 
		te.session_id,
		te.anonymous_id,
		te.event_id,
		te.event_time,
		st.country_name,
        CASE WHEN page_path LIKE '%business%' THEN 'B2B' ELSE 'B2C'END AS customer_type,
		te.device_type,
		s.is_paid,
		CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW'ELSE 'RECURRING' END AS new_recurring_visitor,
		initcap(NULLIF(json_extract_path_text(te.properties,'group'),'')) AS group_name, --probably not needed, FOR now TO filter
		NULLIF(json_extract_path_text(te.properties,'category'),'') AS category_name_navigation_bar,
		NULLIF(json_extract_path_text(te.properties,'sub_category'),'') AS subcategory_name_navigation_bar,
		NULLIF(json_extract_path_text(te.properties,'brand'),'') AS brand_navigation_bar,
		NULLIF(json_extract_path_text(te.properties,'trending'),'') AS trending_navigation_bar,
		ROW_NUMBER () OVER (PARTITION BY te.event_id ORDER BY nb.page_view_start) AS rowno, --linking only page_views after the navigation bar click   
		ROW_NUMBER () OVER (PARTITION BY nb.page_view_id ORDER BY te.event_time DESC) AS row_no, --linking the following page_views only to most recent/closest click 
		CASE WHEN row_no = 1 THEN nb.page_view_id END AS page_view_id,
		CASE WHEN row_no = 1 THEN nb.page_view_start END AS page_view_start,
		CASE WHEN row_no = 1 THEN nb.page_type END AS page_type,
		CASE WHEN row_no = 1 THEN nb.page_type_detail END AS page_type_detail
	FROM segment.track_events te 
	LEFT JOIN ods_production.store st 
		ON st.id = te.store_id
	LEFT JOIN traffic.sessions s
	  ON s.session_id = te.session_id
	LEFT JOIN traffic.snowplow_user_mapping cu
		ON cu.anonymous_id = te.anonymous_id
			AND cu.session_id = te.session_id
	LEFT JOIN tmp_page_views_navigation_bar nb 
		ON nb.session_id = te.session_id
			AND nb.page_view_start >= te.event_time - interval '5 seconds' --some sessions have navigation clicked as follow up event 
	WHERE event_name = 'Navigation Clicked'
		AND event_time >= current_date - 3
		AND group_name NOT IN (
								'Account','Deals','Wishlist','How-It-Works','Trending','Hamburger-Menu','My-Tech','Brands'
							  )			  
		--AND nb.page_type <> 'pdp'
)
--SELECT 
--	nc.*
--FROM navigation_clicked nc
--WHERE rowno = 1;
, pdp_attribution_navigation_bar_raw AS ( 
	SELECT 
		nc.session_id,
		nc.anonymous_id,
		nc.event_id,
		nc.event_time,
		nc.country_name,
		nc.customer_type,
		nc.device_type,
		nc.group_name,
		initcap(nc.category_name_navigation_bar) AS category_name_navigation_bar,
		initcap(nc.subcategory_name_navigation_bar) AS subcategory_name_navigation_bar,
		initcap(nc.brand_navigation_bar) AS brand_navigation_bar,
		initcap(nc.trending_navigation_bar) AS trending_navigation_bar,
		nc.page_view_id,
		nc.page_view_start,
		nc.page_type,
		nc.is_paid,
		nc.new_recurring_visitor,
		ROW_NUMBER () OVER (PARTITION BY COALESCE(nc.page_view_id,'a'),nc.event_id ORDER BY nb.page_view_start) AS rowno_pdp, --want to keep all navigation events so added to partition  
		ROW_NUMBER () OVER (PARTITION BY nb.page_view_id ORDER BY nc.page_view_start DESC) AS rowno_attribution,
		CASE
			WHEN nc.page_type = 'pdp' THEN nc.page_view_start	
			WHEN rowno_attribution = 1 THEN nb.page_view_start 
		END AS pdp_event_start,
		CASE
			WHEN nc.page_type = 'pdp' THEN nc.page_view_id	
			WHEN rowno_attribution = 1 THEN nb.page_view_id 
		END AS pdp_event_id,
		CASE
			WHEN nc.page_type = 'pdp' THEN nc.page_type_detail	
			WHEN rowno_attribution = 1 THEN nb.page_type_detail
		END AS product_sku
	FROM navigation_clicked nc
	LEFT JOIN tmp_page_views_navigation_bar nb
		ON nc.session_id = nb.session_id
			AND nb.page_view_start >= nc.page_view_start
			AND nb.page_type = 'pdp'
			AND nc.page_type <> 'pdp' --avoiding pdp AFTER pdp		
	WHERE nc.rowno = 1
)
, pdp_attribution_navigation_bar AS (
	SELECT 	
		r.session_id,
		r.anonymous_id,
		r.event_id,
		r.event_time,
		r.country_name,
		r.customer_type,
		r.device_type,
		r.group_name,
		r.category_name_navigation_bar,
		r.subcategory_name_navigation_bar,
		r.brand_navigation_bar,
		r.trending_navigation_bar,
		r.page_view_id,
		r.page_view_start,
		r.page_type,
		r.is_paid,
		r.new_recurring_visitor,
		CASE 
			WHEN trim(lower(replace(r.category_name_navigation_bar,'-',''))) <> trim(lower(replace(replace(p.category_name,' & ','and'),' ','')))
				THEN NULL 
			ELSE r.pdp_event_start	
		END AS pdp_event_start,
		CASE 
			WHEN trim(lower(replace(r.category_name_navigation_bar,'-',''))) <> trim(lower(replace(replace(p.category_name,' & ','and'),' ','')))
				THEN NULL 
			ELSE r.pdp_event_id	
		END AS pdp_event_id,
		CASE 
			WHEN trim(lower(replace(r.category_name_navigation_bar,'-',''))) <> trim(lower(replace(replace(p.category_name,' & ','and'),' ','')))
				THEN NULL 
			ELSE r.product_sku	
		END AS product_sku,
		CASE 
			WHEN trim(lower(replace(r.category_name_navigation_bar,'-',''))) <> trim(lower(replace(replace(p.category_name,' & ','and'),' ','')))
				THEN NULL 
			ELSE p.category_name	
		END AS category_name,
		CASE 
			WHEN trim(lower(replace(r.category_name_navigation_bar,'-',''))) <> trim(lower(replace(replace(p.category_name,' & ','and'),' ','')))
				THEN NULL 
			ELSE p.subcategory_name	
		END AS subcategory_name,
		CASE 
			WHEN trim(lower(replace(r.category_name_navigation_bar,'-',''))) <> trim(lower(replace(replace(p.category_name,' & ','and'),' ','')))
				THEN NULL 
			ELSE p.brand	
		END AS brand,
		CASE 
			WHEN trim(lower(replace(r.category_name_navigation_bar,'-',''))) <> trim(lower(replace(replace(p.category_name,' & ','and'),' ','')))
				THEN NULL 
			ELSE p.product_name	
		END AS product_name
	FROM pdp_attribution_navigation_bar_raw r
	LEFT JOIN ods_production.product p
		ON p.product_sku = r.product_sku
	WHERE r.rowno_pdp = 1
)
, product_added_to_cart AS (
	SELECT 
		te.session_id,
		te.anonymous_id,
		te.event_id AS product_added_event_id, 
		te.event_time AS product_added_event_time,
		CASE WHEN is_valid_json(te.properties) THEN NULLIF(json_extract_path_text(properties,'product_sku'),'')  END AS product_sku,
		te.order_id,
		o.created_date,
		o.submitted_date,
		o.paid_date,
		o.new_recurring AS new_recurring_order
	FROM segment.track_events te
	LEFT JOIN master."order" o 
		ON o.order_id = te.order_id 
	WHERE event_time::date >= current_date - 3
		AND event_name IN ('Product Added','Product Added to Cart')
)
SELECT 
	nb.*,
	ROW_NUMBER () OVER (PARTITION BY nb.pdp_event_id ORDER BY pa.product_added_event_time) AS rowno_pa,
	CASE WHEN rowno_pa = 1 THEN pa.order_id END AS order_id,
	CASE WHEN rowno_pa = 1 THEN pa.product_added_event_id END AS product_added_event_id, 
	CASE WHEN rowno_pa = 1 THEN pa.product_added_event_time END AS product_added_event_time,
	CASE WHEN rowno_pa = 1 THEN pa.created_date END AS created_date,
	CASE WHEN rowno_pa = 1 THEN pa.submitted_date END AS submitted_date,
	CASE WHEN rowno_pa = 1 THEN pa.paid_date END AS paid_date,
	CASE WHEN rowno_pa = 1 THEN pa.new_recurring_order END AS new_recurring_order
FROM pdp_attribution_navigation_bar nb
LEFT JOIN product_added_to_cart pa
	ON pa.session_id = nb.session_id	--currently hard join, we might need to switch with anonymous_id: we built this under the assumption of customers creating the order within the same session, 
										--but instead it could happen with some days of "delay", which means we would not capture such product addition. If we consider the same anonymous_id and we 
										--remove the session join to the temp table, but we link it to the final dm table, we could link the product additions happening few days later (to be discussed)
										--which are coming from the same customer generating a new pdp on the same product he/she first clicked on the navi bar
		AND pa.product_sku = nb.product_sku
		AND pa.product_added_event_time >= nb.pdp_event_start
;


BEGIN TRANSACTION;

DELETE FROM dm_commercial.navigation_bar_performance
    USING tmp_navigation_bar_performance tmp
WHERE navigation_bar_performance.event_id = tmp.event_id;

DELETE FROM dm_commercial.navigation_bar_performance
WHERE event_time::date < DATEADD('month', -6,date_trunc('month',current_date))
;

INSERT INTO dm_commercial.navigation_bar_performance
SELECT *
FROM tmp_navigation_bar_performance;

END TRANSACTION;

DROP TABLE IF EXISTS tmp_navigation_bar_performance;

DROP TABLE IF EXISTS tmp_page_views_navigation_bar;

GRANT SELECT ON dm_commercial.navigation_bar_performance TO tableau;
