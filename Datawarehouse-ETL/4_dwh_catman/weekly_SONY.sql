DROP VIEW IF EXISTS dm_catman.v_weekly_sony;
CREATE VIEW dm_catman.v_weekly_sony AS
WITH page_view AS (
	SELECT
		 page_view_date::date
		,page_type_detail AS pv_product_sku
		,s.marketing_channel 
		,COUNT(DISTINCT page_view_id ) AS product_page_views
		,COUNT(DISTINCT pv.session_id) AS sessions_with_product_page_views
	FROM traffic.page_views pv 
	LEFT JOIN traffic.sessions s
		   	ON pv.session_id = s.session_id 
	WHERE page_view_date BETWEEN DATEADD('month', -1, current_date) AND DATEADD('day', -1, current_date)
		AND page_type ='pdp'
	GROUP BY 1,2,3
)
,orders AS (
	SELECT 
		 o.created_date::date
		,oi.product_sku
		,o.marketing_channel 
		,COUNT(DISTINCT oi.order_id) AS carts
		,COUNT(DISTINCT CASE WHEN o.completed_orders >= 1 THEN o.order_id END) AS sub_orders
		,count(DISTINCT s.subscription_id) AS num_subscriptions
	FROM master."order" o 
	INNER JOIN ods_production.order_item oi 	
		ON o.order_id = oi.order_id
	LEFT JOIN master.subscription s 
		ON oi.product_sku = s.product_sku
		AND o.order_id = s.order_id
	WHERE o.created_date BETWEEN DATEADD('month', -1, current_date) AND DATEADD('day', -1, current_date)
	GROUP BY 1,2,3
)	
SELECT
	 COALESCE(pj.page_view_date::date, oj.created_date::date) AS page_view_date
	,COALESCE(pj.pv_product_sku, oj.product_sku) AS product_sku
	,pp.product_name
	,pp.brand
	,COALESCE(pj.marketing_channel, oj.marketing_channel) AS marketing_channel
	,COALESCE(pj.product_page_views,0) AS product_page_views
	,COALESCE(pj.sessions_with_product_page_views,0) AS sessions_with_product_page_views
	,COALESCE(oj.carts,0) AS carts
	,COALESCE(oj.sub_orders,0) AS sub_orders
	,COALESCE(oj.num_subscriptions,0) AS num_subscriptions
FROM page_view pj
FULL JOIN orders oj
	ON pj.page_view_date::date = oj.created_date::date
	AND pj.pv_product_sku = oj.product_sku
	AND pj.marketing_channel = oj.marketing_channel
LEFT JOIN ods_production.product pp 
	ON COALESCE(pj.pv_product_sku, oj.product_sku) = pp.product_sku 
WITH NO SCHEMA BINDING;	
		
GRANT SELECT ON dm_catman.v_weekly_sony TO matillion;
	


