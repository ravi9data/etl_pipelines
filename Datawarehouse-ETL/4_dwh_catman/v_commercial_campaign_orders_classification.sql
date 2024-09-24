CREATE OR REPLACE VIEW dm_commercial.v_commercial_campaign_orders_classification AS 
WITH prep AS (
	SELECT DISTINCT 
		order_id,
		product_sku,
		product_viewed_time_start,
		user_id,
		anonymous_id,
		campaign_title
	FROM dm_commercial.commercial_campaigns_tracking
	WHERE order_id IS NOT NULL 
)		
, orders_prep AS (
	SELECT  
		o.order_id,
		o.created_date::date AS order_created_date, 
		CASE 
			WHEN cct.order_id IS NOT NULL THEN 1 ELSE 0
				END is_order_direct_from_campaign,
		FIRST_VALUE(cct.campaign_title) IGNORE NULLS OVER
           		 (PARTITION BY o.order_id ORDER BY cct.product_viewed_time_start DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
           		 AS campaign_title_direct_from_campaign,
        FIRST_VALUE(cct2.campaign_title) IGNORE NULLS OVER
           		 (PARTITION BY o.order_id ORDER BY cct2.product_viewed_time_start DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
           		 AS campaign_title_influenced_by_campaign,      		 
		MAX(CASE WHEN cct2.order_id IS NOT NULL THEN 1 ELSE 0 END) 
           OVER (PARTITION BY o.order_id) AS is_order_influenced_by_campaign
	FROM master."order" o 
	LEFT JOIN prep cct 
		ON o.order_id = cct.order_id 
	LEFT JOIN ods_production.order_item oi
		ON cct.order_id = oi.order_id
	LEFT JOIN segment.customer_mapping_web cw
		ON cw.customer_id = o.customer_id
	LEFT JOIN prep cct2 
		ON (o.customer_id = cct2.user_id OR cw.anonymous_id = cct2.anonymous_id)
		AND cct2.product_sku = oi.product_sku 
		AND o.submitted_date >= cct2.product_viewed_time_start  
		AND DATEADD('day',-7,o.created_date::timestamp) <= cct2.product_viewed_time_start 
	WHERE o.created_date >= '2023-05-01'
		AND o.completed_orders > 0
	GROUP BY 1,2,3,cct.product_viewed_time_start, cct.campaign_title, cct2.product_viewed_time_start, cct2.campaign_title, cct2.order_id
)
SELECT 
	order_created_date,
	is_order_direct_from_campaign,
	is_order_influenced_by_campaign,
	campaign_title_direct_from_campaign,
	campaign_title_influenced_by_campaign,
	CASE WHEN is_order_direct_from_campaign = 0 
			AND is_order_influenced_by_campaign = 0 
			THEN 1 ELSE 0 END AS is_order_not_influenced,
	count(DISTINCT order_id) AS orders
FROM orders_prep
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA BINDING;