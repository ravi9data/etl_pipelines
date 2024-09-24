CREATE OR REPLACE VIEW  dm_finance.v_grove_care_order_conversion AS
WITH a AS (
SELECT 
	gc.*,
FROM 
WHERE 
	gc.country in ('Austria','United States','Germany')
	AND gc.order_submitted_date < current_Date
)
,
b AS (
SELECT 	
		o.submitted_date,
		o.new_recurring, 
		CASE 
			WHEN a.order_id IS NULL THEN 'disabled'
			ELSE enabled_flag
		END AS enabled_flag,
		CASE
			WHEN oc.os ilike 'web' THEN 'web'
			WHEN oc.os in ('android','ios') THEN 'app'
			WHEN o.device = 'n/a' THEN 'app'
			ELSE NULL
		END AS app_web,
		o.order_id,
		o.paid_orders,
		o.store_country
FROM 
	master.order o
LEFT JOIN traffic.order_conversions oc  
    ON o.order_id = oc.order_id
LEFT JOIN 
	a
	ON a.order_id = o.order_id
LEFT JOIN 
	master.subscription s 
	ON o.order_id = s.order_id 
WHERE 
	o.submitted_date >= '2023-09-15'
	AND o.store_country in ('Austria','United States','Germany')
	AND o.submitted_date < current_Date
)
,
c AS (
SELECT 
		submitted_date::date,
		new_recurring,
		enabled_flag,
		store_country,
		app_web,
		COUNT(DISTINCT CASE WHEN submitted_date IS NULL THEN NULL ELSE order_id  END )AS orders_submitted,
		COUNT(DISTINCT CASE WHEN paid_orders >= 1 THEN order_id END) AS paid_orders
FROM 
		b
WHERE 
		NOT (submitted_date < '2023-11-06' AND enabled_flag = 'disabled' AND app_web = 'app')
GROUP BY 
		1,2,3,4,5
)
,
pdp AS 
(
SELECT
    created_date,
    country,
    new_recurring,
    app_web,
    traffic_daily_unique_sessions,
    viewed,
    cart_orders 
FROM 
	dm_finance.grove_care_pdp o
WHERE
	created_date < current_Date  
	AND country in ('Austria','United States','Germany')
)
,agg AS 
(
SELECT 
	COALESCE(a.created_date,c.submitted_date) AS reporting_date,
	COALESCE(a.country,c.store_country) AS country,	
	COALESCE(a.new_recurring,c.new_recurring) AS new_recurring,	
	COALESCE(a.app_web,c.app_web) AS app_web,
	COALESCE(a.traffic_daily_unique_sessions,0) AS traffic_daily_unique_sessions,
	COALESCE(a.viewed,0) AS viewed,
	COALESCE(c.orders_submitted,0) AS orders_submitted,
	COALESCE(c.paid_orders,0) AS paid_orders,
	COALESCE(a.cart_orders,0) AS cart_orders
FROM 
	pdp a
FULL JOIN c
	ON a.created_date = c.submitted_date
	AND a.new_recurring = c.new_recurring
	AND a.country = c.store_country
	AND a.app_web = c.app_web
)
SELECT 
	*,
FROM 
	agg
WHERE 
	reporting_date < (current_date  - 1)
WITH NO SCHEMA BINDING
;
