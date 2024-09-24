CREATE OR REPLACE VIEW dm_finance.v_addon_35up_order_metrics as 
WITH avg_order AS (
SELECT 
	avg(order_value) AS order_value
FROM 
	master.order
WHERE
	status = 'PAID' -- ALL countries
), 
a AS (
SELECT
	*,
	CASE 
		WHEN status = 'PAID' 
		THEN addon_item_count ELSE 0
	END AS add_ons_paid,
	CASE 
		WHEN status = 'PAID' 
		THEN addon_price ELSE 0
	END AS add_ons_revenue_paid,
	CASE 
		WHEN status = 'PAID' 
		THEN order_item_count ELSE 0
	END AS subscriptions_paid, 
	CASE 
		WHEN status = 'PAID' 
		THEN order_value ELSE 0
	END AS order_revenue_paid,
	CASE 
		WHEN status = 'PAID' 
		THEN 1 ELSE 0
	END AS orders_paid
FROM
	ods_production.addon_35up_order
)
SELECT 
	submitted_date::date AS reporting_date
	,sum(add_ons_paid) AS add_ons_paid
	,sum(subscriptions_paid) AS subscriptions_paid
	,sum(add_ons_revenue_paid) AS add_ons_revenue_paid
	,sum(addon_item_count) AS addon_item_count
	,count(*)  AS total_orders 
	,avg(add_ons_revenue_paid) AS avg_add_ons_revenue_paid
	,avg(avg_order.order_value) AS order_value
	,sum(orders_paid) AS orders_paid
FROM 
	a
LEFT JOIN 
	avg_order
	ON 1=1
GROUP BY 
	1
WITH NO SCHEMA BINDING
;

GRANT SELECT  on  dm_finance.v_addon_35up_order_metrics TO tableau;
