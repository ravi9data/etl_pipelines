CREATE TEMP TABLE vector AS 
WITH fact_days AS (
	SELECT DISTINCT 
		datum AS fact_day
	FROM public.dim_dates d
	WHERE datum >= dateadd('week',-1,date_trunc('week',current_date)) 
		AND datum <= current_date
)
, product_countries AS (
	SELECT DISTINCT
		p.product_sku, 
		v.variant_sku,
		s.country_name AS country_name
	FROM ods_production.product p
	LEFT JOIN ods_production.variant v
		ON v.product_id = p.product_id
	CROSS JOIN (SELECT DISTINCT country_name FROM ods_production.store WHERE country_name NOT IN ('Andorra', 'United Kingdom')) s
)
SELECT DISTINCT 
	d.fact_day,
	p.product_sku,
	p.variant_sku,
	p.country_name,
	s.customer_type, 
	s.new_recurring
FROM fact_days d
CROSS JOIN product_countries p
CROSS JOIN (SELECT DISTINCT customer_type, new_recurring FROM master.subscription) s
;



CREATE TEMP TABLE tmp_weekly_performance_report AS 
WITH fact_dates AS (
	SELECT DISTINCT fact_day
	FROM vector
)
, orders_raw AS (
	SELECT
		o.order_id,
		o.store_country AS country_name,
		i.product_sku AS product_sku,
		i.variant_sku AS variant_sku, 
		o.created_date::date AS created_date,
		o.submitted_date::date submitted_date,
		o.new_recurring, 
		COALESCE(o.customer_type, CASE WHEN o.store_commercial NOT LIKE '%B2B%' THEN 'normal_customer' ELSE 'business_customer' END) AS customer_type,
		count(*) OVER (PARTITION BY o.order_id) AS orders_number,
		count(DISTINCT o.order_id)/orders_number::float AS carts,
		count(DISTINCT CASE WHEN submitted_date IS NOT NULL THEN o.order_id END)/orders_number::float AS completed_orders,
		count(DISTINCT CASE WHEN o.status = 'PAID' THEN o.order_id END)/orders_number::float AS paid_orders
	FROM ods_production.order_item i
	INNER JOIN master."order" o 
		ON i.order_id = o.order_id	
	WHERE o.created_date::date >= (SELECT min(fact_day) FROM vector)
	GROUP BY 1,2,3,4,5,6,7,8
)
, cart_orders AS (
	SELECT
		created_date,
		country_name,
		product_sku,
		variant_sku,
		customer_type,
		new_recurring,
		sum(carts) AS carts
	FROM orders_raw 
	GROUP BY 1,2,3,4,5,6
)
, completed_paid_orders AS (
	SELECT 
		submitted_date,
		country_name,
		product_sku,
		variant_sku,
		customer_type,
		new_recurring,
		sum(completed_orders) AS completed_orders,
		sum(paid_orders) AS paid_orders
	FROM orders_raw
	GROUP BY 1,2,3,4,5,6
)
, acquired_subs AS (
	SELECT
		start_date::date AS start_date,
		s.country_name AS country_name,
		s.product_sku AS product_sku,
		s.variant_sku AS variant_sku,
		o.customer_type,
		s.new_recurring,
		sum(subscription_value) AS acquired_subscription_value,
		sum(subscription_value_euro) AS acquired_subscription_value_euro,
		sum(committed_sub_value) AS acquired_committed_subscription_value,
		avg(subscription_value) AS avg_price,
		avg(rental_period) AS avg_rental_duration,
		count(DISTINCT subscription_id) AS acquired_subscriptions
	FROM master.subscription s
	LEFT JOIN master.order o 
		ON o.order_id = s.order_id 
	WHERE s.start_date::date >= (SELECT min(fact_day) FROM vector)
	GROUP BY 1,2,3,4,5,6
) 
, cancelled_subs AS (
	SELECT
		cancellation_date::date AS cancellation_date,
		COALESCE(s.country_name, 'N/A') AS country_name,
		COALESCE(product_sku, 'N/A') AS product_sku,
		COALESCE(variant_sku, 'N/A') AS variant_sku,
		o.customer_type,
		s.new_recurring,
		sum(subscription_value) AS cancelled_subscription_value,
		sum(subscription_value_euro) AS cancelled_subscription_value_euro,
		count(DISTINCT subscription_Id) AS cancelled_subscriptions,
		avg(effective_duration) AS avg_effective_duration
	FROM master.subscription s
	LEFT JOIN master.order o 
		ON o.order_id = s.order_id
	WHERE s.cancellation_date::date >= (SELECT min(fact_day) FROM vector)
	GROUP BY 1,2,3,4,5,6
)
, pre_final AS (
	SELECT  
       v.fact_day AS fact_day, 
       v.product_sku AS product_sku,
       v.variant_sku AS variant_sku,
       v.country_name AS country_name,
       v.customer_type,
       v.new_recurring,
       COALESCE(carts, 0) AS carts,
       COALESCE(completed_orders, 0) AS completed_orders,
       COALESCE(paid_orders, 0) AS paid_orders,
       COALESCE(acquired_subscription_value, 0) AS acquired_subscription_value,
       COALESCE(acquired_subscription_value_euro, 0) AS acquired_subscription_value_euro,
       COALESCE(acquired_committed_subscription_value, 0) AS acquired_committed_subscription_value,
       COALESCE(avg_price, 0) AS avg_price,
       COALESCE(avg_rental_duration, 0) AS avg_rental_duration,
       COALESCE(acquired_subscriptions, 0) AS acquired_subscriptions,
       COALESCE(cancelled_subscription_value, 0) AS cancelled_subscription_value,
       COALESCE(cancelled_subscription_value_euro, 0) AS cancelled_subscription_value_euro,
       COALESCE(cancelled_subscriptions, 0) AS cancelled_subscriptions
	FROM vector v
	LEFT JOIN cart_orders o 
		ON v.product_sku = o.product_sku
			AND v.variant_sku = o.variant_sku
			AND v.fact_day = o.created_date
			AND v.country_name = o.country_name
			AND v.customer_type = o.customer_type 
			AND v.new_recurring = o.new_recurring
	LEFT JOIN completed_paid_orders cp
		ON v.product_sku = cp.product_sku
			AND v.variant_sku = cp.variant_sku
			AND v.fact_day = cp.submitted_date
			AND v.country_name = cp.country_name
			AND v.customer_type = cp.customer_type 
			AND v.new_recurring = cp.new_recurring	
	LEFT JOIN acquired_subs a
		ON a.product_sku = v.product_sku
			AND a.variant_sku = v.variant_sku
			AND a.start_date = v.fact_day
			AND a.country_name = v.country_name
			AND a.customer_type = v.customer_type
			AND a.new_recurring = v.new_recurring
	LEFT JOIN cancelled_subs c 
		ON c.product_sku = v.product_sku
			AND v.variant_sku = c.variant_sku
			AND c.cancellation_date = v.fact_day
			AND c.country_name = v.country_name
			AND c.customer_type = v.customer_type
			AND c.new_recurring = v.new_recurring
	WHERE COALESCE(carts, 0) +
       COALESCE(completed_orders, 0) +
       COALESCE(paid_orders, 0) +
       COALESCE(acquired_subscription_value, 0) +
       COALESCE(acquired_subscription_value_euro, 0) +
       COALESCE(acquired_committed_subscription_value, 0) +
       COALESCE(avg_price, 0) +
       COALESCE(avg_rental_duration, 0) +
       COALESCE(acquired_subscriptions, 0) +
       COALESCE(cancelled_subscription_value, 0) +
       COALESCE(cancelled_subscription_value_euro, 0) +
       COALESCE(cancelled_subscriptions, 0) <> 0	
)
SELECT 
	date_trunc('week',ppf.fact_day)::date AS week_of_fact_day,
	ppf.product_sku,
	ppf.variant_sku,
	product_name,
    brand,
    category_name,
    subcategory_name,
	ppf.country_name,
   	ppf.customer_type,
    sum(ppf.carts) AS carts,
    sum(ppf.completed_orders) AS completed_orders,
    sum(ppf.paid_orders) AS paid_orders,
    sum(ppf.acquired_subscription_value) AS acquired_subscription_value,
    sum(ppf.acquired_subscription_value_euro) AS acquired_subscription_value_euro,
    sum(ppf.acquired_committed_subscription_value) AS acquired_committed_subscription_value,
    sum(ppf.avg_price) AS avg_price,
    sum(ppf.avg_rental_duration) AS avg_rental_duration,
    sum(ppf.acquired_subscriptions) AS acquired_subscriptions,
    sum(ppf.cancelled_subscription_value) AS cancelled_subscription_value,
    sum(ppf.cancelled_subscription_value_euro) AS cancelled_subscription_value_euro,
    sum(ppf.cancelled_subscriptions) AS cancelled_subscriptions
FROM pre_final ppf
LEFT JOIN ods_production.product p 
	ON p.product_sku = ppf.product_sku
GROUP BY 1,2,3,4,5,6,7,8,9	
;


BEGIN TRANSACTION;

DELETE FROM dm_catman.weekly_performance_report_financial_metrics
WHERE fact_day::date >= dateadd('week',-1,date_trunc('week',current_date));

INSERT INTO dm_catman.weekly_performance_report_financial_metrics
SELECT *
FROM tmp_weekly_performance_report;

END TRANSACTION;

DROP TABLE IF EXISTS vector;

DROP TABLE IF EXISTS tmp_weekly_performance_report;
