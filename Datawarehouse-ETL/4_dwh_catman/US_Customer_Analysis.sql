CREATE OR REPLACE VIEW dm_catman.us_geographical_analysis AS 
WITH products_orders AS ( 
	SELECT 
		o.order_id,
		s.category_name,
		s.subcategory_name, 
		s.product_sku,
		s.product_name,
		SUM(s.quantity) AS total_products,
		SUM(s.price) AS total_price
	FROM master."order" o 
	INNER JOIN ods_production.order_item s 
	  ON o.order_id = s.order_id
	WHERE o.created_date >= date_trunc('month',dateadd('month',-12,current_date))
	GROUP BY 1,2,3,4,5
)
, products_orders_category AS (
	SELECT 
		o.order_id,
		o.category_name,
		sum(o.total_products) AS total_products,
		sum(o.total_price) AS total_price
	FROM products_orders o
	GROUP BY 1,2
)
, row_order_category AS (  
	SELECT 
		o.order_id, 
		o.category_name, 
		ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.total_price DESC, o.total_products DESC, category_name) AS rowno
	FROM products_orders_category o
)
, row_order_product AS (
 	SELECT 
 		o.order_id,
 		o.subcategory_name,
 		o.product_sku,
 		o.product_name,
 		ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.total_price DESC, o.total_products DESC, o.product_name) AS rowno_product
 	FROM products_orders o 
 	INNER JOIN row_order_category c
 		ON o.order_id = c.order_id 
 		AND o.category_name = c.category_name
 		AND c.rowno = 1
 )
, order_category AS (
	SELECT 
		o.customer_id, 
		cat.order_id,
		cat.category_name,
		rp.subcategory_name,
		rp.product_sku,
		rp.product_name,
		o.created_date::date AS order_created_date,
		o.marketing_channel,
		count(DISTINCT CASE WHEN o.created_date IS NOT NULL THEN o.order_id END) AS carts,
		count(DISTINCT CASE WHEN o.completed_orders > 0 THEN o.order_id END) AS submitted_orders,
		count(DISTINCT CASE WHEN o.approved_date IS NOT NULL THEN o.order_id END) AS approved_orders,
		count(DISTINCT CASE WHEN o.status = 'PAID' THEN o.order_id END) AS paid_orders
	FROM row_order_category cat 
	LEFT JOIN master.ORDER o 
		ON cat.order_id = o.order_id
	LEFT JOIN ods_production.order_item oi 
		ON cat.order_id = oi.order_id
		AND cat.category_name = oi.category_name
	LEFT JOIN row_order_product rp 
		ON cat.order_id = rp.order_id
		AND rp.rowno_product = 1
	WHERE cat.rowno = 1
		AND o.store_country = 'United States'
	GROUP BY 1,2,3,4,5,6,7,8
)
, active_customers AS (
	SELECT DISTINCT 
	customer_id,
	sum(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS is_customer_active
	FROM master.subscription s 
	WHERE country_name = 'United States'
	GROUP BY 1
)	
SELECT 
	c.customer_id,
	CASE 
		WHEN c.age < 18 THEN 'under 18'
		WHEN c.age BETWEEN 18 AND 22 
			THEN '[18-22]'
		WHEN c.age BETWEEN 23 AND 27 
			THEN '[23-27]'
		WHEN c.age BETWEEN 28 AND 32 
			THEN '[28-32]'
		WHEN c.age BETWEEN 33 AND 37 
			THEN '[33-37]'
		WHEN c.age BETWEEN 38 AND 42 
			THEN '[38-42]'
		WHEN c.age BETWEEN 43 AND 47
			THEN '[43-47]'
		WHEN c.age BETWEEN 48 AND 52 
			THEN '[48-52]'
		WHEN c.age BETWEEN 53 AND 57 
			THEN '[53-57]'
		WHEN c.age BETWEEN 58 AND 62 
			THEN '[58-62]'
		WHEN c.age >= 63 
			THEN '> 62'
	END AS age_buckets,
	c.customer_type,
	c.billing_country,
	c.billing_city,
	c.billing_zip,
	c.shipping_country,
	c.shipping_city,
	c.shipping_zip, 
	c.subscription_limit,
	CASE 
		WHEN a.is_customer_active >=1 THEN 'Active'
		ELSE 'Inactive'
	END AS is_customer_active,
	z.state,
	z."state name" AS state_name,
	z.primary_city AS city_mapped,
	c.subscription_limit, 
	o.order_id,
	o.category_name,
	o.subcategory_name,
	o.product_sku,
	o.product_name,
	o.marketing_channel,
	o.order_created_date,
	o.carts,
	o.submitted_orders,
	o.approved_orders,
	o.paid_orders
FROM ods_data_sensitive.customer_pii c
LEFT JOIN order_category o
	ON c.customer_id = o.customer_id
LEFT JOIN active_customers a 
	ON a.customer_id = c.customer_id
INNER JOIN staging.us_zip_codes z
	ON z.zip = COALESCE(c.shipping_zip,billing_zip)
WHERE COALESCE(c.billing_country, c.shipping_country)  = 'United States'	
WITH NO SCHEMA BINDING;