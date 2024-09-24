DROP TABLE IF EXISTS basis;
CREATE TEMP TABLE basis AS 
WITH subs AS (
	SELECT
		customer_id,
		subscription_id,
		MIN(start_date) OVER (PARTITION BY customer_id)::date AS acquisition_date,
		start_date::date,
		cancellation_date::date,
		CASE WHEN cancellation_reason_new IN ('SOLD 1-EUR', 'SOLD EARLY') THEN TRUE ELSE FALSE END AS purchased,
		MAX(COALESCE(cancellation_date::date, '9999-12-31')) OVER (PARTITION BY customer_id) AS last_cancellation_date
	FROM master.subscription 
	WHERE start_date IS NOT NULL 
)
, next_sub AS (
	SELECT 
		c.customer_id,
		c.subscription_id,
		c.cancellation_date,
		c.purchased,
		s.start_date,
		ROW_NUMBER() OVER (PARTITION BY c.customer_id, c.subscription_id, c.cancellation_date ORDER BY s.start_date) AS rowno
	FROM subs c
	LEFT JOIN master.subscription s 
	  ON c.customer_id = s.customer_id
	  AND c.cancellation_date < s.start_date
	WHERE c.cancellation_date IS NOT NULL
)
, next_sub_start_date AS (
	SELECT 
		s.*,
		ns.start_date::date AS next_start_date,
		DATEDIFF('month', DATE_TRUNC('month', s.cancellation_date), DATE_TRUNC('month', ns.start_date)) AS months_between_subs
	FROM subs s
	LEFT JOIN next_sub ns
	  ON s.subscription_id = ns.subscription_id
	  AND ns.rowno = 1 
)
, raw_ AS (
	SELECT 
		r1.*,
		CASE WHEN r2.subscription_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_active_sub_in_churn_period,
		CASE WHEN has_active_sub_in_churn_period = FALSE THEN r1.cancellation_date END AS lost_date,
		MAX(CASE WHEN has_active_sub_in_churn_period = FALSE THEN r1.cancellation_date END) OVER (PARTITION BY r1.customer_id)::date AS last_lost_date_customer,
		ROW_NUMBER() OVER (PARTITION BY r1.customer_id, r1.cancellation_date ORDER BY r2.start_date) rowno1
	FROM next_sub_start_date r1
	LEFT JOIN next_sub_start_date r2
	  ON r1.customer_id = r2.customer_id
	  AND ((r2.start_date < r1.cancellation_date AND COALESCE(r2.cancellation_date, '9999-12-31') > r1.cancellation_date)
	       OR (r2.start_date > r1.cancellation_date AND r2.start_date < r1.next_start_date))
)
SELECT 
	customer_id,
	subscription_id,
	purchased,
	acquisition_date,
	start_date,
	cancellation_date,
	has_active_sub_in_churn_period,
	lost_date,
	next_start_date,
	months_between_subs,
	last_cancellation_date,
	last_lost_date_customer
FROM raw_
WHERE rowno1 = 1 
  AND last_lost_date_customer IS NOT NULL
;

DROP TABLE IF EXISTS rental_period;
CREATE TEMP TABLE rental_period AS 
WITH rental_period_raw AS (
	SELECT 
		customer_id,
		rental_period,
		count(DISTINCT subscription_id) AS num_subs
	FROM master.subscription
	GROUP BY 1,2
)
SELECT 
	customer_id,
	rental_period,
	ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY num_subs DESC, rental_period DESC) AS rowno
FROM rental_period_raw
;

DROP TABLE IF EXISTS category;
CREATE TEMP TABLE category AS 
WITH category_raw AS (
	SELECT 
		customer_id,
		category_name AS category,
		COUNT(DISTINCT product_sku) AS total_products,
		SUM(subscription_value_euro) AS total_sub_value
	FROM master.subscription
	WHERE product_sku IS NOT NULL
	GROUP BY 1,2
)
SELECT 
	customer_id,
	category, 
	ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_sub_value DESC, total_products DESC, category) rowno
FROM category_raw 
;

DROP TABLE IF EXISTS subcategory;
CREATE TEMP TABLE subcategory AS 
WITH subcategory_raw AS (
	SELECT 
		customer_id,
		subcategory_name AS subcategory,
		COUNT(DISTINCT product_sku) AS total_products,
		SUM(subscription_value_euro) AS total_sub_value
	FROM master.subscription
	WHERE product_sku IS NOT NULL
	GROUP BY 1,2
)
SELECT 
	customer_id,
	subcategory, 
	ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_sub_value DESC, total_products DESC, subcategory) rowno
FROM subcategory_raw 
;

DROP TABLE IF EXISTS purchased;
CREATE TEMP TABLE purchased AS 
SELECT
	customer_id,
	CASE WHEN cancellation_reason_new IN ('SOLD 1-EUR', 'SOLD EARLY') THEN TRUE ELSE FALSE END AS purchased,
	MAX(CASE WHEN purchased THEN 1 ELSE 0 END) OVER (PARTITION BY customer_id) AS ever_purchased,
	ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY cancellation_date DESC) AS rowno
FROM master.subscription
WHERE cancellation_date IS NOT NULL 
;

DROP TABLE IF EXISTS lost_customers;
CREATE TEMP TABLE lost_customers AS
SELECT DISTINCT
	b.customer_id,
	b.lost_date,
	b.next_start_date,
	b.months_between_subs,
	b.acquisition_date,
	b.last_cancellation_date,
	b.last_lost_date_customer,
	p.ever_purchased::boolean,
	p.purchased AS last_device_purchased,
	r.rental_period,
	c.category,
	s.subcategory,
	sub.country_name AS acquisition_country,
	sub.store_label AS acquisition_store,
	sub.subscriptions_per_customer
FROM basis b
LEFT JOIN rental_period r
  ON b.customer_id = r.customer_id
  AND r.rowno = 1
LEFT JOIN category c
  ON b.customer_id = c.customer_id
  AND c.rowno = 1
LEFT JOIN subcategory s
  ON b.customer_id = s.customer_id
  AND s.rowno = 1
LEFT JOIN purchased p
  ON b.customer_id = p.customer_id
  AND p.rowno = 1
LEFT JOIN master.subscription sub
  ON b.customer_id = sub.customer_id
  AND sub.rank_subscriptions = 1
WHERE b.lost_date IS NOT NULL 
;

DROP TABLE IF EXISTS dm_product.churn_dashboard;
CREATE TABLE dm_product.churn_dashboard AS
SELECT * 
FROM lost_customers 
UNION
SELECT DISTINCT
	b.customer_id,
	NULL::date,
	NULL::date,
	NULL::int,
	MIN(b.start_date) OVER (PARTITION BY b.customer_id)::date AS acquisition_date,
	MAX(b.cancellation_date) OVER (PARTITION BY b.customer_id)::date AS last_cancellation_date,
	NULL::date,
	p.ever_purchased::boolean,
	p.purchased AS last_device_purchased,
	r.rental_period,
	c.category,
	s.subcategory,
	sub.country_name AS acquisition_country,
	sub.store_label AS acquisition_store,
	sub.subscriptions_per_customer
FROM master.subscription b
LEFT JOIN rental_period r
  ON b.customer_id = r.customer_id
  AND r.rowno = 1
LEFT JOIN category c
  ON b.customer_id = c.customer_id
  AND c.rowno = 1
LEFT JOIN subcategory s
  ON b.customer_id = s.customer_id
  AND s.rowno = 1
LEFT JOIN purchased p
  ON b.customer_id = p.customer_id
  AND p.rowno = 1
LEFT JOIN master.subscription sub
  ON b.customer_id = sub.customer_id
  AND sub.rank_subscriptions = 1
WHERE NOT EXISTS (SELECT NULL FROM lost_customers l WHERE l.customer_id = b.customer_id)
  AND b.start_date IS NOT NULL 
;

