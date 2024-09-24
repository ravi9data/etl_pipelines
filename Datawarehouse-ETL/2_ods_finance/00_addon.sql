BEGIN;

TRUNCATE TABLE ods_production.addon;

INSERT INTO ods_production.addon
WITH submitted_add_on AS (
SELECT
	customer_id::int,
	order_id,
	addon_id,
	variant_id,
	related_product_sku,
	related_variant_sku,
	event_timestamp,
	CASE
		WHEN store_code='de' THEN 'Germany'
		WHEN store_code='us' THEN 'United States'
		WHEN store_code='nl' THEN 'Netherlands'
		WHEN store_code='es' THEN 'Spain'
		WHEN store_code='at' THEN 'Austria'
	END AS Country
FROM
	stg_curated.checkout_addons_submitted_v1 casv 
), 
status_change_add_on AS (
	SELECT 
		*,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp) AS asc_idx,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp DESC) AS desc_idx
	FROM stg_curated.addons_order_status_change_v1)
, last_idx AS (
	SELECT *
FROM status_change_add_on
WHERE desc_idx = 1
)
, dates_ AS (
	SELECT 
	s.order_id, min(s.event_timestamp) AS submitted_date, 
	min(CASE WHEN sc.event_name = 'pending approval' THEN sc.event_timestamp ELSE NULL END) AS pending_approval_date, 
	min(CASE WHEN sc.event_name = 'approved' THEN sc.event_timestamp ELSE NULL END) AS approved_date,
	min(CASE WHEN p.order_id IS NOT NULL THEN p.paid_date ELSE NULL END) AS paid_date
FROM submitted_add_on s
LEFT JOIN status_change_add_on sc 
	ON	s.order_id = sc.order_id
LEFT JOIN ods_production.payment_addon p
	ON p.order_id = s.order_id
GROUP BY 1
)
SELECT 
	s.addon_id, 
	s.order_id, 
	s.customer_id, 
	s.related_variant_sku,
	s.related_product_sku,
	CASE WHEN s.addon_id=1 THEN 'Gigs' ELSE li.addon_name END addon_name,
	p.product_name,
	p.category_name,
	p.subcategory_name,
	--add on variant
	s.variant_id AS add_on_variant_id, 
	--order info 
	s.country,
	CASE 
		WHEN d.pending_approval_date IS NULL AND d.approved_date IS NULL AND d.paid_date IS NULL THEN 'submitted'
		WHEN d.pending_approval_date IS NOT NULL AND d.approved_date IS NULL AND d.paid_date IS NULL THEN 'pending approval'
      WHEN d.pending_approval_date IS NOT NULL AND d.approved_date IS NOT NULL AND d.paid_date IS NULL THEN 'approved'
		WHEN d.pending_approval_date IS NOT NULL AND d.approved_date IS NOT NULL AND d.paid_date IS NOT NULL THEN 'paid'
		ELSE li.event_name
	END AS add_on_status, o.status AS order_status, o.initial_scoring_decision,
	--dates
	d.submitted_date::timestamp, 
	d.approved_date,
	d.paid_date,
	--add on info
	o.order_value AS order_amount, 
	li.amount::decimal(30, 2) AS addon_amount,
	li.duration::int, 
	o.avg_plan_duration, 
	li.quantity::int
FROM submitted_add_on s
LEFT JOIN ods_production.order o
	ON s.order_id = o.order_id
LEFT JOIN ods_production.product p
	ON s.related_product_sku=p.product_sku 
LEFT JOIN dates_ d 
	ON s.order_id = d.order_id
LEFT JOIN last_idx li
	ON li.order_id = s.order_id;

--GRANT SELECT ON ods_production.addon TO tableau;
COMMIT;