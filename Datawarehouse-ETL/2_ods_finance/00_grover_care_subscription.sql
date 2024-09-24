BEGIN;
---- new infra
WITH flag_kafka AS  -- GC flag from kafka 
(
SELECT DISTINCT 
		event_timestamp,
		order_number AS order_id,
		CASE 
		deductible_fees
FROM 	
		staging.kafka_order_placed_v2  
WHERE 
		event_timestamp >= '2023-09-15'  
)
,
flag_boundary AS -- GC flag from boundary until kafka started populating 
(
SELECT DISTINCT 
		created_at,
		order_number AS order_id,
		CASE 
			ELSE NULL 
FROM 	
)
SELECT 
	DISTINCT 
	c.contract_id AS subscription_id,
    c.order_number AS order_id,
    c.customer_id,
	s.start_date AS subscription_start_date,
	o.submitted_date AS order_submitted_date,
	s.rank_subscriptions,
	s.subscriptions_per_customer,
	s.status AS subscription_status,
	s.subscription_value, 
    o.store_country AS country,
    c1.customer_type,
    CASE
		WHEN s.subscription_id IS NOT NULL 
        THEN 1
	ELSE 0
	CASE
		WHEN order_submitted_date::date < '2023-11-06' THEN 'disabled'
		ELSE NULL
    s.variant_sku,
	s.first_asset_delivery_date,
	s.subscription_plan,
	s.rental_period,
	s.committed_sub_value,
	CASE 
			WHEN s.subscription_id IS NOT NULL 
			THEN org.new_recurring
	ELSE NULL
	END AS new_recurring, -- To populate only paid subscriptions
	s.minimum_cancellation_date,
	s.subscription_bo_id
FROM 
	staging.customers_contracts c  
LEFT JOIN 
	ods_production.order o
	ON c.order_number = o.order_id 
LEFT JOIN 
	ods_production.subscription s
	ON c.contract_id  = s.subscription_id 
LEFT JOIN 
	ods_production.customer c1
	ON c.customer_id  = c1.customer_id 
LEFT JOIN 
	ods_production.order_retention_group org
	ON c.order_number  = org.order_id 
WHERE 	
	c.created_at >= '2023-09-15'   -- Grover Care Launch Date
	AND c.event_name = 'created'
)
SELECT 
		a.subscription_id,
		a.order_id,
		a.customer_id,
		a.subscription_start_date,
		a.order_submitted_date,
		a.rank_subscriptions,
		a.subscriptions_per_customer,
		a.subscription_status,
		a.subscription_value,
		a.country,
		a.customer_type,
		CASE 
			ELSE NULL
		CASE 
			WHEN (b.deductible_fees IS NOT NULL AND b.deductible_fees <> '') THEN 1
			ELSE 0
		END AS is_deductible_fees,
		a.variant_sku,
		a.first_asset_delivery_date,
		a.subscription_plan,
		a.rental_period,
		a.committed_sub_value,
		a.new_recurring,
		a.minimum_cancellation_date,
		a.subscription_bo_id
FROM 
LEFT JOIN 
		flag_kafka b
		ON a.order_id = b.order_id
LEFT JOIN 
		flag_boundary c
		ON a.order_id = c.order_id
;
---- old infra

WITH a AS (
SELECT 	
	*, 
	CONCAT(order_id,CONCAT(variant_sku,rental_plan_length)) AS pkey, -- used as subscription_id
	ROW_NUMBER() OVER (PARTITION BY pkey ORDER BY kafka_received_at ASC ) AS rn -- remove dups
FROM 
)
,b AS (
SELECT 
	* 
FROM 	
	a 
WHERE 
	rn = 1
),c AS (
SELECT 
	b.*,
	op.order_id AS oid
FROM 
	b
LEFT JOIN 
	ON op.order_id = b.order_id
), d AS (
SELECT 
	DISTINCT 
	c.pkey AS subscription_id,
    c.order_id,
    o.customer_id::varchar,
	NULL AS subscription_start_date,
	o.submitted_date AS order_submitted_date,
	NULL AS rank_subscriptions,
	NULL AS subscriptions_per_customer,
	NULL AS subscription_status,
	NULL AS subscription_value,
    o.store_country AS country,
    c1.customer_type,
    CASE
		WHEN c.oid IS NOT NULL 
        THEN 1
	ELSE 0
	CASE 
			ELSE NULL
	CASE 
		WHEN (c.deductible_fees IS NOT NULL AND c.deductible_fees <> '') THEN 1
		ELSE 0
	END AS is_deductible_fees,
    c.variant_sku,
	NULL AS first_asset_delivery_date,
	NULL AS subscription_plan,
	c.rental_plan_length::integer AS rental_period,
	NULL AS committed_sub_value,
	CASE 
			WHEN c.oid IS NOT NULL 
			THEN org.new_recurring
	ELSE NULL
	END AS new_recurring,  
	NULL AS minimum_cancellation_date,
	NULL AS subscription_bo_id
FROM 
	c
LEFT JOIN 
	ods_production.order o
	ON c.order_id = o.order_id 
LEFT JOIN 
	ods_production.subscription s
	ON c.pkey   = CONCAT(s.order_id,concat(s.variant_sku,s.rental_period)) 
LEFT JOIN 
	ods_production.customer c1
	ON o.customer_id  = c1.customer_id 
LEFT JOIN 
	ods_production.order_retention_group org
	ON c.order_id  = org.order_id 
)
SELECT 
	DISTINCT 
	subscription_id,
    order_id,
    customer_id,
	subscription_start_date::timestamp,
	order_submitted_date,
	rank_subscriptions::integer,
	subscriptions_per_customer::integer,
	subscription_status,
	subscription_value::integer, 
    country,
    customer_type,
	is_deductible_fees,
    variant_sku,
	first_asset_delivery_date::timestamp,
	subscription_plan,
	rental_period,
	committed_sub_value::integer, 
	new_recurring,  
	minimum_cancellation_date::timestamp,
	subscription_bo_id
FROM 
	d
WHERE 	
	TRUE 
UNION 
SELECT 
	*
FROM 
;

COMMIT;
