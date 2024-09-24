DROP TABLE IF EXISTS trans_dev.customer_info_external_services;

CREATE TABLE trans_dev.customer_info_external_services AS
WITH mapping AS (
SELECT
	DISTINCT customer_id,
	'Customer' AS entity
FROM
	ods_data_sensitive.customer_pii
UNION ALL
SELECT
	DISTINCT a.customer_id::int,
	'Allocation_1000' AS entity
FROM
	ods_production.allocation a
LEFT JOIN ods_production.asset ast ON
	ast.asset_id = a.asset_id
WHERE
	a.delivered_at IS NOT NULL
	AND ast.initial_price > 1000
UNION ALL
SELECT
	DISTINCT customer_id::int,
	'Allocation_DHL_Label' AS entity
FROM
	ods_production.allocation
WHERE
	lower(shipment_provider) LIKE '%dhl%'
	AND (shipment_tracking_number IS NOT NULL
		OR return_shipment_tracking_number IS NOT NULL)
UNION ALL
SELECT
	DISTINCT customer_id::int,
	'Allocation_Label' AS entity
FROM
	ods_production.allocation
WHERE
	(shipment_tracking_number IS NOT NULL
		OR return_shipment_tracking_number IS NOT NULL)
UNION ALL
SELECT
	DISTINCT a.customer_id::int,
	'Allocation_TV' AS entity
FROM
	ods_production.allocation a
LEFT JOIN ods_production.asset ast ON
	ast.asset_id = a.asset_id
WHERE
	subcategory_name = 'TV'
	AND delivered_at IS NOT NULL
	AND shipment_tracking_number IS NULL
	AND shipment_at IS NULL
UNION ALL
SELECT
	DISTINCT customer_id::int,
	'Allocation_UPS_Label' AS entity
FROM
	ods_production.allocation
WHERE
	lower(shipment_provider) LIKE '%ups%'
	AND (shipment_tracking_number IS NOT NULL
		OR return_shipment_tracking_number IS NOT NULL)
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer, BC_DE' AS entity
FROM
	ods_data_sensitive.customer_pii
WHERE
	customer_type = 'business_customer'
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_AT_DE' AS entity
FROM
	stg_realtimebrandenburg.burgel_data
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_AT_DE' AS entity
FROM
	stg_realtimebrandenburg.crifburgel_data
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_complete' AS entity
FROM
	ods_production.customer
WHERE
	profile_status = 'complete'
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_ods' AS entity
FROM
	ods_production.customer
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_paid' AS entity
FROM
	ods_production."order"
WHERE
	paid_date IS NOT NULL
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_voucher' AS entity
FROM
	ods_production."order"
WHERE
	voucher_code IS NOT NULL
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_DE' AS entity
FROM
	ods_data_sensitive.customer_pii
WHERE
	(billing_country IN ('Germany')
		OR shipping_country IN ('Germany'))
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_Schufa' AS entity
FROM
	ods_data_sensitive.schufa
UNION ALL
SELECT
	DISTINCT customer_id,
	'Customer_NL' AS entity
FROM
	ods_data_sensitive.customer_pii
WHERE
	(billing_country IN ('Netherlands')
		OR shipping_country IN ('Netherlands'))
UNION ALL
SELECT
	DISTINCT cp.customer_id,
	'Customer_address' AS entity
FROM
	ods_data_sensitive.customer_pii cp
WHERE
	(cp.billing_country IS NOT NULL
		OR cp.shipping_country IS NOT NULL )
UNION ALL
SELECT
	DISTINCT bd.customer_id,
	'Customer*' AS entity
FROM
	stg_realtimebrandenburg.boniversum_data bd
LEFT JOIN ods_production.customer cp ON
	bd.customer_id = cp.customer_id
WHERE
	 ((cp.customer_type = 'business_customer'
		AND company_type_name IN ('Einzelunternehmen', 'Freelancer'))
		OR cp.customer_type = 'normal_customer')
	AND (cp.billing_country IN ('Germany')
		OR cp.shipping_country IN ('Germany'))
UNION ALL
SELECT
	o.customer_id,
	CASE
		WHEN lower(op.payment_method) LIKE '%paypal%' THEN 'Order payment method_Paypal'
		WHEN lower(op.payment_method) LIKE '%adyen%' THEN 'Order payment Method_Adyen'
		WHEN lower(op.payment_method) LIKE '%dalenys%' THEN 'Order payment Method_Dalenys'
		WHEN lower(op.payment_method) LIKE '%sepa%' THEN 'Order payment Method_SEPA'
		WHEN op.payment_method = 'Credit/Debit Cards'
		AND lower(payment_method_type) LIKE '%braintree%' THEN 'Order payment Method_Braintree'
	END AS entity
FROM
	ods_data_sensitive.order_payment_method op
LEFT JOIN ods_production."order" o ON
	o.order_id = op.order_id
WHERE
	entity IS NOT NULL
GROUP BY
	1,
	2
UNION ALL
SELECT
	DISTINCT customer_id::int,
	'Ixopay' AS entity
FROM
	ods_data_sensitive.ixopay_transactions
WHERE
	customer_id IS NOT NULL
	AND customer_id != ''
UNION ALL
SELECT
	dc.customer_id,
	CASE
		WHEN lower(store_commercial) LIKE '%international%' THEN 'Subscription_DC_AT_NL'
		WHEN agency_dca = 'COEO' THEN 'Subscription_DC_COEO'
		WHEN agency_dca = 'Pair Finance' THEN 'Subscription_DC_Pair_Finance'
		WHEN customer_type = 'business_customer' THEN 'Subscription_DC_B2B'
	END AS entity
FROM
	ods_production.debt_collection dc
LEFT JOIN ods_production.subscription s ON
	dc.subscription_id = s.subscription_id
LEFT JOIN ods_production.customer c ON
	dc.customer_id = c.customer_id
WHERE
	entity IS NOT NULL
GROUP BY
	1,
	2
UNION ALL
SELECT
	DISTINCT customer_id::int AS customer_id,
	'Grover_cash' AS entity
FROM
	ods_grover_card.grover_card_transactions
WHERE
	event_name = 'payment-successful'
	)
   ,
services AS (
SELECT
	*
FROM
	staging.external_services_grover_gdprsheet1
WHERE
	entity != 'null')
	SELECT
	m.customer_id,
	m.entity AS mapping_entity,
	s.*
FROM
	mapping m
LEFT JOIN services s ON
	m.entity = s.entity;
