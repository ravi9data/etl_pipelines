BEGIN;

TRUNCATE TABLE ods_production.payment_addon;

INSERT INTO ods_production.payment_addon 
WITH addon_name as (
SELECT 
	event_name,
	order_id,
	addon_name,
	event_timestamp,
	ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp desc) AS rn
FROM stg_curated.addons_order_status_change_v1  
),
addon_country AS (
SELECT order_id,
store_code,
CASE 
		WHEN store_code='de' THEN 'Germany'
		WHEN store_code='us' THEN 'United States'
		WHEN store_code='nl' THEN 'Netherlands'
		WHEN store_code='es' THEN 'Spain'
		WHEN store_code='at' THEN 'Austria'
	END AS country_name
FROM stg_curated.checkout_addons_submitted_v1 ),
ledger_addon_all AS (
SELECT
	external_id,
	type,
	id,
	metadata,
	current_allocated_amount,
	status,
	transactions,
 	value,
	latest_movement,
	slug,
	value_without_tax,
 	country_iso,
	consumed_at,
 	published_at,
 	resource_created_at,
 	tax_rate,
 	created_at,
 	currency,
 	origin,
 	current_allocated_taxes,
 	tax_amount,
 	replay,
	order_id,
	latest_movement_id,
	psp_reference_id,
    latest_movement_status,
	money_received_at_timestamp,
	latest_movement_created_at_timestamp,
	resource_created_at_timestamp,
	created_at_timestamp,
	customer_id,
	payment_method,
	amount_to_allocate,
	latest_movement_context_reason,
	variant_id,
 	ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC, consumed_at DESC) rn
FROM ods_production.ledger_curated
WHERE status NOT IN ('REFUNDED', 'PARTIALLY_REFUNDED')  
AND type in ('addon-purchase','addon')
),
attempts AS (
SELECT 
 	--slug AS payment_id,
	id as resource_id,
	count(1) AS attempts_to_pay  
FROM ledger_addon_all
WHERE 
	latest_movement_status IN ('SUCCESS','FAILED')
	AND (replay IS NULL OR UPPER(replay)='FALSE')
GROUP BY 1
),
ledger_addon_final AS (
SELECT 
	a.external_id AS addon_id,     
 	a.order_id,
	a.slug AS payment_id,
	a.id as resource_id,
  	a.latest_movement_id AS movement_id,
 	a.psp_reference_id,
 	UPPER(type) AS payment_type,
 	a.resource_created_at_timestamp AS due_date,
 	CASE a.latest_movement_status
  		WHEN 'SUCCESS' THEN a.latest_movement_created_at_timestamp
  	     ELSE NULL
      END AS paid_date,
	a.money_received_at_timestamp AS money_received_at,
  	CASE a.latest_movement_status
   		WHEN 'RUNNING' THEN a.latest_movement_created_at_timestamp 
   	     ELSE NULL
      END AS pending_date,
    b.failed_date,
    b.failed_reason,
	c.attempts_to_pay,
	a.resource_created_at_timestamp AS created_at,
	a.created_at_timestamp AS updated_at,
 	CASE a.latest_movement_status 
  		WHEN 'SUCCESS' THEN 'PAID'
  		WHEN 'RUNNING' THEN 'PENDING'
  		WHEN 'FAILED'  THEN 'FAILED'    
  	  END AS status,
 	a.customer_id AS customer_id, 
 	a.variant_id, 
 	--JSON_EXTRACT_PATH_TEXT(a.metadata,'store_code') AS store_code, /*since it's not available under the metadata for addons, I'm taking it from addons_order_status_change_v1 table*/
 	a.currency,
 	a.payment_method,
 	a.value::DECIMAL(22, 6) AS amount_due,
 	CASE a.latest_movement_status 
  		WHEN 'SUCCESS' THEN a.amount_to_allocate::DECIMAL(22, 6) 
  	      ELSE 0
   	  END AS amount_paid,
 	a.tax_rate::DECIMAL(22, 6) AS tax_rate,
 	a.tax_amount::DECIMAL(22, 6) AS amount_tax,
 	a.latest_movement_context_reason AS payment_context_reason 	
FROM ledger_addon_all a
  LEFT JOIN ods_production.ledger_failed_payment_reason b 
    ON a.id = b.id
   AND b.rn = 1	
  LEFT JOIN attempts c 
    ON a.id=c.resource_id
WHERE 
	a.rn=1
),
addon_refunds AS (
SELECT 
  	slug, 
  	id AS resource_id,
  	CASE latest_movement_status
 		WHEN 'SUCCESS' THEN latest_movement_created_at_timestamp 
  		 ELSE NULL
   	  END AS refund_date, 
 	value::DECIMAL(22, 6) AS refund_amount,
 	ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC, consumed_at DESC) rn 
FROM ods_production.ledger_curated
WHERE 
 	type in ('addon-purchase','addon')
  	AND status IN ('REFUNDED', 'PARTIALLY_REFUNDED') 
 )
 SELECT 
 	a.payment_id,
 	a.resource_id,
 	a.movement_id,
	a.psp_reference_id,
	a.customer_id,
	a.order_id,
	a.addon_id, 
	b.addon_name,
	a.variant_id,
	a.payment_type,
	a.created_at,
	a.updated_at,
	a.status,
	a.due_date,
	a.paid_date,
	a.money_received_at,
	a.currency,
	a.payment_method,
	a.payment_context_reason,
	e.invoice_url,
	e.invoice_number_pdf AS invoice_number,
	e.invoice_date,
	a.amount_due,
	a.amount_paid,
	a.amount_tax,
	a.tax_rate,
	a.pending_date,
	a.failed_date, 
	a.attempts_to_pay,
	a.failed_reason,
 	d.refund_date,
 	d.refund_amount,
 	c.country_name
 FROM ledger_addon_final a
   LEFT JOIN addon_name b 
     ON a.order_id=b.order_id 
    AND b.rn = 1
   LEFT JOIN addon_country c 
     ON a.order_id=c.order_id
   LEFT JOIN addon_refunds d 
     ON a.resource_id=d.resource_id 
    AND d.rn = 1 
   LEFT JOIN ods_production.billing_invoices_clerk e 
     ON a.movement_id = e.movement_id
;

COMMIT;