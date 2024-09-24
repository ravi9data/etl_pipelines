
BEGIN;

TRUNCATE TABLE ods_production.payment_addon_35up;

INSERT INTO ods_production.payment_addon_35up	
WITH ledger_addon_35up AS (
SELECT
  external_id
 ,type
 ,id
 ,metadata
 ,current_allocated_amount
 ,status
 ,transactions
 ,value
 ,latest_movement
 ,slug
 ,value_without_tax
 ,country_iso
 ,consumed_at
 ,published_at
 ,resource_created_at
 ,tax_rate
 ,created_at
 ,currency
 ,origin
 ,current_allocated_taxes
 ,tax_amount
 ,replay
 ,order_id
 ,JSON_EXTRACT_PATH_TEXT(metadata,'name') AS addon_name
 ,latest_movement_id
 ,psp_reference_id
 ,latest_movement_status
 ,money_received_at_timestamp
 ,latest_movement_created_at_timestamp
 ,resource_created_at_timestamp
 ,created_at_timestamp
 ,customer_id
 ,payment_method
 ,amount_to_allocate
 ,latest_movement_context_reason
 ,ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC, consumed_at DESC) rn
FROM ods_production.ledger_curated
WHERE TRUE 
  AND status NOT IN ('REFUNDED', 'PARTIALLY_REFUNDED')  
  AND type = ('external-physical-otp')
)
,attempts AS (
SELECT 
  id AS resource_id
/* CHECK THIS LOGIC WHEN WE HAVE NEW RECORDS WE CAN ALSO CONSIDER TO COUNT NUMBER OF UNIQUE MOVEMENT_IDs */
 ,COUNT(1) OVER (PARTITION BY resource_id, status) AS attempts_to_pay   
FROM ledger_addon_35up
WHERE latest_movement_status IN ('SUCCESS','FAILED')
GROUP BY resource_id, status
)
,ledger_addon_35up_final AS (
SELECT 
  a.external_id AS addon_id
 ,a.addon_name
 ,a.order_id
 ,a.slug AS payment_id
 ,a.id AS resource_id
 ,a.latest_movement_id AS movement_id
 ,a.psp_reference_id
 ,UPPER(type) AS payment_type
 ,a.resource_created_at_timestamp AS due_date
 ,CASE a.latest_movement_status
   WHEN 'SUCCESS'
    THEN a.latest_movement_created_at_timestamp
   ELSE NULL
  END AS paid_date
 ,a.money_received_at_timestamp AS money_received_at
 ,CASE a.latest_movement_status
   WHEN 'RUNNING' 
    THEN a.latest_movement_created_at_timestamp 
   ELSE NULL
  END AS pending_date
 ,b.failed_date
 ,b.failed_reason
 ,c.attempts_to_pay
 ,a.resource_created_at_timestamp AS created_at
 ,a.created_at_timestamp AS updated_at
 ,CASE a.latest_movement_status 
   WHEN 'SUCCESS'
	THEN 'PAID'
   WHEN 'RUNNING'
	THEN 'PENDING'
   WHEN 'FAILED'
	THEN 'FAILED'    
  END AS status
 ,a.customer_id AS customer_id
 ,JSON_EXTRACT_PATH_TEXT(a.metadata,'store_code') AS store_code
 ,a.currency
 ,a.payment_method
 ,a.value::DECIMAL(22, 6) AS amount_due
 ,CASE a.latest_movement_status 
   WHEN 'SUCCESS'
	THEN a.amount_to_allocate::DECIMAL(22, 6) 
   ELSE 0
  END AS amount_paid
 ,a.tax_rate::DECIMAL(22, 6) AS tax_rate
 ,a.tax_amount::DECIMAL(22, 6) AS amount_tax
 ,a.latest_movement_context_reason AS payment_context_reason 	
FROM ledger_addon_35up a
  LEFT JOIN ods_production.ledger_failed_payment_reason b 
    ON a.id = b.id
   AND b.rn = 1
  LEFT JOIN attempts c 
    ON a.id = c.resource_id
WHERE a.rn = 1
)
,addon_refunds AS (
SELECT 
  slug 
 ,id AS resource_id
 ,CASE latest_movement_status
   WHEN 'SUCCESS'
    THEN latest_movement_created_at_timestamp 
   ELSE NULL
  END AS refund_date
 ,value::DECIMAL(22, 6) AS refund_amount
 ,ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC, consumed_at DESC) rn 
FROM ods_production.ledger_curated
WHERE TRUE
  AND type = 'external-physical-otp'
  AND status IN ('REFUNDED', 'PARTIALLY_REFUNDED') 
 )
SELECT DISTINCT
  a.payment_id
 ,a.resource_id
 ,a.movement_id
 ,a.psp_reference_id
 ,a.customer_id
 ,a.order_id
 ,a.addon_id
 ,a.addon_name
 ,a.payment_type
 ,a.created_at
 ,a.updated_at
 ,a.status
 ,a.due_date
 ,a.paid_date
 ,a.money_received_at
 ,a.currency
 ,a.payment_method
 ,a.payment_context_reason
 ,e.invoice_url
 ,e.invoice_number_pdf AS invoice_number
 ,e.invoice_date
 ,a.amount_due
 ,a.amount_paid
 ,a.amount_tax
 ,a.tax_rate
 ,a.pending_date
 ,a.failed_date
 ,a.attempts_to_pay
 ,a.failed_reason
 ,d.refund_date
 ,d.refund_amount
 ,CASE 
   WHEN a.store_code='de'
    THEN 'Germany'
   WHEN a.store_code='us'
    THEN 'United States'
   WHEN a.store_code='nl'
    THEN 'Netherlands'
   WHEN a.store_code='es'
    THEN 'Spain'
   WHEN a.store_code='at'
    THEN 'Austria'
  END AS country_name
 FROM ledger_addon_35up_final a
   LEFT JOIN addon_refunds d 
     ON a.resource_id = d.resource_id 
	AND d.rn = 1 
   LEFT JOIN ods_production.billing_invoices_clerk e 
     ON a.movement_id = e.movement_id
;

COMMIT;