BEGIN;


WITH billing_service_payment_group AS (
/*THIS IS NEEDED TO GET INVOICES OF NEW INFRA GROVER CARE PAYMENTS
 * CURRENTLY THEY ARE GENERATED FROM BS*/
SELECT DISTINCT 
  contractid 
  ,period
  ,"group" AS payment_group_id
FROM oltp_billing.payment_order bnpo 
WHERE TRUE 
  AND paymenttype = 'subscription'
)
SELECT
  led.external_id
 ,led.type
 ,led.id
 ,led.payment_number
 ,led.metadata
 ,led.current_allocated_amount
 ,led.status
 ,led.transactions
 ,led.value
 ,led.latest_movement
 ,led.slug
 ,led.value_without_tax
 ,led.country_iso
 ,led.consumed_at
 ,led.published_at
 ,led.resource_created_at
 ,led.tax_rate
 ,led.created_at
 ,led.currency
 ,led.origin
 ,led.current_allocated_taxes
 ,led.tax_amount
 ,led.replay
 ,led.order_id
 ,led.latest_movement_id
 ,led.psp_reference_id
 ,led.latest_movement_status
 ,led.money_received_at_timestamp
 ,led.latest_movement_created_at_timestamp
 ,led.resource_created_at_timestamp
 ,led.created_at_timestamp
 ,led.customer_id
 ,led.payment_method
 ,led.amount_to_allocate
 ,led.latest_movement_context_reason
 ,ROW_NUMBER() OVER (PARTITION BY led.id ORDER BY led.created_at DESC, led.consumed_at DESC) rn
 ,bs.payment_group_id
FROM ods_production.ledger_curated led 
  LEFT JOIN billing_service_payment_group AS bs
    ON led.external_id = bs.contractid
   AND led.payment_number = bs.period
WHERE TRUE 
  AND led.status NOT IN ('REFUNDED', 'PARTIALLY_REFUNDED')  
)
,attempts AS (
SELECT 
	id as resource_id
	,COUNT(1) AS attempts_to_pay  
WHERE TRUE
	AND latest_movement_status IN ('SUCCESS','FAILED')
	AND (replay IS NULL OR UPPER(replay) = 'FALSE')
GROUP BY 1
)
SELECT 
  a.external_id AS subscription_id     
 ,a.order_id
 ,a.slug AS payment_id
 ,a.id as resource_id
 ,a.latest_movement_id AS movement_id
 ,a.psp_reference_id
 ,UPPER(type) AS payment_type
 ,a.payment_number
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
 ,a.currency
 ,a.country_iso
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
 ,a.payment_group_id
  LEFT JOIN ods_production.ledger_failed_payment_reason b 
    ON a.id = b.id
   AND b.rn = 1 
  LEFT JOIN attempts c 
    ON a.id=c.resource_id
WHERE a.rn = 1
)
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
  AND status IN ('REFUNDED', 'PARTIALLY_REFUNDED') 
 )
 SELECT 
  a.payment_id
 ,a.resource_id
 ,a.movement_id
 ,a.payment_group_id
 ,a.psp_reference_id
 ,a.customer_id
 ,a.order_id
 ,a.subscription_id
 ,a.payment_type
 ,a.payment_number
 ,a.created_at
 ,a.updated_at
 ,a.status
 ,a.due_date
 ,a.paid_date
 ,a.money_received_at
 ,a.currency
 ,a.payment_method
 ,a.payment_context_reason
 ,COALESCE(e1.invoice_url, e2.invoice_url) AS invoice_url 
 ,COALESCE(e1.invoice_number_pdf, e2.invoice_number_pdf) AS invoice_number
 ,COALESCE(e1.invoice_date, e2.invoice_date) AS invoice_date
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
 ,CASE a.country_iso
 	 WHEN 'DE'
 	  THEN 'Germany'
 	 WHEN 'ES'
 	  THEN 'Spain'
 	 WHEN 'NL'
 	  THEN 'Netherlands'
 	 WHEN 'AT'
 	  THEN 'Austria'
 	 WHEN 'US'
 	  THEN 'United States'
 	END AS country_name
    ON a.resource_id = d.resource_id 
   AND d.rn = 1 
  LEFT JOIN ods_production.billing_invoices_clerk e1 
    ON a.payment_group_id = e1.payment_group_id
  LEFT JOIN ods_production.billing_invoices_clerk e2 
    ON a.movement_id = e2.movement_id
;
