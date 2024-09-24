CREATE OR REPLACE VIEW dm_payments.v_ledger_monitoring AS 
SELECT 
  a.id AS resource_id
 ,TRUNC(a.resource_created_at_timestamp::DATE) AS resource_created_date
 ,a.status
 ,a.latest_movement_id
 ,a.latest_movement_status
 ,a.latest_movement_created_at_timestamp
 ,a.id||'_'||a.latest_movement_id AS key_
 ,a.currency
 ,a.value
 ,a.country_iso
 ,UPPER(a."type")
 ,a.replay
 ,a.payment_number
 ,a.payment_method
 ,a.latest_movement
 ,a.latest_movement_context
 ,a.latest_movement_context_reason
 ,b.failed_date
 ,b.failed_reason
 ,CASE 
   WHEN b.failed_reason ILIKE '%is invalid.%' 
    THEN 'Payment method is invalid' 
   WHEN b.failed_reason ILIKE '%Referenced transaction WITH UUID%' 
	THEN 'Referenced transaction UUID is not found'
   WHEN b.failed_reason ILIKE '%Parent transaction%'
	THEN 'Parent transaction is missing'
   WHEN b.failed_reason ILIKE '%Unable to capture authorization%'
    THEN 'Unable to capture authorization'
   ELSE b.failed_reason
  END AS failed_reason_grouped
 ,c.customer_type
 ,ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY latest_movement_created_at_timestamp) AS rn
FROM ods_production.ledger_curated a
  LEFT JOIN ods_production.ledger_failed_payment_reason b
    ON a.latest_movement_id = b.latest_movement_id 
   AND a.id = b.id
  LEFT JOIN ods_production.customer c
    ON a.customer_id = c.customer_id
WHERE TRUE
  AND a.latest_movement_context <> 'REFUND'
  AND a.latest_movement_status <> 'RUNNING'
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_payments.v_ledger_monitoring TO tableau;