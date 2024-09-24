DROP VIEW IF EXISTS dm_payments.v_retry_payments;
CREATE VIEW dm_payments.v_retry_payments AS 
WITH payments_all AS (
SELECT
  a.account_to AS group_id
 ,a.account_to || a.created_at AS key_
 ,a.status
 ,a.created_at AS createdat 
 ,a.type
 ,a.failed_reason AS failedreason 
 ,JSON_EXTRACT_PATH_TEXT(a.gateway_response,'paymentMethod') AS payment_method
 ,JSON_EXTRACT_PATH_TEXT(a.gateway_response,'type') AS payment_method_type
 ,JSON_EXTRACT_PATH_TEXT(a.gateway_response,'referenceId') AS reference_ID
 ,JSON_EXTRACT_PATH_TEXT(a.gateway_response,'transactionId') AS payment_transaction_ID
 ,b.contractid
 ,b.ordernumber
 ,b.uuid AS payment_id
 ,b.currency
 ,b.amount
 ,b.duedate
 ,CASE
   WHEN b."group" IS NULL 
    THEN FALSE 
   ELSE TRUE
  END AS group_id_available
 ,c.external_customer AS customer_id
 ,CASE
   WHEN b.period = 1 
    THEN 'FIRST'
   ELSE 'RECURRING'
  END AS payment_type
 ,b.paymenttype 
 ,ROW_NUMBER() OVER (PARTITION BY a.account_to, a.status ORDER BY a.created_at::DATE asc) AS rownum
FROM oltp_billing.transaction a
  LEFT JOIN oltp_billing.payment_order b
    ON a.account_to = b.group
  LEFT JOIN oltp_billing.wallet c
    ON b.walletuuid = c.uuid
WHERE a.type <> 'refund'
)
SELECT
  a.group_id
 ,a.key_
 ,a.status
 ,a.createdat
 ,a.type
 ,a.failedreason
 ,CASE
   WHEN a.payment_method = '' 
    THEN a.payment_method_type
   ELSE a.payment_method
  END AS payment_method
 ,a.reference_ID
 ,a.payment_transaction_ID
 ,a.contractid
 ,a.ordernumber
 ,a.payment_id
 ,a.currency
 ,a.amount
 ,a.duedate
 ,a.group_id_available
 ,a.customer_id
 ,a.payment_type
 ,a.paymenttype
 ,b.customer_type
 ,c.store_country
 ,CASE 
   WHEN a.status = 'failed' 
     AND a.rownum = 1 
    THEN TRUE 
   ELSE FALSE  
  END AS first_attempt_failed
 ,CASE 
   WHEN a.status = 'failed' 
     AND a.rownum = 1 
    THEN a.failedreason 
  END AS first_failed_reason
FROM payments_all a
  LEFT JOIN master.customer b
    ON a.customer_id = b.customer_id
  LEFT JOIN master.order c
    ON a.ordernumber = c.order_id
WITH NO SCHEMA BINDING;

GRANT SELECT ON	dm_payments.v_retry_payments to group BI;
GRANT SELECT ON dm_payments.v_retry_payments to tableau;