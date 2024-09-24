DROP VIEW IF EXISTS dm_payments.v_payments_recovery;
CREATE VIEW dm_payments.v_payments_recovery as
WITH failed_payments AS (
SELECT 
  account_to AS group_uuid
 ,created_at::TIMESTAMP AS createdat
 ,type
 ,amount
 ,currency
 ,status AS initial_status
 ,failed_reason AS failedreason
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'paymentMethod') AS failed_payment_method
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'type') AS failed_payment_method_type
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'transactionId') AS failed_payment_transaction_id
FROM oltp_billing.transaction
WHERE TRUE
  AND status = 'failed' 
  AND type <> 'refund' 
) 
,last_success_event AS (
SELECT 
  account_to AS group_uuid
 ,created_at AS createdat_1
 ,status AS last_status
 ,type AS last_transaction_type
 ,created_at AS paid_date
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'paymentMethod') AS paid_payment_method
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'type') AS paid_payment_method_type
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'referenceId') AS paid_reference_ID
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'transactionId') AS paid_payment_transaction_ID
 ,ROW_NUMBER() OVER (PARTITION BY account_to ORDER BY created_at::TIMESTAMP DESC) AS rownum
FROM oltp_billing.transaction
WHERE TRUE 
  AND status ='success' 
  AND type <> 'refund'
)
SELECT 
  a.group_uuid
 ,a.createdat
 ,b.createdat_1 AS paid_date
 ,CASE 
   WHEN c.period = 1 
    THEN 'FIRST' 
   ELSE 'RECURRING' 
  END AS payment_type
 ,c.contractid AS contract_id
 ,c.duedate::DATE AS due_date
 ,c.ordernumber
 ,c.uuid AS payment_id
 ,a.type
 ,b.last_transaction_type
 ,a.amount
 ,a.currency
 ,c.amount AS amount_due
 ,c.currency AS amount_due_currency
 ,CASE 
   WHEN c."group" IS NULL 
    THEN 'N' 
   ELSE 'Y' 
  END AS group_id_available
 ,c.paymenttype
 ,a.failedreason
 ,a.initial_status
 ,b.last_status
 ,COALESCE(a.failed_payment_method, a.failed_payment_method_type) AS failed_payment_method
 ,COALESCE(b.paid_payment_method, b.paid_payment_method_type) AS paid_payment_method
 ,a.failed_payment_transaction_ID
 ,d.external_customer AS customer_id
 ,e.customer_type
 ,f.store_country
FROM failed_payments a
  LEFT JOIN last_success_event b 
    ON a.group_uuid = b.group_uuid 
    AND b.rownum = 1 
  LEFT JOIN oltp_billing.payment_order c 
    ON a.group_uuid = c.group
  LEFT JOIN oltp_billing.wallet d 
    ON c.walletuuid = d.uuid
  LEFT JOIN master.customer e 
    ON d.external_customer = e.customer_id
  LEFT JOIN master."order" f 
    ON c.ordernumber = f.order_id /*I could have used only master.order table to pull customer type AND store country instead of joining master.customer table above. However, there are some NULL order_ids in the master.order table. Therefore, it's not giving me the customer type for all. Therefore, I also joined WITH master.customer table */
ORDER BY createdat DESC
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_payments.v_payments_recovery to group BI;
GRANT SELECT ON dm_payments.v_payments_recovery to tableau;