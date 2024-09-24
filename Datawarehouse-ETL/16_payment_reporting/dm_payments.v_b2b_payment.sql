CREATE OR REPLACE VIEW dm_payments.v_b2b_payment AS 
WITH clean_billing_wallet AS (
SELECT DISTINCT 
  uuid
 ,strategy
 ,external_customer AS customer_id
FROM oltp_billing.wallet 
)
,billing_new AS (
SELECT
  a.uuid
 ,a.strategy
 ,a.customer_id
 ,b.ordernumber AS order_id
 ,b.contractid
 ,b.uuid AS payment_id
 ,b.group
 ,b.scopeuuid
 ,b.walletuuid
 ,b.amount
 ,b.countrycode
 ,b.currency
 ,b.taxincluded
 ,b.period
 ,b.status
 ,b.updatedat
 ,b.duedate::DATE
 ,b.attempts
 ,b.paymenttype
 ,b.contracttype
 ,b.shippingcost
 ,b.groupstrategy
 ,c.type type_transaction
 ,c.status status_transaction
 ,c.failed_reason AS failedreason
 ,JSON_EXTRACT_PATH_TEXT(c.gateway_response,'referenceId') AS reference_ID
 ,JSON_EXTRACT_PATH_TEXT(c.gateway_response,'paymentMethod') AS payment_method
 ,JSON_EXTRACT_PATH_TEXT(c.gateway_response,'type') AS payment_method_type
 ,JSON_EXTRACT_PATH_TEXT(c.gateway_response,'transactionId') AS payment_transaction_ID
 ,CASE
   WHEN b.period = 1 
    THEN 'FIRST'
   ELSE 'RECURRING'
  END AS payment_type
 ,COUNT(*) OVER (PARTITION BY b.uuid) AS cnt
 ,ROW_NUMBER() OVER (PARTITION BY b.uuid ORDER BY b.updatedat DESC) AS rn
FROM clean_billing_wallet a
  INNER JOIN oltp_billing.payment_order b 
    ON a.uuid = b.walletuuid
  LEFT JOIN oltp_billing.transaction c 
    ON c.account_to = b."group"
)
,admin_panel_status AS (
SELECT 
 	JSON_EXTRACT_PATH_TEXT(payload,'id') AS subscription_id,
 	JSON_EXTRACT_PATH_TEXT(payload,'state') AS status,
 	kafka_received_at,
	ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY kafka_received_at DESC) AS rn
FROM s3_spectrum_kafka_topics_raw.customers_contracts   
)
,subscription_status AS (
SELECT 
  COALESCE(NULLIF(subscription_bo_id,''), subscription_id) AS contract_id
 ,status 
 ,subscription_id
 ,subscription_bo_id
 ,start_date
 ,cancellation_date
 ,product_name
 ,ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY (start_date) DESC) AS rn /*to remove duplicates in sub IDs allocated to subs_bo_id*/  
FROM master.subscription
)
,invoices AS (
SELECT 
  payment_group_id
 ,invoice_url
 ,invoice_number_pdf AS invoice_number
 ,invoice_date
FROM ods_production.billing_invoices_clerk 
)
,tax_info AS (
SELECT 
  order_id
 ,payment_group_id
 ,payment_group_tax_currency
 ,payment_group_tax_rate
FROM stg_curated.order_additional_info
)
,payments_tax AS (
SELECT 
  a.*
 ,b.payment_group_tax_currency 
 ,b.payment_group_tax_rate
FROM billing_new a
  LEFT JOIN tax_info b 
    ON a.order_id = b.order_id
   AND a."group" = b.payment_group_id   
)
,payments_final AS (
SELECT 
 *
 ,CASE
   WHEN payment_group_tax_currency = 'USD'
    THEN (payment_group_tax_rate * amount)::DECIMAL(10,2)
   ELSE ((payment_group_tax_rate * amount)/(1 + payment_group_tax_rate))::DECIMAL(10,2)
  END AS vat_amount_fix
FROM payments_tax
)
SELECT
  a.customer_id
 ,a.customer_type
 ,a.company_name
 ,c.store_country
 ,b.uuid
 ,b.strategy
 ,b.order_id
 ,b.contractid
 ,b.payment_id
 ,e.product_name
 ,b.group
 ,b.scopeuuid
 ,b.walletuuid
 ,b.amount
 ,b.countrycode
 ,b.currency
 ,b.taxincluded
 ,b.payment_group_tax_rate AS tax_rate
 ,ROUND(CASE
   WHEN b.currency = 'USD' 
	THEN ROUND(b.vat_amount_fix::DOUBLE PRECISION, 2)
   ELSE b.vat_amount_fix::DOUBLE PRECISION
  END::DECIMAL(22, 6), 2) AS order_tax_amount
 ,b.payment_group_tax_currency
 ,b.payment_group_tax_rate AS order_tax_rate
 ,b.period
 ,b.status
 ,b.duedate
 ,b.attempts
 ,b.paymenttype
 ,b.contracttype
 ,b.shippingcost
 ,b.groupstrategy
 ,b.payment_type
 ,b.type_transaction
 ,b.status_transaction
 ,b.failedreason
 ,b.reference_ID
 ,b.payment_method
 ,b.payment_method_type
 ,b.payment_transaction_ID
 ,b.cnt
 ,b.rn
 ,CASE 
   WHEN b.status= 'paid' THEN b.amount ELSE 0 END amount_paid
 ,CASE 
   WHEN b.status= 'refund' 
    THEN b.amount ELSE 0 
  END amount_refund
 ,CASE 
   WHEN b.status in ('new', 'paid', 'failed', 'pending') 
    THEN b.amount 
   ELSE 0 
  END AS amount_plan
 ,CASE 
   WHEN b.status in ('failed', 'pending') 
    THEN b.amount 
   ELSE 0 
  END AS amount_overdue
 ,a.active_subscription_value
 ,UPPER(COALESCE(d.status, e.status)) AS subscription_status
 ,f.invoice_url
 ,f.invoice_number
 ,f.invoice_date
FROM master.customer a
  INNER JOIN payments_final b
    ON a.customer_id=b.customer_id
   AND b.rn = 1
  LEFT JOIN master.order c
    ON b.order_id = c.order_id
  LEFT JOIN admin_panel_status d
    ON b.contractid = d.subscription_id 
   AND d.rn = 1
  LEFT JOIN subscription_status e 
    ON b.contractid=e.contract_id 
   AND e.rn = 1
  LEFT JOIN invoices f 
    ON b.group = f.payment_group_id
WHERE a.customer_type = 'business_customer' 
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_payments.v_b2b_payment to tableau; 
