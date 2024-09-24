CREATE OR REPLACE VIEW dm_payments.v_monthly_payments AS 
WITH sf_payment AS (
SELECT DISTINCT
  sp.id AS subscription_payment_id
 ,sp.charge_id__c AS psp_reference_id
 ,CAST(a.spree_customer_id__c AS INT) AS customer_id
 ,o.spree_order_number__c AS order_id
 ,o2.store_country AS store_country
 ,COALESCE(u.user_type, 'normal_customer') AS customer_type
 ,sp.subscription__c AS subscription_id
 ,sp.type__c AS payment_type
 ,sp.number__c::varchar AS payment_number
 ,sp.createddate::timestamp AS created_at
 ,CASE
   WHEN sp.message__c::TEXT = 'Agreement was canceled'::TEXT 
    THEN sp.status__c 
   ELSE sp.status__c 
  END AS status
 ,CASE
   WHEN o.spree_order_number__c ='R962588649'
    AND sp.number__c > 1 
   THEN sp.date_due__c::DATE+45 ELSE sp.date_due__c 
  END AS due_date
 ,sp.currency__c AS currency
 ,CASE
   WHEN sp.f_paid_manually__c
    THEN 'Manual'::character varying
   WHEN sp.payment_method__c::TEXT = 'AdyenContract'::TEXT
     AND BTRIM(sp.payment_method_details__c::TEXT) = 'sepadirectdebit'::TEXT
    THEN 'Adyen, SEPA'::character varying
   WHEN sp.payment_method__c::TEXT = 'AdyenContract'::TEXT 
    THEN 'Adyen, Other'::character varying
   WHEN sp.payment_method__c::TEXT = 'Adyen'::TEXT 
    THEN 'Adyen, 1-click'::character varying
   WHEN COALESCE(sp.amount_f_due__c, 0::numeric) = 0::numeric 
    THEN 'Amount Due = 0'::character varying
   ELSE COALESCE(sp.payment_method__c, 'Not Available'::character varying)
  END::CHARACTER VARYING(256) AS payment_method_detailed
 ,sp.amount_subscription__c AS amount
 ,COALESCE(sp.amount_f_due__c, NULL) AS amount_due
 ,COALESCE(sp.amount_paid__c, NULL) AS amount_paid
 ,sp.message__c AS failed_reason
 ,sp.tries_charge__c::VARCHAR AS attempts_to_pay
 ,'legacy' AS src_tbl
 ,'' AS payment_group_id
 FROM stg_salesforce.subscription_payment__c AS sp
   LEFT JOIN stg_salesforce.order AS o 
     ON o.id = sp.order__c
   LEFT JOIN ods_production."order" o2 
     ON o2.order_id = o.spree_order_number__c
   LEFT JOIN stg_salesforce.account AS a 
     ON a.id = sp.customer__c
   LEFT JOIN stg_api_production.spree_users AS u 
     ON a.spree_customer_id__c = u.id
)
,clean_billing_wallet AS (
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
 ,UPPER(b.status) AS status
 ,b.duedate::DATE
 ,b.attempts
 ,b.paymenttype
 ,b.contracttype
 ,b.shippingcost
 ,b.groupstrategy
 ,b.createdat
 ,c.type AS type_transaction
 ,c.status AS status_transaction
 ,c.failed_reason
 ,JSON_EXTRACT_PATH_TEXT(c.gateway_response,'referenceId') AS reference_ID
 ,JSON_EXTRACT_PATH_TEXT(c.gateway_response,'paymentMethod') AS payment_method
 ,JSON_EXTRACT_PATH_TEXT(c.gateway_response,'type') AS payment_method_type
 ,JSON_EXTRACT_PATH_TEXT(c.gateway_response,'transactionId') AS payment_transaction_ID
 ,CASE
   WHEN b.period = 1 
    THEN 'FIRST'
   ELSE 'RECURRING'
  END AS payment_type
 ,CASE
	 WHEN IS_VALID_JSON(JSON_SERIALIZE(b.payment_group_tax_breakdown[0]))
	   AND b.payment_group_tax_breakdown IS NOT NULL
	  THEN JSON_SERIALIZE(b.payment_group_tax_breakdown[0])
	 ELSE NULL
	END AS order_payload
 ,MAX(order_payload) OVER (PARTITION BY "group") AS max_order_payload	
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(max_order_payload	,'taxRate'),'') AS tax_rate
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(max_order_payload	,'tax'),'in_cents'),'')/100 AS order_tax_amount
 ,ROW_NUMBER() OVER (PARTITION BY b.uuid ORDER BY c.created_at DESC) AS rownum
FROM clean_billing_wallet a
  INNER JOIN oltp_billing.payment_order b 
    ON a.uuid = b.walletuuid
  LEFT JOIN oltp_billing.transaction c 
    ON c.account_to = b."group" 
   AND c.type <> 'refund'
WHERE TRUE 
  AND IS_VALID_JSON_ARRAY(JSON_SERIALIZE(b.lineitems))
  AND b.status NOT IN ('refund', 'partial refund')
  AND b.paymenttype = 'subscription'
)
,bs_payment AS (
SELECT
  CAST(b.customer_id AS INT) AS customer_id
 ,c.customer_type
 ,c.store_country
 ,b.order_id
 ,b.contractid AS subscription_id
 ,b.payment_id AS subscription_payment_id
 ,b.group AS payment_group_id
 ,b.createdat::timestamp AS created_at
 ,b.amount
 ,b.currency
 ,b.period::varchar AS payment_number
 ,b.status
 ,b.duedate AS due_date
 ,b.attempts AS attempts_to_pay
 ,b.payment_type
 ,b.failed_reason AS failed_reason
 ,b.reference_id AS psp_reference_id
 ,CASE 
   WHEN b.type_transaction= 'manual_booking' 
    THEN 'Manual' 
   WHEN b.payment_method IS NULL 
    THEN b.payment_method_type 
   ELSE COALESCE(b.payment_method, 'Not Available') 
  END AS payment_method_detailed
 ,CASE 
   WHEN b.status = 'paid'
     AND b.currency = 'USD' 
    THEN b.amount+order_tax_amount
   WHEN b.status='paid' 
    THEN b.amount 
   ELSE 0 
  END amount_paid
 ,CASE 
   WHEN b.currency = 'USD' 
    THEN b.amount + order_tax_amount
   WHEN b.currency <> 'USD' 
    THEN b.amount 
   ELSE 0 
  END amount_due
 ,'billing service' AS src_tbl
FROM billing_new b 
  LEFT JOIN master.order c 
    ON b.order_id=c.order_id
WHERE b.rownum = 1
)
,payment_final AS (
SELECT 
  subscription_payment_id
 ,psp_reference_id
 ,customer_id
 ,order_id
 ,store_country
 ,customer_type
 ,subscription_id
 ,payment_type
 ,payment_number
 ,created_at
 ,due_date
 ,status
 ,currency
 ,payment_method_detailed
 ,amount
 ,amount_due
 ,amount_paid
 ,failed_reason
 ,attempts_to_pay
 ,src_tbl
 ,payment_group_id
FROM sf_payment 
UNION ALL 
SELECT 
  subscription_payment_id
 ,psp_reference_id
 ,customer_id
 ,order_id
 ,store_country
 ,customer_type
 ,subscription_id
 ,payment_type
 ,payment_number
 ,created_at
 ,due_date
 ,status
 ,currency
 ,payment_method_detailed
 ,amount
 ,amount_due
 ,amount_paid
 ,failed_reason
 ,attempts_to_pay
 ,src_tbl
 ,payment_group_id
FROM bs_payment
)
SELECT
  status
 ,currency
 ,payment_type
 ,store_country
 ,customer_type
 ,payment_method_detailed
 ,failed_reason
 ,DATE_TRUNC('month',due_date) AS due_date
 ,due_date AS due_date_day
 ,COUNT(*)
 ,SUM(amount_due) amount_due
 ,NVL(SUM(amount_paid),0) amount_paid
FROM payment_final
WHERE TRUNC(due_date::DATE) BETWEEN '2021-12-01' AND CURRENT_DATE 
GROUP BY 1,2,3,4,5,6,7,8,9
WITH NO SCHEMA BINDING;