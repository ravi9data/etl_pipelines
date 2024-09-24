CREATE OR REPLACE VIEW dm_payments.v_failed_payments_all AS 
WITH billing_transactions AS (
SELECT 
  account_to
 ,COUNT(uuid) OVER (PARTITION BY account_to) AS attempts
 ,status AS transaction_status
 ,type AS transaction_type
 ,created_at AS createddate
 ,updated_at
 ,JSON_EXTRACT_PATH_TEXT(gateway_response, 'paymentMethod') AS payment_method
 ,JSON_EXTRACT_PATH_TEXT(gateway_response, 'type') AS payment_methodtype
 ,JSON_EXTRACT_PATH_TEXT(gateway_response, 'referenceId') AS psp_reference
 ,failed_reason
FROM oltp_billing.transaction
)
,billing_payments AS (
SELECT 
  a.uuid AS payment_id
 ,a.ordernumber AS order_id
 ,a.contractid AS subscription_id
 ,a.group AS group_id
 ,a.duedate AS due_date
 ,a.updatedat
 ,a.period AS payment_number
 ,UPPER(a.status) AS payment_status
 ,UPPER(split_part(a.paymentpurpose, ';', 1)) AS payment_type
FROM oltp_billing.payment_order a
WHERE a.paymenttype = 'subscription'
)
,billing_payments_all AS (
SELECT
  a.*
 ,b.psp_reference
 ,b.attempts
 ,b.transaction_status
 ,b.createddate
 ,b.updated_at AS updateddate
 ,CASE
   WHEN b.payment_methodtype = 'pay-by-invoice' 
    THEN b.payment_methodtype
   WHEN b.payment_methodtype IS NULL
     AND b.payment_method IS NULL
     AND b.transaction_type = 'manual_booking' 
    THEN 'Manual'
   ELSE NULLIF(b.payment_method, '')
  END AS payment_method_type
 ,b.failed_reason
 ,CASE
   WHEN b.transaction_status = 'success'
     AND a.payment_status IN ('paid', 'manual_booking_summary', 'partial refund', 'refund') 
    THEN b.createddate
   WHEN a.payment_status IN ('paid', 'manual_booking_summary') 
    THEN a.updatedat::TIMESTAMP
    ELSE NULL
  END AS paid_date
FROM billing_payments a
  INNER JOIN billing_transactions b
    ON a.group_id = b.account_to
)
,billing_payments_final AS (
SELECT
  payment_id
 ,subscription_id
 ,order_id
 ,psp_reference
 ,group_id
 ,due_date
 ,paid_date
 ,payment_number
 ,payment_status
 ,payment_type
 ,attempts
 ,transaction_status AS status
 ,createddate
 ,payment_method_type AS payment_method
 ,failed_reason AS failed_reason
 ,'' AS paid_status
 ,'' AS id
 ,CASE 
   WHEN transaction_status = 'failed' 
    THEN updateddate
   ELSE NULL
  END AS last_failed_date
FROM billing_payments_all
)
,ct AS (
SELECT
  a.subscription__c AS subscription_id
 ,b.spree_order_number__c AS order_id
 ,a.id AS payment_id
 ,a.charges__c AS tries
 ,a.number__c::varchar AS payment_number
 ,a.tries_charge__c AS attempts
 ,a.payment_method__c AS payment_method
 ,a.date_due__c AS due_date
 ,a.charge_id__c AS psp_reference
 ,a.type__c AS payment_type
 ,a.status__c AS payment_status
 ,a.date_failed__c AS last_failed_date
 ,LEFT(a.message__c, CASE 
   WHEN POSITION(';' IN a.message__c) = 0 
    THEN 9999 ELSE POSITION(';' IN a.message__c) -1 
  END) AS failed_reason
 ,COALESCE((CASE 
   WHEN a.status__c::TEXT = 'PAID'::TEXT 
      AND a.amount_f_due__c <= 0::NUMERIC 
      AND COALESCE(a.amount_paid__c, 0::NUMERIC) = 0::NUMERIC
    THEN a.date_due__c
   WHEN a.status__c::TEXT = 'FAILED'::TEXT 
    THEN NULL::TIMESTAMP WITHOUT TIME ZONE 
  ELSE date_paid__c
 END), NULL) AS paid_date
 ,a.reason_paid__c AS paid_status
 ,JSON_PARSE(charges__c) AS json_str
FROM stg_salesforce.subscription_payment__c a
  LEFT JOIN stg_salesforce.order b 
    ON b.id = a.order__c
WHERE IS_VALID_JSON_ARRAY(charges__c)
)
,failed_payments AS (
SELECT
  a.subscription_id
 ,a.order_id
 ,a.payment_id
 ,a.attempts
 ,a.payment_number
 ,a.payment_method
 ,a.due_date
 ,a.psp_reference
 ,a.payment_type
 ,a.payment_status
 ,a.last_failed_date
 ,a.failed_reason
 ,a.paid_date
 ,a.paid_status
 ,cs.id::TEXT
 ,CASE
   WHEN a.paid_date = a.due_date 
    THEN a.paid_date::TIMESTAMP
   ELSE cs."date"::TIMESTAMP
  END AS createddate
 ,cs.status::TEXT
FROM ct a, a.json_str AS cs
WHERE cs.status::TEXT NOT IN ('submitted_for_settlement', 'Authorised', 'PENDING', 'authorization_expired', 'settling')
)
,succesful_payments AS (
SELECT
  a.subscription__c AS subscription_id
 ,b.spree_order_number__c AS order_id
 ,a.id AS payment_id
 ,a.tries_charge__c AS attempts
 ,a.number__c::varchar AS payment_number
 ,a.payment_method__c AS payment_method
 ,a.date_due__c AS due_date
 ,a.charge_id__c AS psp_reference
 ,a.type__c AS payment_type
 ,a.status__c AS payment_status
 ,a. date_failed__c AS last_failed_date
 ,LEFT(a.message__c, CASE WHEN POSITION(';' IN a.message__c) = 0 THEN 9999 ELSE POSITION(';' IN a.message__c) -1 END) AS failed_reason
 ,COALESCE((CASE
   WHEN a.status__c::TEXT = 'PAID'::TEXT
     AND a.amount_f_due__c <= 0::NUMERIC
     AND COALESCE(a.amount_paid__c, 0::NUMERIC) = 0::NUMERIC
    THEN date_due__c
   WHEN a.status__c::TEXT = 'FAILED'::TEXT 
    THEN NULL::TIMESTAMP WITHOUT TIME ZONE 
   ELSE date_paid__c
   END), NULL) AS paid_date
 ,a.reason_paid__c AS paid_status
 ,'' AS id
 ,CASE
   WHEN COALESCE((CASE
      WHEN a.status__c::TEXT = 'PAID'::TEXT 
        AND a.amount_f_due__c <= 0::NUMERIC 
        AND COALESCE(a.amount_paid__c, 0::NUMERIC) = 0::NUMERIC
       THEN date_due__c
      WHEN a.status__c::TEXT = 'FAILED'::TEXT
       THEN NULL::TIMESTAMP WITHOUT TIME ZONE 
      ELSE date_paid__c
     END), NULL) = a.date_due__c 
    THEN COALESCE((CASE
       WHEN a.status__c::TEXT = 'PAID'::TEXT
         AND a.amount_f_due__c <= 0::NUMERIC
         AND COALESCE(a.amount_paid__c, 0::NUMERIC) = 0::NUMERIC
        THEN date_due__c
       WHEN a.status__c::TEXT = 'FAILED'::TEXT 
        THEN NULL::TIMESTAMP WITHOUT TIME ZONE
       ELSE date_paid__c
     END), NULL)::TIMESTAMP
   ELSE a.date_due__c::TIMESTAMP
  END AS createddate
 ,'' AS status
FROM stg_salesforce.subscription_payment__c a
  LEFT JOIN stg_salesforce.order b
    ON b.id = a.order__c
WHERE payment_id NOT IN (SELECT payment_id FROM failed_payments)
)
,all_SF_payments AS (
SELECT *
FROM succesful_payments
UNION ALL
SELECT *
FROM failed_payments
)
,all_payments_final AS (
SELECT 
  subscription_id
 ,order_id
 ,payment_id
 ,payment_id AS group_id
 ,attempts
 ,payment_number
 ,payment_method
 ,due_date::TIMESTAMP
 ,last_failed_date::TIMESTAMP
 ,psp_reference
 ,payment_type
 ,payment_status
 ,failed_reason
 ,paid_date::TIMESTAMP
 ,paid_status
 ,id
 ,createddate::TIMESTAMP
 ,CASE
   WHEN status = ''
    THEN payment_status
   ELSE UPPER(status)
  END AS status
 ,payment_id || '_' || createddate::TIMESTAMP AS key_
 ,'salesforce' src_tbl
FROM all_SF_payments
WHERE TRUNC(due_date::DATE) BETWEEN '2023-01-01' AND CURRENT_DATE
  OR (TRUNC(createddate::DATE) BETWEEN '2023-01-01' AND CURRENT_DATE)
UNION ALL
SELECT
  subscription_id
 ,order_id
 ,payment_id
 ,group_id
 ,attempts
 ,payment_number::varchar
 ,payment_method
 ,due_date::TIMESTAMP
 ,last_failed_date::TIMESTAMP
 ,psp_reference
 ,payment_type
 ,payment_status
 ,failed_reason
 ,paid_date::TIMESTAMP
 ,paid_status
 ,id
 ,createddate::TIMESTAMP
 ,UPPER(status) AS status
 ,group_id || '_' || createddate::TIMESTAMP AS key_
 ,'billing service' AS src_tbl
FROM billing_payments_final
WHERE TRUNC(due_date::DATE) BETWEEN '2023-01-01' AND CURRENT_DATE
  OR (TRUNC(createddate::DATE) BETWEEN '2023-01-01' AND CURRENT_DATE)
)
SELECT 
  a.* 
 ,b.store_country
 ,b.new_recurring
 ,b.customer_type
 ,CASE
   WHEN UPPER(a.failed_reason) ILIKE '%CHARGEBACK%' 
    THEN 'Chargeback'
   WHEN UPPER(a.failed_reason) ILIKE '%FRAUD%' 
    THEN 'Suspected fraud'
   WHEN UPPER(a.failed_reason) ILIKE '%THE TRANSACTION DID NOT COMPLETE%' 
    THEN 'Insufficient funds'
   WHEN (a.payment_status IN ('FAILED', 'FAILED FULLY')
     OR a.status IN ('ERROR', 'FAILED', 'FAILED FULLY', 'GATEWAY_REJECTED', 'PROCESSOR_DECLINED', 'REFUSED'))
     AND a.failed_reason IS NULL 
    THEN 'Tech issue - NULL'
   ELSE NVL(c.failed_reason_grouped, 'N/A')
  END AS failed_reason_final
 ,ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY	createddate DESC) AS rn
FROM all_payments_final a
  LEFT JOIN master.order b 
    ON b.order_id = a.order_id
  LEFT JOIN staging_airbyte_bi.failed_reasons_grouping c
    ON a.failed_reason = c.failed_reason
WITH NO SCHEMA BINDING;

GRANT SELECT ON	dm_payments.v_failed_payments_all TO tableau;