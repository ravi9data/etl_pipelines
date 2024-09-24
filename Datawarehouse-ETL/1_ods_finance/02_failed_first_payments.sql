DROP TABLE IF EXISTS stg_bs_first_subscription_payment;
CREATE TEMP TABLE stg_bs_first_subscription_payment
SORTKEY(subscription_payment_id)
DISTKEY(subscription_id) AS 
WITH legacy_payments AS (
SELECT
	b.subscription_id__c || a.number__c::TEXT AS uid
FROM stg_salesforce.subscription_payment__c a
LEFT JOIN stg_salesforce.subscription__c b ON a.subscription__c= b.id
WHERE TRUE 
	AND a.status__c= 'FAILED'
	AND a.type__c='FIRST'
    AND uid IS NOT NULL
), 
billing_transactions AS (
SELECT 
  account_to AS group_uuid
 ,type
 ,amount
 ,status AS payment_status
 ,failed_reason
 ,account_to
 ,created_at AS transaction_date
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response,'paymentMethod'), '') AS payment_method
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(gateway_response,'gatewayResponse'),'paymentMethod'),'') AS payment_method2
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'referenceId') AS payment_method_reference_id
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'type') AS payment_method_type
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response,'transactionId'),'') AS payment_transaction_id
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response,'purchaseId'),'') AS payment_purchase_id
 ,ROW_NUMBER() OVER (PARTITION BY account_to ORDER BY created_at::TIMESTAMP DESC) idx
 ,COUNT(uuid) OVER (PARTITION BY account_to) AS attempts_to_pay
FROM oltp_billing.transaction
)
,order_latest_tax_data AS (
SELECT
  order_id
 ,payment_group_tax_rate
 ,payment_group_tax_currency
 ,payment_group_shipping_in_cents
 ,ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY max_updated_at DESC) AS rn 
FROM stg_curated.order_additional_info
)
,billing_payments AS (
SELECT DISTINCT
  1 AS idx 
 ,sp.createdat::TIMESTAMP AS createdat 
 ,sp.updatedat::TIMESTAMP AS updatedat 
 ,CASE 
   WHEN sp.paymentstatus = 'unknown' 
      AND bt.payment_status = 'success'  
	THEN 'paid' 
   ELSE sp.paymentstatus 
  END AS event_name
 ,sp.uuid AS subscription_payment_id
 ,sp."group" AS g_uuid
 ,sp.contractid  AS subscription_id
 ,split_part(sp.paymentpurpose,';',1) AS payment_type
 ,sp.contracttype AS contract_type
 ,NULL::TEXT AS entity_type
 ,JSON_ARRAY_LENGTH(JSON_SERIALIZE(sp.lineitems)) AS total_order_items
 ,JSON_SERIALIZE(sp.lineitems) AS line_items
 ,sp.ordernumber AS order_id
 ,JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items,0),'period') AS payment_period
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, 0),'price'), 'in_cents'),'')/100.00 AS price
 ,JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items,0),'price'), 'currency') AS price_currency
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items,0),'discount'), 'in_cents'),'')/100.00 AS discount_value
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items,0),'base_price'), 'in_cents'),'')/100.00 AS base_price
 ,o.customer_id
 ,sp.status AS status
 ,sp.duedate::timestamp AS due_date
 ,NULL::TEXT AS consolidated
 ,CASE
   WHEN bt.payment_method_type = 'pay-by-invoice'
    THEN bt.payment_method_type
   WHEN bt.payment_method_type IS NULL 
      AND bt.payment_method IS NULL 
	  AND bt.type = 'manual_booking'
	THEN 'Manual'
   ELSE COALESCE(bt.payment_method, bt.payment_method2)
  END AS payment_method_type
 ,sp.currency AS payment_method_currency
 ,NULL::TEXT AS payment_method_gateway
 ,bt.payment_method_reference_id AS payment_method_reference_id
 ,COALESCE(payment_transaction_id, payment_purchase_id) AS transaction_id
 ,NULL::TEXT AS transaction_token
 ,bt.account_to AS account_uuid
 ,bt.failed_reason
 ,NULL::TEXT AS billing_account_id
 ,base_price - discount_value AS amount_due_value
 ,COALESCE(bts.payment_group_shipping_in_cents, olt.payment_group_shipping_in_cents)  /100.00 shipping_cost_price
 ,COALESCE(bts.payment_group_tax_currency, olt.payment_group_tax_currency) AS tax_currency
 ,COALESCE(bts.payment_group_tax_rate, olt.payment_group_tax_rate) AS payment_group_tax_rate
 ,sp.billingdate::DATE AS billingdate
 ,CASE
   WHEN payment_status = 'success' 
     AND sp.status IN ('paid','manual_booking_summary','partial refund','refund')  
	THEN bt.transaction_date
   WHEN sp.status IN ('paid','manual_booking_summary') 
    THEN sp.updatedat::timestamp
   ELSE NULL
  END AS paid_date
 ,o.store_country AS country_name
 ,bt.attempts_to_pay::VARCHAR
 ,'billing_service' as src_tbl
FROM oltp_billing.payment_order sp
  LEFT JOIN ods_production.order o
    ON sp.ordernumber = o.order_id
  LEFT JOIN billing_transactions bt
    ON bt.account_to = sp."group" 
   AND bt.idx = 1
  LEFT JOIN stg_curated.order_additional_info bts
    ON sp.ordernumber = bts.order_id
   AND sp."group" = bts.payment_group_id
  LEFT JOIN order_latest_tax_data olt
    ON bts.order_id = olt.order_id 
WHERE TRUE 
  AND IS_VALID_JSON_ARRAY(JSON_SERIALIZE(sp.lineitems))
  AND sp.contractid||sp.period::TEXT NOT IN (SELECT uid FROM legacy_payments) -- exclude legacy payments data
  AND sp.status = 'failed' 
  AND sp."period" = 1 
),
parsing2 AS (
SELECT * 
FROM billing_payments
)
,sub_payment_excl AS (
SELECT *
FROM parsing2
WHERE payment_type != ('purchase')
)
,sub_payment_idx AS (
SELECT 
 *
 ,CASE
   WHEN tax_currency = 'USD' 
    THEN (payment_group_tax_rate * price)::DECIMAL(10,2)
   ELSE ((payment_group_tax_rate * price)/(1 + payment_group_tax_rate))::DECIMAL(10,2)
  END AS vat_amount_fix
 ,DENSE_RANK() OVER (PARTITION BY subscription_payment_id, src_tbl ORDER BY updatedat DESC) AS idx_payment
FROM sub_payment_excl
),
sub_payment_dates AS (
SELECT
 subscription_payment_id
 ,MAX(CASE
   WHEN event_name = 'new' 
     OR src_tbl = 'billing_service'
	THEN createdat 
 END) AS created_at
 ,MIN(CASE
   WHEN event_name = 'new' 
     OR src_tbl = 'billing_service'
	THEN billingdate 
  END) AS billing_period_start
 ,MAX(CASE
   WHEN event_name IN ('paid','manual_booking_summary','partial refund','refund')
    THEN paid_date 
  END) AS paid_date
 ,MAX(CASE
   WHEN event_name = 'failed' 
    THEN updatedat 
  END) AS failed_date
 ,MAX(CASE
   WHEN event_name = 'cancelled'
    THEN updatedat 
  END) AS cancellation_date
 ,MAX(updatedat) AS updated_at
FROM sub_payment_idx
GROUP BY 1
)
,sp_final AS (
SELECT 
  sp.g_uuid AS payment_group_id
 ,sp.subscription_payment_id
 ,NULL AS subscription_payment_name
 ,sp.payment_method_reference_id AS psp_reference_id
 ,sp.customer_id
 ,sp.order_id
 ,sp.subscription_id
 ,UPPER(sp.payment_type) AS payment_type
 ,sp.payment_period AS payment_number
 ,sd.created_at::TIMESTAMP AS created_at
 ,sd.updated_at::TIMESTAMP AS updated_at
 ,sd.billing_period_start::TIMESTAMP  AS billing_period_start
 ,DATEADD('DAY', -1, DATEADD('MONTH', 1 , sd.billing_period_start::TIMESTAMP)) AS billing_period_end
 ,CASE 
   WHEN UPPER(sp.event_name) IN ('REFUND','PARTIAL REFUND') 
     AND sp.paid_date IS NOT NULL 
	THEN 'PAID' 
   ELSE UPPER(sp.event_name) 
  END AS status
 ,sp.due_date AS due_date
 ,sd.paid_date::TIMESTAMP AS paid_date
 ,NULL AS paid_status
 ,sp.price_currency AS currency
 ,sp.payment_method_type AS payment_method
 ,NULL AS payment_method_detailed
 ,NULL AS payment_method_funding
 ,NULL AS payment_method_details
 ,bi.invoice_url
 ,bi.invoice_number_pdf AS invoice_number
 ,bi.invoice_date::TIMESTAMP AS invoice_date
 ,NULL::TIMESTAMP AS invoice_sent_date
 ,sp.base_price::DOUBLE PRECISION AS amount_subscription
 ,CASE
   WHEN sp.price_currency = 'USD' 
    THEN round((sp.price + sp.vat_amount_fix)::DOUBLE PRECISION,2)
   ELSE sp.price::DOUBLE PRECISION
  END AS amount_due
 ,CASE
   WHEN sp.price_currency = 'USD'
    THEN sp.price::DOUBLE PRECISION
   ELSE (sp.price - sp.vat_amount_fix)::DOUBLE PRECISION
  END AS amount_due_without_taxes
 ,CASE
   WHEN sp.event_name in ('paid','manual_booking_summary') 
     AND sp.price_currency = 'USD' 
	THEN round((sp.price + sp.vat_amount_fix)::DOUBLE PRECISION,2)
   WHEN sp.event_name in ('paid','manual_booking_summary','refund','partial refund')
    THEN sp.price::DOUBLE PRECISION
   ELSE 0::DOUBLE PRECISION
  END AS amount_paid
 ,0::DOUBLE PRECISION AS  amount_discount
 ,CASE 
   WHEN (sp.discount_value::DOUBLE PRECISION) < 0
    THEN sp.discount_value::DOUBLE PRECISION 
   ELSE (-1 * sp.discount_value::DOUBLE PRECISION) 
  END AS amount_voucher
 ,sp.shipping_cost_price::DOUBLE PRECISION AS amount_shipment --To be checked
 ,NULL::DOUBLE PRECISION AS amount_overdue_fee
 ,CASE
   WHEN sp.price_currency = 'USD'
    THEN NULL::DOUBLE PRECISION
   ELSE sp.vat_amount_fix::DOUBLE PRECISION
 END AS amount_vat_tax
 ,CASE
   WHEN sp.price_currency = 'USD'
    THEN NULL::DOUBLE PRECISION
   ELSE sp.payment_group_tax_rate::DOUBLE PRECISION
 END AS vat_tax_rate
 ,CASE
   WHEN sp.price_currency = 'USD'
    THEN round(sp.vat_amount_fix::DOUBLE PRECISION,2)
   ELSE NULL::DOUBLE PRECISION
 END AS amount_sales_tax
 ,CASE
   WHEN sp.price_currency = 'USD'
    THEN sp.payment_group_tax_rate::DOUBLE PRECISION
   ELSE NULL::DOUBLE PRECISION
 END AS sales_tax_rate
 ,FALSE AS is_paid_manually
 ,NULL::TIMESTAMP AS held_date
 ,NULL AS held_reason
 ,sd.failed_date::TIMESTAMP AS failed_date
 ,sp.failed_reason AS payment_processor_message
 ,sp.attempts_to_pay
 ,NULL::TIMESTAMP AS next_try_date
 ,sd.cancellation_date::TIMESTAMP AS cancellation_date
 ,NULL AS cancellation_reason
 ,s.cancellation_date AS subsription_cancellation_date
 ,COALESCE(sp.country_name, s.country_name) AS country_name
 ,sp.src_tbl
FROM sub_payment_idx sp
  LEFT JOIN sub_payment_dates sd
    ON sp.subscription_payment_id = sd.subscription_payment_id
  LEFT JOIN ods_production.subscription s
    ON s.subscription_id = sp.subscription_id
  LEFT JOIN ods_production.billing_invoices_clerk bi
    ON bi.payment_group_id = sp.g_uuid
WHERE idx_payment = 1 OR src_tbl = 'billing_service'
)
SELECT *  
FROM sp_final 
;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS stg_legacy_first_subscription_payment;
CREATE TEMP TABLE stg_legacy_first_subscription_payment 
	SORTKEY(subscription_payment_id)
	DISTKEY(subscription_payment_id)
AS
SELECT DISTINCT 
  sp.id AS subscription_payment_id
 ,sp.name AS subscription_payment_name --earlier it was 'subscription_payment_sfid'
 ,sp.charge_id__c AS psp_reference_id
 ,CAST(a.spree_customer_id__c AS int) AS customer_id
 ,o.spree_order_number__c AS order_id
 ,sp.asset__c AS asset_id
 ,sp.allocation__c AS allocation_id
 ,sp.subscription__c AS subscription_id
 ,sp.type__c AS payment_type
 ,sp.number__c::VARCHAR AS payment_number
 ,sp.createddate AS created_at
 ,sp.date_billing_start__c AS billing_period_start
 ,sp.date_billing_end__c AS billing_period_end
 ,CASE
   WHEN sp.message__c::TEXT = 'Agreement was canceled'::TEXT 
    THEN sp.status__c
   ELSE sp.status__c
 END AS status
 ,CASE 
   WHEN o.spree_order_number__c ='R962588649'
     AND sp.number__c >1 
    THEN sp.date_due__c::DATE+45 
   ELSE sp.date_due__c 
  END AS due_date
 ,COALESCE((CASE 
   WHEN sp.status__c::TEXT = 'PAID'::TEXT
     AND sp.amount_f_due__c <= 0::NUMERIC
	 AND COALESCE(sp.amount_paid__c, 0::NUMERIC) = 0::NUMERIC 
    THEN sp.date_due__c
   WHEN sp.status__c::TEXT = 'FAILED'::TEXT 
    THEN NULL::TIMESTAMP WITHOUT TIME ZONE
   ELSE sp.date_paid__c
  END), NULL) AS paid_date
 ,sp.reason_paid__c AS paid_status
 ,sp.currency__c AS currency
 ,sp.payment_method__c AS payment_method
 ,CASE
   WHEN sp.f_paid_manually__c 
    THEN 'Manual'::CHARACTER VARYING
   WHEN sp.payment_method__c::TEXT = 'AdyenContract'::TEXT
     AND btrim(sp.payment_method_details__c::TEXT) = 'sepadirectdebit'::TEXT
	THEN 'Adyen, SEPA'::CHARACTER VARYING
   WHEN sp.payment_method__c::TEXT = 'AdyenContract'::TEXT
    THEN 'Adyen, Other'::CHARACTER VARYING
   WHEN sp.payment_method__c::TEXT = 'Adyen'::TEXT
    THEN 'Adyen, 1-click'::CHARACTER VARYING
   WHEN COALESCE(sp.amount_f_due__c, 0::NUMERIC) = 0::NUMERIC
    THEN 'Amount Due = 0'::CHARACTER VARYING
   ELSE COALESCE(sp.payment_method__c, 'Not Available'::CHARACTER VARYING)
  END::CHARACTER VARYING(256) AS payment_method_detailed
 ,sp.payment_method_funding__c AS payment_method_funding
 ,sp.payment_method_details__c AS payment_method_details
 ,sp.invoice_url__c AS invoice_url
 ,sp.invoice_number__c AS invoice_number
 ,sp.date_invoiced__c AS invoice_date
 ,sp.date_invoice_sent__c AS invoice_sent_date
 ,sp.amount_subscription__c AS amount_subscription
 ,COALESCE(sp.amount_f_due__c,NULL) AS amount_due
 ,sp.amount_f_due_without_taxes__c AS amount_due_without_taxes
 ,COALESCE(sp.amount_paid__c,NULL) AS amount_paid
 ,sp.amount_discount__c AS amount_discount
 ,sp.amount_voucher__c AS amount_voucher
 ,sp.amount_shipment__c AS amount_shipment
 ,sp.overdue_fee_amount__c AS amount_overdue_fee
 ,sp.amount_f_vat__c AS amount_vat_tax
 ,sp.rate_vat__c AS vat_tax_rate
 ,sp.amount_f_sales_tax__c AS amount_sales_tax
 ,sp.rate_sales_tax__c AS sales_tax_rate
 ,sp.f_paid_manually__c AS is_paid_manually
 ,sp.date_held__c AS held_date
 ,sp.reason_held__c AS held_reason
 ,sp.date_failed__c AS failed_date
 ,sp.reason_failed__c AS failed_reason
 ,sp.message__c AS payment_processor_message --earlier it was 'failed_message'
 ,sp.tries_charge__c::VARCHAR AS attempts_to_pay
 ,sp.date_try_next__c AS next_try_date
 ,sp.date_cancel__c AS cancellation_date
 ,sp.reason_cancel__c AS cancellation_reason
 ,'legacy' as src_tbl
FROM stg_salesforce.subscription_payment__c AS sp
  LEFT JOIN stg_salesforce.order AS o 
    ON o.id = sp.order__c
  LEFT JOIN stg_salesforce.account AS a 
    ON a.id = sp.customer__c
  LEFT JOIN stg_salesforce.customer_asset_allocation__c aa 
    ON aa.id=sp.allocation__c
  LEFT JOIN stg_salesforce.subscription__c s 
    ON s.id=sp.subscription__c    
WHERE TRUE
  AND sp.type__c='FIRST' 
  AND sp.status__c IN ('FAILED','FAILED FULLY')    
;
	

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------	

DROP TABLE IF EXISTS ods_payments.all_failed_first_payments;
CREATE TABLE ods_payments.all_failed_first_payments AS
SELECT 
  payment_group_id
 ,subscription_payment_id
 ,subscription_payment_name
 ,psp_reference_id
 ,customer_id
 ,order_id
 ,subscription_id
 ,payment_type
 ,payment_number
 ,created_at
 ,status
 ,due_date
 ,paid_date
 ,currency
 ,payment_method
 ,payment_method_detailed
 ,invoice_number
 ,amount_subscription
 ,amount_due
 ,amount_due_without_taxes
 ,amount_discount
 ,amount_voucher
 ,COALESCE( amount_vat_tax,  amount_sales_tax) AS tax_amount
 ,COALESCE(vat_tax_rate,sales_tax_rate) AS tax_rate
 ,failed_date
 ,payment_processor_message
 ,payment_processor_message AS  payment_processor_message_clean
 ,attempts_to_pay
 ,next_try_date
 ,src_tbl
FROM stg_bs_first_subscription_payment
UNION ALL	
SELECT 
  subscription_payment_id AS payment_group_id
 ,subscription_payment_id
 ,subscription_payment_name
 ,psp_reference_id
 ,customer_id
 ,order_id
 ,subscription_id
 ,payment_type
 ,payment_number
 ,created_at
 ,status
 ,due_date
 ,paid_date
 ,currency
 ,payment_method
 ,payment_method_detailed
 ,invoice_number
 ,amount_subscription
 ,amount_due
 ,amount_due_without_taxes
 ,amount_discount
 ,amount_voucher
 ,COALESCE( amount_vat_tax,  amount_sales_tax) AS tax_amount
 ,COALESCE(vat_tax_rate, sales_tax_rate) AS tax_rate
 ,failed_date
 ,payment_processor_message
 ,LEFT(payment_processor_message, CASE 
   WHEN POSITION(';' IN payment_processor_message) = 0 
    THEN 9999 
   ELSE POSITION(';' IN payment_processor_message) -1  
  END) AS payment_processor_message_clean
 ,attempts_to_pay
 ,next_try_date
 ,src_tbl
FROM stg_legacy_first_subscription_payment;
   
GRANT SELECT ON ods_payments.all_failed_first_payments TO marcel_weber;
GRANT SELECT ON ods_payments.all_failed_first_payments TO group risk_manual_review;
GRANT SELECT ON ods_payments.all_failed_first_payments TO tableau;