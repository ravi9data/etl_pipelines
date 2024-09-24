BEGIN;

TRUNCATE TABLE ods_production.payment_subscription;

INSERT INTO ods_production.payment_subscription
-------------------------------------------------------------------------------
-----------------------------GENERAL CTES--------------------------------------
-------------------------------------------------------------------------------
WITH asset_return_date AS (
/*THIS WAS BEING USED ALREADY IN BILLING SERVICE
 * NOT SURE IF IT IS THE RIGHT APPROACH*/
 SELECT 
  COALESCE(s.subscription_bo_id, s.subscription_id) AS subscription_id
 ,MIN(ast.capital_source_name) AS capital_source_name
 ,MAX(allo.return_delivery_date) AS max_return_delivery_date
 ,MIN(COALESCE(allo.return_delivery_date, '1999-01-01')) AS min_return_delivery_date
 ,CASE
   WHEN min_return_delivery_date = '1999-01-01' 
    THEN NULL
   ELSE max_return_delivery_date
  END AS asset_return_date
FROM ods_production.allocation allo
  LEFT JOIN ods_production.asset ast 
    ON ast.asset_id = allo.asset_id
  LEFT JOIN ods_production.subscription s 
    ON allo.subscription_id = s.subscription_id
GROUP BY 1    
)
/* TO CAPTURE PAYMENT METHODS THAT ARE EITHER NULL OR HAVE AN ALPHANUMERIC VALUE IN THE BILLING SERVICE OR LEDGER */
,wallet_transaction AS (
SELECT DISTINCT
  a.external_id
 ,a.ixopay_uuid 
 ,b."option" AS pm 
FROM oltp_wallet.transaction a
  LEFT JOIN oltp_wallet.payment_method b 
    ON a.payment_method_id = b.id
)
-------------------------------------------------------------------------------
----------------------------OLD INFRA - SALESFORCE-----------------------------
-------------------------------------------------------------------------------
,salesforce_refund_dates AS (
SELECT
  subscription_payment__c AS subscription_payment_id
 ,MAX(GREATEST(lastmodifieddate, systemmodstamp)) AS updated_at
 ,SUM(CASE 
   WHEN type__c='REFUND' 
     AND status__c = 'PAID' 
    THEN  amount_refunded__c 
  END) AS refund_amount
 ,COUNT(DISTINCT CASE 
   WHEN type__c='REFUND' 
     AND status__c = 'PAID' 
    THEN id  
  END) AS refund_transactions
 ,MAX(CASE 
   WHEN type__c='REFUND' 
     AND status__c = 'PAID' 
    THEN date_paid__c 
  END) AS max_refund_date
 ,MIN(CASE 
   WHEN type__c='REFUND' 
     AND status__c = 'PAID' 
    THEN date_paid__c 
  END) AS min_refund_date
 ,SUM(CASE 
   WHEN type__c='CHARGE BACK' 
     AND status__c = 'PAID' 
    THEN amount_refunded__c 
  END) AS chargeback_amount
 ,COUNT(DISTINCT CASE 
   WHEN type__c='CHARGE BACK' 
     AND status__c = 'PAID' 
    THEN id  
  END) AS chargeback_transactions
 ,MAX(CASE 
   WHEN type__c='CHARGE BACK' 
     AND status__c = 'PAID' 
    THEN date_paid__c 
  END) AS max_chargeback_date
 ,MIN(CASE 
   WHEN type__c='CHARGE BACK' 
     AND status__c = 'PAID' 
    THEN date_paid__c 
  END) AS min_chargeback_date
FROM stg_salesforce.refund_payment__c
GROUP BY 1
)
,salesforce_subscription_payment_final AS (
SELECT DISTINCT
/*payment_group_id is only relevant from billing service and ledger. 
 * For SF, we simply populate the payment id since there is no payment group id*/
  sp.id AS payment_group_id
 ,sp.id AS subscription_payment_id
 ,sp.name AS subscription_payment_name --earlier it was 'subscription_payment_sfid'
 ,NULL::TEXT AS transaction_id 
 ,NULL::TEXT AS resource_id
 ,NULL::TEXT AS movement_id
 ,sp.charge_id__c AS psp_reference_id
 ,CAST(a.spree_customer_id__c AS int) AS customer_id
 ,o.spree_order_number__c AS order_id
 ,sp.asset__c AS asset_id
 ,sp.allocation__c AS allocation_id
 ,sp.subscription__c AS subscription_id
 ,sp.type__c AS payment_type
 ,sp.number__c::VARCHAR AS payment_number
 ,sp.createddate AS created_at
 ,GREATEST(sp.lastmodifieddate, sp.systemmodstamp
 ,o.lastmodifieddate, o.systemmodstamp
 ,a.lastmodifieddate, a.systemmodstamp
 ,aa.lastmodifieddate, aa.systemmodstamp
 ,s.lastmodifieddate, s.systemmodstamp
 ,r.updated_at
 ,ass.lastmodifieddate, ass.systemmodstamp
 ,cap.lastmodifieddate, cap.systemmodstamp) AS updated_at
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
 ,NULL::TIMESTAMP AS money_received_at
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
 ,COALESCE(sp.amount_f_vat__c, sp.amount_f_sales_tax__c) AS amount_tax
 ,COALESCE(sp.rate_vat__c, sp.rate_sales_tax__c) AS tax_rate
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
 ,s.dc_customer_contact_result__c
 ,sp.date_debt_collection_requested__c AS debt_collection_requested_date
 ,CASE 
   WHEN s.agency_for_dc_processing__c='PAIR Finance'
    THEN COALESCE(sp.date_debt_collection_handover__c,s.date_cancellation__c)
   ELSE sp.date_debt_collection_handover__c 
  END AS date_debt_collection_handover
 ,sp.f_debt_collection_status__c AS debt_collection_status
 ,sp.f_paid_debt_agency__c AS is_paid_to_debt_agency
 ,sp.date_debt_collection_not_recoverable__c AS debt_collection_non_recoverable_date
 ,r.refund_amount
 ,r.refund_transactions
 ,r.max_refund_date
 ,r.min_refund_date
 ,r.chargeback_amount
 ,r.chargeback_transactions
 ,r.max_chargeback_date
 ,r.min_chargeback_date
 ,sp.x1st_warning_sent_date__c AS x1st_warning_sent_date
 ,sp.x2nd_warning_sent_date__c AS x2nd_warning_sent_date
 ,sp.x3rd_warning_sent_date__c AS x3rd_warning_sent_date
 ,COALESCE(aa.return_delivered__c, aa.return_picked_by_carrier__c,aa.cancelltion_returned__c
 ,CASE 
   WHEN aa.status__c ='RETURNED' 
    THEN aa.subscription_cancellation__c 
  END) AS asset_return_date
 ,s.date_first_asset_delivery__c AS first_asset_delivery_date
 ,s.date_cancellation__c AS subsription_cancellation_date
 ,CASE
   WHEN acsd.asset_id IS NOT NULL 
     AND paid_date::DATE < acsd.capital_source_sold_date::DATE 
    THEN acsd.old_value
   WHEN acsd.asset_id IS NOT NULL 
     AND due_date::DATE <= acsd.capital_source_sold_date::DATE 
     AND paid_date IS NULL 
    THEN acsd.old_value
   WHEN acsd2.asset_id IS NOT NULL 
     AND paid_date::DATE <= acsd2.capital_source_sold_date::DATE 
    THEN acsd2.old_value
   WHEN acsd2.asset_id IS NOT NULL 
     AND due_date::DATE <= acsd2.capital_source_sold_date 
     AND paid_date IS NULL 
    THEN acsd2.old_value
   ELSE COALESCE(cap.name,ass.purchase_payment_method__c)
  END AS capital_source
 ,COALESCE(s1.country_name,o1.store_country) AS country_name
 ,'legacy' AS src_tbl
FROM stg_salesforce.subscription_payment__c AS sp
  LEFT JOIN stg_salesforce.order AS o 
    ON o.id = sp.order__c
  LEFT JOIN ods_production.subscription AS s1 
    ON sp.subscription__c = s1.subscription_id
  LEFT JOIN ods_production.order AS o1 
    ON o1.order_id = o.spree_order_number__c
  LEFT JOIN stg_salesforce.account AS a 
    ON a.id = sp.customer__c
  LEFT JOIN stg_salesforce.customer_asset_allocation__c aa 
    ON aa.id=sp.allocation__c
  LEFT JOIN stg_salesforce.subscription__c s 
    ON s.id=sp.subscription__c
  LEFT JOIN salesforce_refund_dates r 
    ON r.subscription_payment_id=sp.id
  LEFT JOIN stg_salesforce.asset ass 
    ON ass.id=sp.asset__c
  LEFT JOIN stg_salesforce.capital_source__c cap 
    ON ass.capital_source__c=cap.id
  LEFT JOIN dwh.asset_capitalsource_sold_date acsd 
    ON acsd.asset_id = sp.asset__c  
   AND acsd.capital_source_sold_date = '2021-08-01 00:01:00'
  LEFT JOIN dwh.asset_capitalsource_sold_date acsd2 
    ON acsd2.asset_id = sp.asset__c  
   AND acsd2.capital_source_sold_date = '2021-09-01 00:01:00'
WHERE TRUE 
  AND ((sp.allocation__c IS NOT NULL) OR ((sp.subscription__c!='') AND (sp.subscription__c IS NOT NULL) ))
  AND sp.status__c NOT IN ('CANCELLED')
)
-------------------------------------------------------------------------------
----------------------------NEW INFRA - BILLING SERVICE------------------------
-------------------------------------------------------------------------------
,legacy_payment_exclusion_list AS (
/*WE USE THIS SCRIPT TO EXCLUDE SF PAYMENTS FROM BILLING SERVICE*/
SELECT
  s.subscription_id__c || sp.number__c::TEXT AS uid
FROM stg_salesforce.subscription_payment__c sp
  LEFT JOIN stg_salesforce.subscription__c s 
    ON s.id = sp.subscription__c
WHERE TRUE 
  AND	s.subscription_id__c IS NOT NULL
  AND sp.status__c <> 'CANCELLED'
  AND uid IS NOT NULL
)
,billing_transactions AS (
SELECT 
  account_to AS group_uuid
 ,type
 ,amount
 ,status AS payment_status
 ,failed_reason
 ,account_to
 ,parent AS parent_account_uuid
 ,created_at AS transaction_date
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response,'paymentMethod'),'') AS payment_method
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(gateway_response,'gatewayResponse'),'paymentMethod'),'') AS payment_method2
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'referenceId') AS payment_method_reference_id
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'type') AS payment_method_type
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response,'transactionId'),'') AS payment_transaction_id
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response,'purchaseId'),'') AS payment_purchase_id
 ,ROW_NUMBER() OVER (PARTITION BY account_to ORDER BY created_at::TIMESTAMP DESC) idx
 ,COUNT(uuid) OVER (PARTITION BY account_to) AS attempts_to_pay
FROM oltp_billing.transaction
)
,billing_payments AS (
SELECT DISTINCT
  1 AS idx
 ,sp.createdat::TIMESTAMP AS createdat
 ,sp.updatedat::TIMESTAMP AS updatedat
 ,CASE 
   WHEN sp.paymentstatus = 'unknown' AND bt.payment_status = 'success' 
    THEN 'paid' 
   ELSE sp.paymentstatus 
  END AS event_name
 ,sp.uuid AS uuid
 ,sp."group" AS g_uuid
 ,contractid  AS contract_id
 ,SPLIT_PART(sp.paymentpurpose,';',1) AS payment_type
 ,contracttype AS contract_type
 ,JSON_SERIALIZE(sp.lineitems) AS line_items
 ,sp.ordernumber AS order_number
 ,sp.period::TEXT AS payment_period
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, 0),'price'), 'in_cents'),'')/100.00 AS price
 ,JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, 0),'price'), 'currency') AS price_currency
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, 0),'discount'), 'in_cents'),'')/100.00 AS discount_value
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items,0),'base_price'), 'in_cents'),'')/100.00 AS base_price
 ,o.customer_id::TEXT AS user_id
 ,sp.status AS status
 ,sp.duedate::TIMESTAMP AS due_date
 ,NULL::TEXT AS consolidated
 ,CASE
   WHEN bt.payment_method_type = 'pay-by-invoice' 
    THEN bt.payment_method_type
   WHEN bt.payment_method_type IS NULL and bt.payment_method IS NULL and bt.type = 'manual_booking' 
    THEN 'Manual'
   ELSE COALESCE(bt.payment_method, bt.payment_method2)
  END AS payment_method_type
 ,bt.payment_method_reference_id AS payment_method_reference_id
 ,COALESCE(bt.payment_transaction_id, bt.payment_purchase_id) AS transaction_id
 ,NULL::TEXT AS transaction_token
 ,bt.parent_account_uuid AS parent_account_uuid
 ,bt.account_to AS account_uuid
 ,bt.failed_reason
 ,NULL::TEXT AS billing_account_id
 ,bts.payment_group_shipping_in_cents /100.00 shipping_cost_price
 ,bts.payment_group_tax_currency AS tax_currency
 ,bts.payment_group_tax_rate
 ,'billing_service' AS src_tbl
 ,sp.billingdate::DATE AS billingdate
 ,CASE
   WHEN payment_status = 'success'
	   AND sp.status IN ('paid','manual_booking_summary','partial refund','refund') 
	  THEN bt.transaction_date
   WHEN sp.status IN ('paid','manual_booking_summary') 
	  THEN sp.updatedat::TIMESTAMP
   ELSE NULL
  END AS paid_date
 ,o.store_country AS country_name
 ,bt.attempts_to_pay::VARCHAR 
FROM oltp_billing.payment_order sp
  LEFT JOIN ods_production.order o 
    ON sp.ordernumber = o.order_id
  LEFT JOIN billing_transactions bt 
    ON bt.account_to = sp."group" 
   AND bt.idx = 1
  LEFT JOIN stg_curated.order_additional_info bts 
    ON sp.ordernumber = bts.order_id
   AND sp."group" = bts.payment_group_id    
WHERE TRUE 
  AND IS_VALID_JSON_ARRAY(JSON_SERIALIZE(sp.lineitems))
  AND sp.contractid||COALESCE(sp.period, 9999)::TEXT NOT IN 
    (SELECT uid FROM legacy_payment_exclusion_list) -- exclude legacy payments data
  AND NOT (sp.status = 'failed' AND sp."period" = 1)
)
,partial_paid_base AS (
SELECT 
  g_uuid AS payment_group_id 
 ,SUM(CASE
   WHEN tax_currency = 'USD' 
    THEN price * (1 + payment_group_tax_rate)::DECIMAL(10,2)
   ELSE price
  END) AS total_amount
FROM billing_payments
WHERE status = 'partial paid'
GROUP BY 1
) 
,sub_payment_excl AS (
SELECT *
FROM billing_payments
WHERE payment_type != ('purchase')
)
,refunds AS (
SELECT 
  uuid
 ,contract_id
 ,event_name
 ,price AS refund_amount
 ,updatedat
FROM billing_payments
WHERE status in ('refund','partial refund')
)
,refund_dates AS (
SELECT
  uuid
 ,contract_id
 ,MAX(CASE 
 	 WHEN event_name = 'refund' 
 	  THEN updatedat 
  END) AS max_refund_date
 ,MIN(CASE 
 	 WHEN event_name = 'refund' 
 	  THEN updatedat 
  END) AS min_refund_date
 ,MAX(CASE 
 	 WHEN event_name = 'partial refund' 
 	  THEN updatedat 
  END) AS partial_refund_date
 ,COUNT(*) AS refund_transactions
FROM refunds
GROUP BY 1,2
)
,sub_payment_idx AS (
SELECT 
  *
 ,CASE
   WHEN tax_currency = 'USD' 
    THEN (payment_group_tax_rate * price)::DECIMAL(10,2)
   ELSE ((payment_group_tax_rate * price)/(1 + payment_group_tax_rate))::DECIMAL(10,2)
  END AS vat_amount_fix
 ,DENSE_RANK() OVER (PARTITION BY uuid, src_tbl ORDER BY updatedat DESC) AS idx_payment
FROM sub_payment_excl
)
, partially_paid_ratio AS (
SELECT 
  pp.payment_group_id
 ,pp.total_amount
 ,SUM(bt.amount) AS paid_amount
 ,CASE 
   WHEN pp.total_amount = 0 
    THEN 0
	 WHEN paid_amount / pp.total_amount > 1
	  THEN 1 
	 ELSE paid_amount / pp.total_amount 
  END AS partial_payment_ratio
FROM billing_transactions bt
  INNER JOIN partial_paid_base pp
    ON bt.account_to = pp.payment_group_id
WHERE bt.payment_status = 'success'
GROUP BY 1,2
)
,sub_payment_dates AS (
SELECT
  uuid
 ,MAX(CASE
   WHEN event_name = 'new' or src_tbl = 'billing_service' 
    THEN createdat 
  END) AS created_at
 ,MIN(CASE
   WHEN event_name = 'new' or src_tbl = 'billing_service' 
    THEN billingdate 
  END) AS billing_period_start
 ,MAX(CASE
   WHEN event_name in ('paid','manual_booking_summary','partial refund','refund') 
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
, bo_id_duplicates AS (
/*THIS IS JUST A PATCH TO REMOVE DUPLICATES  
 * WHENEVER THERE ARE DUPLICATE SUB IDS ALLOCATED TO subscription_bo_id, WE ALLOCATE ONLY TO THE LATEST STARTED ONE
 * WE CAN REMOVE IT ONCE WE DO BI-7704 AND USE SIMPLY ods_production.subscription*/
SELECT 
  subscription_bo_id
 ,COUNT(DISTINCT subscription_id) nr_
 ,MAX(start_date) latest_sub_start_date
FROM ods_production.subscription s 
WHERE NULLIF(subscription_bo_id, '') IS NOT NULL
GROUP BY 1
HAVING nr_ > 1
)
,subscription_base AS (
SELECT 
  s.subscription_id 
 ,CASE 
 	 WHEN b.subscription_bo_id IS NOT NULL 
 	   AND s.start_date <> b.latest_sub_start_date
 	  THEN NULL
 	 ELSE s.subscription_bo_id  
  END AS subscription_bo_id  
 ,s.first_asset_delivery_date
 ,s.cancellation_date
 ,s.country_name
FROM ods_production.subscription s
  LEFT JOIN bo_id_duplicates b
    ON s.subscription_bo_id  = b.subscription_bo_id
)
,wallet_pm AS (
SELECT 
	ixopay_reference_id 
	,"option"  AS pm
FROM oltp_wallet.payment_method 
UNION
SELECT DISTINCT
	a.ixopay_uuid AS ixopay_reference_id
	,a.pm 
FROM wallet_transaction a
  INNER JOIN sub_payment_idx spi
    ON a.ixopay_uuid = spi.payment_method_reference_id
)
,billing_service_subscription_payment_final AS (
SELECT 
  sp.g_uuid AS payment_group_id
 ,sp.uuid AS subscription_payment_id
 ,NULL AS subscription_payment_name
 ,NULL::TEXT AS transaction_id 
 ,NULL::TEXT AS resource_id
 ,NULL::TEXT AS movement_id
 ,sp.payment_method_reference_id AS psp_reference_id
 ,sp.user_id::BIGINT AS customer_id
 ,sp.order_number AS order_id
 ,asm.asset_id AS asset_id
 ,asm.allocation_id AS allocation_id
 ,COALESCE(s.subscription_id, sp.contract_id) AS subscription_id
 ,UPPER(payment_type) AS payment_type
 ,sp.payment_period AS payment_number
 ,sd.created_at::TIMESTAMP AS created_at
 ,sd.updated_at::TIMESTAMP AS updated_at
 ,sd.billing_period_start::TIMESTAMP  AS billing_period_start
 ,DATEADD('DAY', -1 , DATEADD('MONTH', 1, sd.billing_period_start::TIMESTAMP)) AS billing_period_end
 ,CASE 
	 WHEN UPPER(sp.event_name) IN ('REFUND','PARTIAL REFUND') 
	   AND sp.paid_date IS NOT NULL 
		THEN 'PAID' 
	 ELSE UPPER(sp.event_name) 
  END AS status
 ,sp.due_date AS due_date
 ,sd.paid_date::TIMESTAMP AS paid_date
 ,NULL::TIMESTAMP AS money_received_at
 ,NULL AS paid_status
 ,sp.price_currency AS currency
 ,CASE 
   WHEN sp.payment_method_type IS NULL 
     OR LEN(sp.payment_method_type) = 36 
     OR sp.payment_method_type='Invalid PM' 
    THEN wp.pm 
   ELSE sp.payment_method_type 
  END AS payment_method
 ,NULL AS payment_method_detailed
 ,NULL AS payment_method_funding
 ,NULL AS payment_method_details
 ,bi.invoice_url
 ,bi.invoice_number_pdf AS invoice_number
 ,bi.invoice_date::TIMESTAMP AS invoice_date
 ,NULL::TIMESTAMP AS invoice_sent_date
 ,ROUND(sp.base_price::DECIMAL(22, 6), 2) AS amount_subscription
 ,ROUND(CASE
	 WHEN sp.price_currency = 'USD' 
	  THEN ROUND((sp.price + sp.vat_amount_fix)::DOUBLE PRECISION,2)
	 ELSE price::DOUBLE PRECISION
  END::DECIMAL(22, 6), 2) AS amount_due
 ,ROUND(CASE
	 WHEN sp.price_currency = 'USD' 
	  THEN sp.price::DOUBLE PRECISION
	 ELSE (sp.price - sp.vat_amount_fix)::DOUBLE PRECISION
  END::DECIMAL(22, 6), 2) AS amount_due_without_taxes
 ,ROUND(CASE
	 WHEN (sp.event_name IN ('paid','manual_booking_summary','refund','partial refund') OR sp.status IN ('refund', 'partial refund')) 
	   AND sp.price_currency = 'USD' 
	  THEN ROUND((sp.price + sp.vat_amount_fix)::DOUBLE PRECISION,2)
	 WHEN sp.event_name IN ('paid','manual_booking_summary','refund','partial refund')  OR sp.status IN ('refund', 'partial refund')
	 	   AND sp.price_currency = 'EUR' 
	  THEN sp.price::DOUBLE PRECISION
	 WHEN sp.status = 'partial paid'
	   AND sp.price_currency = 'USD' 
	  THEN ROUND(((sp.price + sp.vat_amount_fix) * ppr.partial_payment_ratio)::DOUBLE PRECISION,2)
	 WHEN sp.status = 'partial paid'
	   AND sp.price_currency = 'EUR' 
	  THEN ROUND((sp.price * ppr.partial_payment_ratio)::DOUBLE PRECISION,2)	  
	 ELSE 0::DOUBLE PRECISION
  END::DECIMAL(22, 6), 2) AS amount_paid
 ,0::DECIMAL(22, 2) AS amount_discount
 ,ROUND(CASE 
   WHEN (sp.discount_value::DOUBLE PRECISION) < 0 
    THEN sp.discount_value::DOUBLE PRECISION 
   ELSE (-1 * sp.discount_value::DOUBLE PRECISION) 
  END::DECIMAL(22, 6), 2) AS amount_voucher
 ,ROUND(sp.shipping_cost_price::DECIMAL(22, 6), 2) AS amount_shipment --To be checked
 ,NULL::DOUBLE PRECISION AS amount_overdue_fee
 ,ROUND(CASE
	 WHEN sp.price_currency = 'USD' 
	  THEN ROUND(sp.vat_amount_fix::DOUBLE PRECISION,2)
	 ELSE sp.vat_amount_fix::DOUBLE PRECISION
  END::DECIMAL(22, 6), 2) AS amount_tax
 ,sp.payment_group_tax_rate::DOUBLE PRECISION AS tax_rate
  /*WHY IS THIS SET TO FALSE ALWAYS?*/
 ,FALSE AS is_paid_manually
 ,NULL::TIMESTAMP AS held_date
 ,NULL AS held_reason
 ,sd.failed_date::TIMESTAMP AS failed_date
 ,sp.failed_reason
 ,NULL AS payment_processor_message
 ,sp.attempts_to_pay
 ,NULL::TIMESTAMP AS next_try_date
 ,sd.cancellation_date::TIMESTAMP AS cancellation_date
 ,NULL AS cancellation_reason
 ,NULL AS dc_customer_contact_result__c
 ,NULL::TIMESTAMP AS debt_collection_requested_date
 ,NULL::TIMESTAMP AS date_debt_collection_handover
 ,NULL::TEXT AS debt_collection_status
 ,FALSE AS is_paid_to_debt_agency
 ,NULL::TIMESTAMP AS debt_collection_non_recoverable_date
 ,ROUND(CASE
	 WHEN sp.price_currency = 'USD' 
	  THEN ROUND((refund_amount + sp.vat_amount_fix)::DOUBLE PRECISION,2)
	 ELSE refund_amount::DOUBLE PRECISION
  END::DECIMAL(22, 6), 2) AS refund_amount
 ,rd.refund_transactions AS refund_transactions
 ,rd.max_refund_date::TIMESTAMP AS max_refund_date
 ,rd.min_refund_date::TIMESTAMP AS min_refund_date
 ,NULL::DOUBLE PRECISION AS chargeback_amount
 ,NULL::BIGINT AS chargeback_transactions
 ,NULL::TIMESTAMP AS max_chargeback_date
 ,NULL::TIMESTAMP AS min_chargeback_date
 ,NULL::TIMESTAMP AS x1st_warning_sent_date
 ,NULL::TIMESTAMP AS x2nd_warning_sent_date
 ,NULL::TIMESTAMP AS x3rd_warning_sent_date
 ,ard.asset_return_date
 ,s.first_asset_delivery_date AS first_asset_delivery_date
 ,s.cancellation_date AS subsription_cancellation_date
 ,ard.capital_source_name AS capital_source
 ,COALESCE(sp.country_name, s.country_name) AS country_name
 ,sp.src_tbl
FROM sub_payment_idx sp
  LEFT JOIN sub_payment_dates sd
    ON sp.uuid = sd.uuid
  LEFT JOIN subscription_base s
    ON COALESCE(s.subscription_bo_id, s.subscription_id) = sp.contract_id
  LEFT JOIN refunds r
    ON r.uuid = sp.uuid 
   AND r.contract_id = sp.contract_id
  LEFT JOIN refund_dates rd
    ON rd.uuid = sp.uuid 
   AND rd.contract_id = sp.contract_id
  LEFT JOIN wallet_pm wp 
   ON sp.payment_method_reference_id = wp.ixopay_reference_id
  LEFT JOIN asset_return_date ard
    ON sp.contract_id = ard.subscription_id
  LEFT JOIN ods_production.asset_subscription_mapping asm
    ON sp.contract_id = asm.subscription_id_migrated
   AND asm.rx_last_allocation_per_sub = 1      
  LEFT JOIN ods_production.billing_invoices_clerk bi 
    ON sp.g_uuid = bi.payment_group_id
  LEFT JOIN partially_paid_ratio ppr
    ON sp.g_uuid = ppr.payment_group_id
WHERE idx_payment = 1 OR src_tbl = 'billing_service'	 
)
-------------------------------------------------------------------------------
----------------------------NEW INFRA - LEDGER---------------------------------
-------------------------------------------------------------------------------
,billing_service_exclusion_list AS (
/*WE WILL USE THIS LIST TO EXCLUDE PAYMENTS FROM LEDGER 
 *THAT ARE ALREADY AVAILABLE IN BILLING SERVICE*/
SELECT DISTINCT
  contractid AS subscription_id
  ,period AS payment_number
FROM oltp_billing.payment_order po 
WHERE TRUE 
  AND po.paymenttype = 'subscription'   
)
,ledger_refund_amount_and_dates AS (
SELECT 
  id 
 ,CASE latest_movement_status
   WHEN 'SUCCESS' 
    THEN latest_movement_created_at_timestamp
   ELSE NULL
  END AS refund_date 
 ,ROUND(value::DECIMAL(22, 6) ,2) AS refund_amount
 ,ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC, consumed_at DESC) rx 
FROM ods_production.ledger_curated
WHERE TRUE 
/*DOES IT MAKE SENSE TO ALSO CONSIDER PARTIAL REFUNDS HERE?*/
  AND status IN ('REFUNDED', 'PARTIALLY_REFUNDED') 
  AND type = 'subscription'
)
,ledger_combined_source AS (
SELECT
  external_id
 ,type
 ,id
 ,transaction_id
 ,metadata
 ,current_allocated_amount
 ,status
 ,transactions
 ,ROUND(value::DECIMAL(22, 6),2) AS value
 ,latest_movement
 ,slug
 ,ROUND(value_without_tax::DECIMAL(22, 6),2) AS value_without_tax
 ,country_iso
 ,consumed_at
 ,published_at
 ,resource_created_at
 ,tax_rate
 ,created_at
 ,currency
 ,origin
 ,current_allocated_taxes
 ,ROUND(tax_amount::DECIMAL(22, 6),2) AS tax_amount
 ,subscription_id
 ,latest_movement_created_at_timestamp
 ,latest_movement_clean
 ,provider_response_clean
 ,latest_movement_id
 ,psp_reference_id
 ,customer_id
 ,resource_created_at_timestamp
 ,created_at_timestamp
 ,latest_movement_status
 ,money_received_at_timestamp
 ,payment_method
 ,ROUND(amount_to_allocate::DECIMAL(22, 6),2) AS amount_to_allocate
 ,payment_number
 ,replay
 ,ROW_NUMBER() OVER (PARTITION BY transaction_id, slug ORDER BY created_at DESC, consumed_at DESC) rx
 ,COUNT(transaction_id) OVER (PARTITION BY slug) nr_of_transactions
FROM ods_production.ledger_curated
WHERE TRUE 
  AND "type" = 'subscription'
/*WE ARE ONLY INTERESTED IN THE SUBSCRIPTION PAYMENTS AND NOT THE REFUNDS*/
  AND latest_movement NOT LIKE '%REFUND%'
)
,attempts AS (
SELECT 
  id
 ,COUNT(*) AS attempts_to_pay  
FROM ledger_combined_source
WHERE latest_movement_status IN ('SUCCESS','FAILED')
	AND (replay IS NULL OR UPPER(replay)='FALSE')
GROUP BY 1
)
,discount_base AS (
SELECT 
  d1.*
 ,d2.period_number AS period_number_discount_removed 
FROM ods_production.subscription_discount_new_infra d1
  LEFT JOIN ods_production.subscription_discount_new_infra d2
    ON d1.subscription_id = d2.subscription_id 
   AND d1.discount_source = d2.discount_source 
   AND d1.discount_amount = d2.discount_amount
   AND d1.is_recurring = d2.is_recurring
   AND d2.event_name = 'discount_removed'
WHERE d1.event_name IN ('created', 'discount_applied')   
)
,discount_aggregated AS (
SELECT 
  subscription_id
 ,CASE discount_source
   WHEN 'voucher'
    THEN 'voucher'
   ELSE 'other'
  END discount_type
 ,period_number
 ,CASE 
 	 WHEN is_recurring IS FALSE 
 	  THEN period_number + 1
 	 WHEN is_recurring IS TRUE 
 	   THEN COALESCE(period_number_discount_removed, 9999)
 END AS  period_number_discount_removed
 ,is_recurring
 ,ROUND(SUM(discount_amount)::DECIMAL(22, 6),2) discount_amount
FROM discount_base
WHERE TRUE 
/*IF A DISCOUNT IS REMOVED IN THE SAME PERIOD, WE EXCLUDE IT*/
  AND period_number <> COALESCE(period_number_discount_removed, 9999)
GROUP BY 1,2,3,4,5
)
,ledger_subscription_payment_final AS (
SELECT 
/*CHECK IF THIS WILL BE THE GROUP ID*/
  lcs.latest_movement_id::TEXT AS payment_group_id
 ,lcs.slug AS subscription_payment_id
 ,NULL::TEXT AS subscription_payment_name
 ,lcs.transaction_id
 ,lcs.id AS resource_id
 ,lcs.latest_movement_id
 ,lcs.psp_reference_id
 ,lcs.customer_id
 ,s.order_id
 ,asm.asset_id
 ,asm.allocation_id
 ,COALESCE(s.subscription_id, lcs.subscription_id) AS subscription_id
 ,CASE lcs.payment_number::INT
   WHEN 1
    THEN 'FIRST'
   ELSE 'RECURRENT'
 END AS payment_type
 ,lcs.payment_number::TEXT AS payment_number
 ,lcs.resource_created_at_timestamp AS created_at
 ,lcs.created_at_timestamp AS updated_at
 ,bi.billing_period_start::TIMESTAMP AS billing_period_start
 ,DATEADD('DAY', -1 , DATEADD('month', 1, bi.billing_period_start::TIMESTAMP)) AS billing_period_end
 ,CASE lcs.latest_movement_status 
   WHEN 'SUCCESS' 
    THEN 'PAID'
   WHEN 'RUNNING' 
    THEN 'PENDING'
   WHEN 'FAILED' 
    THEN 'FAILED' 
   ELSE 'OTHER'
  END AS status
 ,lcs.resource_created_at_timestamp AS due_date
 ,CASE lcs.latest_movement_status 
   WHEN 'SUCCESS' 
    THEN lcs.latest_movement_created_at_timestamp
   ELSE NULL
  END AS paid_date
 ,lcs.money_received_at_timestamp AS money_received_at 
 ,NULL::TEXT AS paid_status
 ,lcs.currency
 ,COALESCE(lcs.payment_method, wt.pm) AS payment_method
 ,NULL::TEXT AS payment_method_detailed
 ,NULL::TEXT AS payment_method_funding
 ,NULL::TEXT AS payment_method_details
 ,bi.invoice_url
 ,bi.invoice_number_pdf AS invoice_number
 ,bi.invoice_date::TIMESTAMP  
 ,NULL::TIMESTAMP AS invoice_sent_date
 ,CASE lcs.currency
   WHEN 'USD'
    THEN lcs.value_without_tax 
   ELSE value 
  END::DOUBLE PRECISION AS amount_subscription
 ,lcs.value::DOUBLE PRECISION AS amount_due 
 ,lcs.value_without_tax::DOUBLE PRECISION AS amount_due_without_taxes
 ,CASE lcs.latest_movement_status 
   WHEN 'SUCCESS' 
    THEN lcs.amount_to_allocate
   ELSE '0'
  END::DOUBLE PRECISION AS amount_paid
  /*WE SHOULD INPUT THIS*/
 ,discount.discount_amount::DOUBLE PRECISION AS amount_discount
  /*WE SHOULD INPUT THIS*/
 ,voucher.discount_amount::DOUBLE PRECISION AS amount_voucher
  /*WE SHOULD IDEALLY REMOVE THIS COLUMN COMPLETELY
   * FROM THIS TABLE*/
 ,0::DOUBLE PRECISION AS amount_shipment
 ,0::DOUBLE PRECISION  AS amount_overdue_fee 
 ,lcs.tax_amount::DOUBLE PRECISION AS amount_tax
 ,lcs.tax_rate::DOUBLE PRECISION AS tax_rate
  /*WE SHOULD IDEALLY INPUT THIS*/
 ,FALSE AS is_paid_manually
 ,NULL::TIMESTAMP AS held_date
 ,NULL AS held_reason
 ,fp.failed_date  
 ,fp.failed_reason 
 ,NULL AS payment_processor_message  
 ,c.attempts_to_pay::TEXT
 ,NULL::TIMESTAMP AS next_try_date
  /*WE SHOULD IDEALLY INPUT THIS*/
 ,NULL::TIMESTAMP AS cancelled_date
   /*WE SHOULD IDEALLY INPUT THIS*/
 ,NULL AS cancellation_reason
 ,NULL AS dc_customer_contact_result__c
 ,NULL::TIMESTAMP AS debt_collection_requested_date
 ,NULL::TIMESTAMP AS date_debt_collection_handover
 ,NULL::TEXT AS debt_collection_status
 ,FALSE AS is_paid_to_debt_agency
 ,NULL::TIMESTAMP AS debt_collection_non_recoverable_date
 ,lrd.refund_amount::DOUBLE PRECISION 
 ,NULL::BIGINT AS refund_transactions
 ,NULL::TIMESTAMP AS max_refund_date
 ,NULL::TIMESTAMP AS min_refund_date
 ,NULL::DOUBLE PRECISION AS chargeback_amount
 ,NULL::BIGINT AS chargeback_transactions
 ,NULL::TIMESTAMP AS max_chargeback_date
 ,NULL::TIMESTAMP AS min_chargeback_date
 ,NULL::TIMESTAMP AS x1st_warning_sent_date
 ,NULL::TIMESTAMP AS x2nd_warning_sent_date
 ,NULL::TIMESTAMP AS x3rd_warning_sent_date
 ,ard.asset_return_date
 ,s.first_asset_delivery_date
 ,s.cancellation_date AS subsription_cancellation_date
 ,asm.capital_source
 ,COALESCE(CASE lcs.country_iso
   WHEN 'DE'
    THEN 'Germany'
   WHEN 'AT'
    THEN 'Austria'
   WHEN 'ES'
    THEN 'Spain' 
   WHEN 'NL'
    THEN 'Netherlands'
   WHEN 'US'
    THEN 'United States'
   ELSE NULL 
 	 END, s.country_name) AS country_name 
 ,'ledger'::TEXT AS src_tbl
FROM ledger_combined_source lcs 
  LEFT JOIN ods_production.asset_subscription_mapping asm
    ON lcs.subscription_id = asm.subscription_id_migrated
   AND asm.rx_last_allocation_per_sub = 1 
  LEFT JOIN ods_production.subscription s 
    ON lcs.subscription_id = COALESCE(s.subscription_bo_id, s.subscription_id)
  LEFT JOIN ledger_refund_amount_and_dates lrd
    ON lcs.id = lrd.id
   AND lrd.rx = 1
  LEFT JOIN ods_production.ledger_failed_payment_reason fp
    ON lcs.id = fp.id
   AND fp.rn = 1 
   /*ACCORDING TO PROD ENGINEERING INVOICE WILL APPEAR IN THIS SOURCE
     WE JUST NEED TO FIGURE OUT THE KEY TO JOIN*/
  LEFT JOIN ods_production.billing_invoices_clerk bi
    ON lcs.latest_movement_id = bi.movement_id
  LEFT JOIN attempts c 
    ON lcs.id = c.id
  LEFT JOIN asset_return_date ard
    ON lcs.subscription_id = ard.subscription_id  
  LEFT JOIN discount_aggregated voucher 
    ON lcs.subscription_id = voucher.subscription_id
    AND lcs.payment_number >= voucher.period_number
    AND lcs.payment_number < voucher.period_number_discount_removed
    AND voucher.discount_type = 'voucher'
  LEFT JOIN discount_aggregated discount 
    ON lcs.subscription_id = discount.subscription_id
    AND lcs.payment_number >= discount.period_number
    AND lcs.payment_number < discount.period_number_discount_removed    
    AND voucher.discount_type = 'other'
  LEFT JOIN billing_service_exclusion_list excl
    ON lcs.subscription_id = excl.subscription_id
   AND lcs.payment_number = excl.payment_number
  LEFT JOIN wallet_transaction wt
    ON lcs.latest_movement_id = wt.external_id
WHERE TRUE 
  AND CASE
	 WHEN lcs.nr_of_transactions = 0
	  THEN lcs.rx = 1
	 ELSE lcs.transaction_id IS NOT NULL 
	  AND lcs.rx = 1
	END 
  AND excl.subscription_id IS NULL 
  AND NOT (lcs.latest_movement_status = 'FAILED' AND lcs.payment_number::int = 1)
)
-------------------------------------------------------------------------------
----------------------------UNION SOURCES--------------------------------------
-------------------------------------------------------------------------------
SELECT * 
FROM salesforce_subscription_payment_final
  UNION ALL
SELECT * 
FROM billing_service_subscription_payment_final
  UNION ALL
SELECT * 
FROM ledger_subscription_payment_final
;

COMMIT;