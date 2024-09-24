begin;

truncate table ods_production.payment_asset;

insert into ods_production.payment_asset

-------------------------------------------------------------------------------
-----------------------------GENERAL CTES--------------------------------------
-------------------------------------------------------------------------------
	/* TO CAPTURE PAYMENT METHODS THAT ARE EITHER NULL OR HAVE AN ALPHANUMERIC VALUE 
  IN THE BILLING SERVICE OR LEDGER */
WITH wallet_transaction AS (
SELECT 
  a.external_id
 ,a.ixopay_uuid 
 ,b."option" AS pm 
FROM oltp_wallet."transaction" a
  LEFT JOIN oltp_wallet.payment_method b 
    ON a.payment_method_id = b.id
)
-------------------------------------------------------------------------------
------------------------OLD INFRA - SALESFORCE---------------------------------
-------------------------------------------------------------------------------
,refund_old_infra AS (
SELECT
  asset_payment__c AS asset_payment_id
 ,SUM(CASE 
   WHEN type__c = 'REFUND' 
     AND status__c = 'PAID' 
    THEN GREATEST(amount_refunded__c,amount__c) 
  END) AS refund_amount
 ,COUNT(DISTINCT CASE 
   WHEN type__c ='REFUND' 
     AND status__c = 'PAID' 
    THEN id  
  END) AS refund_transactions
 ,MAX(CASE 
   WHEN type__c ='REFUND' 
     AND status__c = 'PAID' 
    THEN date_paid__c 
  END) AS max_refund_date
 ,MIN(CASE 
   WHEN type__c ='REFUND' 
     AND status__c = 'PAID' 
    THEN date_paid__c 
  END) AS min_refund_date
 ,SUM(CASE 
   WHEN type__c ='CHARGE BACK' 
     AND status__c = 'PAID' 
    THEN GREATEST(amount_refunded__c,amount__c) 
  END) AS chargeback_amount
 ,COUNT(DISTINCT CASE 
   WHEN type__c ='CHARGE BACK' 
     AND status__c = 'PAID' 
    THEN id  
  END) AS chargeback_transactions
 ,MAX(CASE 
   WHEN type__c ='CHARGE BACK' 
     AND status__c = 'PAID' 
    THEN date_paid__c 
  END) AS max_chargeback_date
 ,MIN(CASE 
   WHEN type__c ='CHARGE BACK' 
     AND status__c = 'PAID' 
    THEN date_paid__c 
  END) AS min_chargeback_date
FROM stg_salesforce.refund_payment__c
GROUP BY 1
)
,old_infra_asset_payment AS (
SELECT
/*payment_group_id is only relevant from billing service and ledger. 
 * For SF, we simply populate the payment id since there is no payment group id*/
  ap.id AS payment_group_id
 ,ap.id AS asset_payment_id
 ,ap.name AS asset_payment_sfid
 ,NULL::TEXT AS transaction_id 
 ,NULL::TEXT AS resource_id
 ,NULL::TEXT AS movement_id
 ,ap.charge_id__c AS psp_reference_id
 ,ac.spree_customer_id__c::INT AS customer_id
 ,o.spree_order_number__c AS order_id
 ,ap.asset__c AS asset_id
 ,ap.allocation__c AS allocation_id
 ,aa.subscription__c AS subscription_id
 ,related_subscription_payment__c AS related_subscription_payment_id
 ,ap.type__c AS payment_type
 ,ap.createddate AS created_at
 ,GREATEST(ap.lastmodifieddate,ap.systemmodstamp) AS updated_at
 ,ap.status__c AS status
 ,date_due__c AS due_date
 ,CASE 
   WHEN ap.status__c = 'PAID' AND ap.date_paid__c IS NOT NULL 
    THEN ap.date_paid__c 
  END AS paid_date
 ,NULL::TIMESTAMP AS money_received_at 
 ,CASE 
   WHEN ap.type__c in ('GROVER SOLD','CUSTOMER BOUGHT') 
     AND ap.status__c = 'PAID' 
    THEN ap.date_paid__c 
  END AS sold_date
 ,ap.reason_paid__c AS paid_status
 ,ap.currency__c AS currency
 ,ap.payment_method__c AS payment_method
 ,ap.payment_method_funding__c AS payment_method_funding
 ,ap.payment_method_details__c AS payment_method_details
 ,ap.invoice_url__c AS invoice_url
 ,ap.invoice_number__c AS invoice_number
 ,ap.date_invoiced__c AS invoice_date
 ,ap.date_invoice_sent__c AS invoice_sent_date
 ,ap.amount__c AS amount
 ,ap.amount_f_balance__c AS amount_balance
 ,ap.amount_f_due__c AS amount_due
 ,ap.amount_paid__c AS amount_paid
 ,COALESCE(ap.amount_f_vat__c, ap.amount_f_tax__c) AS amount_tax
 ,ap.rate_vat__c AS tax_rate
 ,ap.amount_voucher__c AS amount_voucher
 ,ap.date_failed__c AS failed_date
 ,ap.reason_failed__c AS failed_reason
 ,ap.message__c AS failed_message
 ,ap.date_refunded__c AS refund_date
 ,ap.date_pending__c AS pending_date
 ,ap.date_try_last__c AS last_try_date
 ,ap.date_try_next__c AS next_try_date
 ,r.refund_amount
 ,r.refund_transactions
 ,r.max_refund_date
 ,r.min_refund_date
 ,r.chargeback_amount
 ,r.chargeback_transactions
 ,r.max_chargeback_date
 ,r.min_chargeback_date
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
   ELSE COALESCE(cap.name, ass.purchase_payment_method__c)
  END AS capital_source
 ,ap.booking_date__c AS booking_date
 ,'legacy'::TEXT AS src_tbl
FROM stg_salesforce.asset_payment__c ap
  LEFT JOIN stg_salesforce.order AS o 
    ON o.id = ap.order__c
  LEFT JOIN stg_salesforce.account AS ac 
    ON ac.id = o.accountid
  LEFT JOIN stg_salesforce.customer_asset_allocation__c aa 
    ON ap.allocation__c = aa.id
  LEFT JOIN refund_old_infra r 
    ON r.asset_payment_id = ap.id
  LEFT JOIN stg_salesforce.asset ass 
    ON ass.id = ap.asset__c
  LEFT JOIN stg_salesforce.capital_source__c cap 
    ON ass.capital_source__c = cap.id
  LEFT JOIN dwh.asset_capitalsource_sold_date acsd 
    ON acsd.asset_id = ap.asset__c  
   AND acsd.capital_source_sold_date = '2021-08-01 00:01:00'
  LEFT JOIN dwh.asset_capitalsource_sold_date acsd2 
    ON acsd2.asset_id = ap.asset__c  
   AND acsd2.capital_source_sold_date = '2021-09-01 00:01:00'
WHERE TRUE
  AND NOT (ap.type__c= 'SHIPMENT' AND ap.status__c in ('NOT PAID'))
  AND ap.status__c NOT IN ('CANCELLED')
)
-------------------------------------------------------------------------------
------------------------NEW INFRA - BILLING SERVICE----------------------------
-------------------------------------------------------------------------------
,billing_service_transactions AS (
SELECT 
  account_to 
 ,status
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'referenceId') AS psp_reference_id
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response,'paymentMethod'),'') AS payment_method
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(gateway_response,'gatewayResponse'),'paymentMethod'),'') AS payment_method2
 ,failed_reason
 ,CASE
 	 WHEN status = 'success'
 	  THEN created_at
 END paid_date
 ,CASE
 	 WHEN status = 'failed'
 	  THEN created_at
 END failed_date
 ,CASE
 	 WHEN status = 'new'
 	  THEN created_at
 END pending_date 
 ,ROW_NUMBER() OVER (PARTITION BY account_to ORDER BY created_at DESC, updated_at DESC) AS rx 
FROM oltp_billing.transaction
)
,billing_service_refund_amount_and_dates AS (
SELECT
  uuid
 ,createdat::TIMESTAMP AS refund_date
 ,amount AS refund_amount
FROM oltp_billing.payment_order po
WHERE TRUE 		
  AND paymenttype = 'purchase'
  AND status = 'refund'
)
,wallet_pm AS (
SELECT 
	ixopay_reference_id 
 ,"option" AS pm
FROM oltp_wallet.payment_method 
UNION
SELECT 
	wt.ixopay_uuid AS ixopay_reference_id
 ,wt.pm 
FROM wallet_transaction wt
   INNER JOIN billing_service_transactions bs
     ON wt.ixopay_uuid = bs.psp_reference_id 
)
,billing_service_asset_payment AS (
SELECT
  po."group" AS payment_group_id
 ,po.uuid AS asset_payment_id
 ,NULL::TEXT AS asset_payment_sfid
 ,NULL::TEXT AS transaction_id 
 ,NULL::TEXT AS resource_id
 ,NULL::TEXT AS movement_id
 ,t.psp_reference_id AS psp_reference_id 
 ,o.customer_id AS customer_id
 ,po.ordernumber AS order_id
 ,asm.asset_id
 ,asm.allocation_id
 ,COALESCE(s.subscription_id, po.contractid) AS subscription_id
 ,NULL::TEXT AS related_subscription_payment_id
 ,'CUSTOMER BOUGHT'::TEXT AS payment_type
 ,po.createdat::TIMESTAMP AS created_at
 ,po.updatedat::TIMESTAMP AS updated_at
 ,UPPER(po.status) AS status 	
 ,duedate::TIMESTAMP AS due_date
 ,t.paid_date
 ,NULL::TIMESTAMP AS money_received_at
 ,t.paid_date AS sold_date
 ,NULL::TEXT AS paid_status
 ,po.currency
 ,CASE 
   WHEN t.payment_method IS NULL 
     OR LEN(t.payment_method) = 36 
     OR t.payment_method ='Invalid PM' 
    THEN wp.pm 
   ELSE COALESCE(t.payment_method, t.payment_method2) 
  END AS payment_method
 ,NULL::TEXT AS payment_method_funding
 ,NULL::TEXT AS payment_method_details
 ,bi.invoice_url
 ,bi.invoice_number_pdf AS invoice_number
 ,bi.invoice_date ::TIMESTAMP AS invoice_date
 ,bi.invoice_date ::TIMESTAMP AS invoice_sent_date
 ,ROUND(CASE po.currency
 	 WHEN 'EUR'
 	  THEN po.amount
 	 WHEN 'USD'
 	  THEN po.amount * (1 + COALESCE(bts.payment_group_tax_rate, 0))
  END::DECIMAL(22, 6), 2) AS amount
 /*THIS IS NOT CORRECT FOR PARTIALLY PAID CASES BUT THERE ARE ONLY A FEW SUCH CASES*/ 
 ,ROUND(CASE 
 	 WHEN po.status <> 'paid' AND po.currency = 'EUR'
 	  THEN po.amount
 	 WHEN po.status <> 'paid' AND po.currency = 'USD'
 	  THEN po.amount * (1 + COALESCE(bts.payment_group_tax_rate, 0))
 	 WHEN po.status = 'paid'
 	  THEN 0
 	 ELSE 0 	  
 END::DECIMAL(22, 6), 2) amount_balance
 ,ROUND(CASE po.currency
 	 WHEN 'EUR'
 	  THEN po.amount
 	 WHEN 'USD'
 	  THEN po.amount * (1 + COALESCE(bts.payment_group_tax_rate, 0)) 
 END::DECIMAL(22, 6), 2) amount_due
 ,ROUND(CASE 
 	 WHEN po.status IN ('paid', 'refund', 'partial refund') AND t.paid_date IS NOT NULL AND po.currency = 'EUR'
 	  THEN po.amount
 	 WHEN po.status IN ('paid', 'refund', 'partial refund') AND t.paid_date IS NOT NULL AND po.currency = 'USD'
 	  THEN po.amount * (1 + COALESCE(bts.payment_group_tax_rate, 0))
 	 ELSE 0 	  
 END::DECIMAL(22, 6), 2) amount_paid 
 ,ROUND((po.amount * bts.payment_group_tax_rate)::DECIMAL(22, 6), 2) AS amount_tax
 ,bts.payment_group_tax_rate::DOUBLE PRECISION AS tax_rate 
 ,NULL::DOUBLE PRECISION  AS amount_voucher
 ,t.failed_date
 ,t.failed_reason
 ,NULL::TEXT AS failed_message
 ,lrd.refund_date
 ,t.pending_date
 ,NULL::TIMESTAMP AS last_try_date
 ,NULL::TIMESTAMP AS next_try_date
 ,ROUND(CASE po.currency
 	 WHEN 'EUR'
 	  THEN lrd.refund_amount
 	 WHEN 'USD'
 	  THEN lrd.refund_amount * (1 + COALESCE(bts.payment_group_tax_rate, 0))
  END::DECIMAL(22, 6), 2) AS refund_amount
 /*LEAVING THIS AND NEXT ONES AS NULL SINCE NR OF REFUDS IS VERY LIMITED
  * AND CHARGEBACKS DON'T EXIST IN BILLING SERVICE*/
 ,NULL::BIGINT AS refund_transactions
 ,NULL::TIMESTAMP AS max_refund_date
 ,NULL::TIMESTAMP AS min_refund_date
 ,NULL::DOUBLE PRECISION AS chargeback_amount
 ,NULL::BIGINT AS chargeback_transactions
 ,NULL::TIMESTAMP AS max_chargeback_date
 ,NULL::TIMESTAMP AS min_chargeback_date
 ,asm.capital_source
 ,NULL::TIMESTAMP AS booking_date
 ,'billing_service'::TEXT AS src_tbl
FROM oltp_billing.payment_order po
  LEFT JOIN billing_service_transactions t
    ON po."group" = t.account_to
   AND t.rx = 1 
  LEFT JOIN wallet_pm wp 
   ON t.psp_reference_id = wp.ixopay_reference_id
  LEFT JOIN ods_production.ORDER o 
    ON po.ordernumber = o.order_id       
  LEFT JOIN ods_production.asset_subscription_mapping asm
    ON po.contractid = asm.subscription_id_migrated
   AND asm.rx_last_allocation_per_sub = 1 
  LEFT JOIN ods_production.billing_invoices_clerk bi
    ON po."group" = bi.payment_group_id  
  LEFT JOIN stg_curated.order_additional_info bts 
    ON po.ordernumber = bts.order_id
   AND po."group" = bts.payment_group_id   
 LEFT JOIN billing_service_refund_amount_and_dates lrd
   ON po.uuid = lrd.uuid
 LEFT JOIN ods_production.subscription s 
   ON po.contractid = COALESCE(s.subscription_bo_id, s.subscription_id)  
WHERE TRUE 
  AND po.paymenttype = 'purchase'   
)
-------------------------------------------------------------------------------
------------------------NEW INFRA - LEDGER-------------------------------------
-------------------------------------------------------------------------------
,billing_service_exclusion_list AS (
/*WE WILL USE THIS LIST TO EXCLUDE PAYMENTS FROM LEDGER 
 *THAT ARE ALREADY AVAILABLE IN BILLING SERVICE*/
SELECT 
  contractid AS subscription_id
 ,duedate::date AS due_date   
FROM oltp_billing.payment_order po 
WHERE TRUE 
  AND po.paymenttype = 'purchase'  
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
  AND type = 'purchase'
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
 ,ROW_NUMBER() OVER (PARTITION BY transaction_id, slug ORDER BY created_at DESC, consumed_at DESC) rx
 ,COUNT(transaction_id) OVER (PARTITION BY slug) nr_of_transactions
FROM ods_production.ledger_curated
WHERE TRUE 
  AND "type" = 'purchase'
/*WE ARE ONLY INTERESTED IN THE ACTUAL ASSET PURCHASES AND NOT THE REFUNDS*/
  AND latest_movement NOT LIKE '%REFUND%'
)
,ledger_asset_payment AS (
SELECT 
  lcs.latest_movement_id::TEXT AS payment_group_id
 ,lcs.slug AS asset_payment_id
 ,NULL::TEXT AS asset_payment_sfid
 ,lcs.transaction_id
 ,lcs.id AS resource_id
 ,lcs.latest_movement_id
 ,lcs.psp_reference_id
 ,lcs.customer_id
 ,s.order_id
 ,asm.asset_id
 ,asm.allocation_id
 ,COALESCE(s.subscription_id, lcs.subscription_id) AS subscription_id
 ,NULL::TEXT AS related_subscription_payment_id
 ,'CUSTOMER BOUGHT' AS payment_type
 ,lcs.resource_created_at_timestamp AS created_at
 ,lcs.created_at_timestamp AS updated_at
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
  /*CHECK IF THIS MAKES SENSE*/
 ,paid_date AS sold_date
 ,NULL::TEXT AS paid_status
 ,lcs.currency
 ,COALESCE(NULLIF(lcs.payment_method,''), wt.pm) AS payment_method
 ,NULL AS payment_method_funding
 ,NULL::TEXT AS payment_method_details
 /*WE SHOULD POPULATE ALL INVOICE RELATED INFO ONCE IT BECOMES AVAILABLE*/
 ,bi.invoice_url
 ,bi.invoice_number_pdf AS invoice_number
 ,bi.invoice_date::TIMESTAMP
 ,NULL::TIMESTAMP AS invoice_sent_date
  ,lcs.value AS amount
  /*THIS GENERATES VALUES LIKE -0.000000000000001 
    WE NEED TO FIX THIS*/
  ,lcs.value - 
   CASE 
 	  WHEN NOT is_valid_json(REPLACE(REPLACE(lcs.transactions, '[', ''), ']', ''))
 	   THEN 0
 	  WHEN JSON_EXTRACT_PATH_TEXT(REPLACE(REPLACE(lcs.transactions, '[', ''), ']', ''),'amount') = ''
 	    THEN 0
 	  ELSE ROUND(JSON_EXTRACT_PATH_TEXT(REPLACE(REPLACE(lcs.transactions, '[', ''), ']', ''),'amount')::DECIMAL(22, 6), 2) 
 END AS amount_balance
 /*NOT SURE IF AMOUNT DUE SHOULD BE DIFFERENT THAN AMOUNT*/
 ,amount AS amount_due
 ,CASE lcs.latest_movement_status 
   WHEN 'SUCCESS' 
    THEN lcs.amount_to_allocate 
   ELSE 0
  END AS amount_paid
 ,lcs.tax_amount AS amount_tax 
 ,lcs.tax_rate::DECIMAL(22, 6) AS tax_rate
 ,NULL::DOUBLE PRECISION  AS amount_voucher
 ,fp.failed_date::TIMESTAMP  
 ,fp.failed_reason
 /*I THINK THERE IS ONLY FAILED REASON IN LEDGER*/
 ,NULL failed_message
 ,lrd.refund_date
 ,CASE lcs.latest_movement_status
   WHEN 'RUNNING' 
    THEN lcs.latest_movement_created_at_timestamp
   ELSE NULL
  END AS pending_date  
/*IDEALLY WE SHOULD REMOVE MOST OF THESE FIELDS*/  
 ,NULL::TIMESTAMP AS last_try_date
 ,NULL::TIMESTAMP AS next_try_date 
 ,lrd.refund_amount
 ,NULL::BIGINT AS refund_transactions
 ,NULL::TIMESTAMP AS max_refund_date
 ,NULL::TIMESTAMP AS min_refund_date
 ,NULL::DOUBLE PRECISION AS chargeback_amount
 ,NULL::BIGINT AS chargeback_transactions
 ,NULL::TIMESTAMP AS max_chargeback_date
 ,NULL::TIMESTAMP AS min_chargeback_date
 ,asm.capital_source
 ,NULL::TIMESTAMP AS booking_date
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
  LEFT JOIN billing_service_exclusion_list excl
    ON lcs.subscription_id = excl.subscription_id
   AND lcs.resource_created_at_timestamp::DATE = excl.due_date
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
)
-------------------------------------------------------------------------------
------------------------UNION ALL----------------------------------------------
-------------------------------------------------------------------------------
SELECT *
FROM billing_service_asset_payment
UNION ALL
SELECT *
FROM ledger_asset_payment
UNION ALL
SELECT *
FROM old_infra_asset_payment
;

commit;