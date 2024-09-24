DROP TABLE IF EXISTS ods_production.payment_refund;
CREATE TABLE ods_production.payment_refund AS
-------------------------------------------------------------------------------
-----------------------------GENERAL CTES--------------------------------------
-------------------------------------------------------------------------------
/* TO CAPTURE PAYMENT METHODS THAT ARE EITHER NULL OR HAVE AN ALPHANUMERIC VALUE IN THE BILLING SERVICE OR LEDGER */
WITH wallet_transaction AS (
SELECT DISTINCT
  a.external_id
 ,a.ixopay_uuid 
 ,b."option" AS pm 
FROM oltp_wallet.transaction a
  LEFT JOIN oltp_wallet.payment_method b 
    ON a.payment_method_id = b.id
)
-------------------------------------------------------------------------------
------------------------OLD INFRA - SALESFORCE---------------------------------
-------------------------------------------------------------------------------
,refund_old_infra AS (
SELECT 
/*payment_group_id is only relevant from billing service and ledger. 
 * For SF, we simply populate the payment id since there is no payment group id*/
  r.id AS payment_group_id
 ,r.id AS refund_payment_id
 ,r.name AS refund_payment_sfid
 ,NULL::TEXT AS transaction_id 
 ,NULL::TEXT AS resource_id
 ,NULL::TEXT AS movement_id
 ,r.charge_id__c AS psp_reference_id
 ,r.type__c AS refund_type
 ,r.createddate AS created_at
 ,r.createdbyid AS created_by
 ,GREATEST(r.lastmodifieddate, r.systemmodstamp) AS updated_at
 ,r.status__c AS status
 ,r.subscription_payment__c AS subscription_payment_id
 ,r.asset_payment__c AS asset_payment_id
 ,r.asset__c AS asset_id
 ,ac.spree_customer_id__c::INT AS customer_id
 ,o.spree_order_number__c AS order_id
 ,r.subscription__c AS subscription_id
 ,r.currency__C AS currency
 ,r.payment_method__c AS payment_method
 ,r.amount__c AS amount
 ,r.amount_f_due__c AS amount_due
 ,COALESCE(r.amount_f_sales_tax__c, r.amount_f_vat__c) AS amount_tax
 ,r.amount_refunded__c AS amount_refunded
 ,r.amount_refunded_sum__c AS amount_refunded_sum
 ,r.amount_repaid__c AS amount_repaid
 ,r.date_paid__c AS paid_date
 ,NULL::TIMESTAMP AS money_received_at 
 ,r.date_repaid__c AS repaid_date
 ,r.date_failed__c AS failed_date
 ,r.date_cancelled__c AS cancelled_Date
 ,r.date_pending__c AS pending_date
 ,r.reason__c AS reason
 ,CASE
   WHEN acsd.asset_id IS NOT NULL 
     AND paid_date::DATE < acsd.capital_source_sold_date::DATE 
    THEN acsd.old_value 
   WHEN acsd.asset_id IS NOT NULL 
     AND pending_date::DATE <= acsd.capital_source_sold_date::DATE AND paid_date IS NULL 
    THEN acsd.old_value 
   WHEN acsd2.asset_id IS NOT NULL 
     AND paid_date::DATE <= acsd2.capital_source_sold_date::DATE 
    THEN acsd2.old_value 
   WHEN acsd2.asset_id IS NOT NULL 
     AND pending_date::DATE <= acsd2.capital_source_sold_date 
     AND paid_date IS NULL 
    THEN acsd2.old_value 
   ELSE COALESCE(cap.name,ass.purchase_payment_method__c) 
  END AS capital_source
 ,CASE
   WHEN sp.id IS NOT NULL 
    THEN 'SUBSCRIPTION_PAYMENT'
   WHEN ap.id IS NOT NULL 
    THEN 'ASSET_PAYMENT'
   ELSE 'UNKNOWN'
  END AS related_payment_type
 ,CASE 
   WHEN sp.id IS NOT NULL 
    THEN sp.type__c
   WHEN ap.id IS NOT NULL 
    THEN ap.type__c
   ELSE 'UNKNOWN'
  END AS related_payment_type_detailed
 ,r.rate_vat__c AS tax_rate
 ,o2.store_country
 ,'legacy'::TEXT AS src_tbl
FROM stg_salesforce.refund_payment__c r 
  LEFT JOIN stg_salesforce.order AS o 
    ON o.id = r.order__c
  LEFT JOIN stg_salesforce.account AS ac 
    ON ac.id = o.accountid
  LEFT JOIN stg_salesforce.asset ass 
    ON ass.id=r.asset__c 
  LEFT JOIN stg_salesforce.capital_source__c cap 
    ON ass.capital_source__c=cap.id
  LEFT JOIN stg_salesforce.subscription_payment__c sp 
    ON r.subscription_payment__c=sp.id
  LEFT JOIN stg_salesforce.asset_payment__c ap 
    ON r.asset_payment__c=ap.id
  LEFT JOIN dwh.asset_capitalsource_sold_date acsd  
    ON acsd.asset_id = r.asset__c  
   AND acsd.capital_source_sold_date = '2021-08-01 00:01:00'
  LEFT JOIN dwh.asset_capitalsource_sold_date acsd2 
    ON acsd2.asset_id = r.asset__c 
   AND acsd2.capital_source_sold_date = '2021-09-01 00:01:00'
  LEFT JOIN ods_production.ORDER o2
    ON o.id = o2.order_id
WHERE r.status__c NOT IN ('CANCELLED')
)
-------------------------------------------------------------------------------
------------------------NEW INFRA - BILLING SERVICE REFUND---------------------
-------------------------------------------------------------------------------
,billing_service_transactions AS (
/*ACTUALLY USING ROW NUMBER HERE IS NOT CORRECT BECAUSE SOMETIMES THERE ARE MULTIPLE TRANSACTIONS FOR MULTIPLE PAYMENT ORDERS
 Eg. 
 
 SELECT * 
 FROM oltp_billing.transaction
 WHERE accountfrom = '3e5e110d-f588-4b71-b91f-359efcb603c0';

 * WE JUST USE IT TO AVOID DUPLICATES*/
SELECT 
  account_from 
 ,status
 ,type
 ,JSON_EXTRACT_PATH_TEXT(gateway_response,'referenceId') AS psp_reference_id
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response,'paymentMethod'),'') AS payment_method
 ,NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(gateway_response,'gatewayResponse'),'paymentMethod'),'') AS payment_method2
 ,JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(gateway_response,'chargebackData'), 'chargebackDateTime') AS chargeback_date
 ,created_at
 ,updated_at
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
 ,COALESCE(NULLIF(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(gateway_response, 'ixopay'), 'uuid'), '')
          ,NULLIF(JSON_EXTRACT_PATH_TEXT(gateway_response, 'ixopay_uuid'), '')) AS ixopay_uuid
 ,ROW_NUMBER() OVER (PARTITION BY account_from ORDER BY created_at DESC, updated_at DESC) AS rx
FROM oltp_billing.transaction
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
)
,refund_billing_service AS (
SELECT 
  po."group" AS payment_group_id
 ,po.uuid AS refund_payment_id
 ,NULL AS refund_payment_sfid
 ,NULL::TEXT AS transaction_id 
 ,NULL::TEXT AS resource_id
 ,NULL::TEXT AS movement_id
/*WE NEED TO BRING THIS PROBABLY FROM oltp_billing.transaction*/ 
 ,t.psp_reference_id AS psp_reference_id
 ,'REFUND' AS refund_type 
 ,po.createdat::TIMESTAMP AS created_at
 ,NULL AS created_by
 ,po.updatedat::TIMESTAMP AS updated_at
 ,CASE po.status 
	 WHEN 'refund'
	  THEN 'REFUND'
	 WHEN 'partial refund'
	  THEN 'PARTIAL REFUND'
	END AS status
 ,CASE 
 	 WHEN po.paymenttype = 'subscription'
 	  THEN po.uuid
 	END AS subscription_payment_id
 ,CASE 
 	 WHEN po.paymenttype = 'purchase'
 	  THEN po.uuid
 	END AS asset_payment_id
 ,asm.asset_id
 ,o.customer_id AS customer_id
 ,po.ordernumber AS order_id
 ,COALESCE(s.subscription_id, po.contractid) AS subscription_id
 ,po.currency AS currency
  ,CASE 
   WHEN t.payment_method IS NULL 
     OR LEN(t.payment_method) = 36 
     OR t.payment_method ='Invalid PM' 
    THEN wp.pm 
   ELSE COALESCE(t.payment_method, t.payment_method2) 
  END AS payment_method
 ,ROUND(CASE po.currency
 	 WHEN 'EUR'
 	  THEN po.amount
 	 WHEN 'USD'
 	  THEN po.amount * (1 + COALESCE(bts.payment_group_tax_rate, 0))
  END::DECIMAL(22, 6), 2) AS amount
 /*LET'S INVESTIGATE IF WE CAN REMOVE THIS COLUMN*/
 ,ROUND(CASE po.currency
 	 WHEN 'EUR'
 	  THEN po.amount
 	 WHEN 'USD'
 	  THEN po.amount * (1 + COALESCE(bts.payment_group_tax_rate, 0))
  END::DECIMAL(22, 6), 2) AS amount_due
, ROUND(NULLIF(JSON_EXTRACT_PATH_TEXT(
		JSON_EXTRACT_PATH_TEXT(
				JSON_SERIALIZE(payment_group_tax_breakdown[0])
		,'tax')
	,'in_cents'),'')::DECIMAL(22, 6)/100, 2) AS amount_tax
	/*THIS IS NOT CORRECT IN PARTIAL REFUNDS*/
 ,amount_due AS amount_refunded
 ,NULL::DECIMAL(22, 2) AS amount_refunded_sum
 ,NULL::DECIMAL(22, 2) AS amount_repaid
 ,t.paid_date::TIMESTAMP  
 ,NULL::TIMESTAMP AS money_received_at 
 ,NULL::TIMESTAMP  repaid_date
 /*WE SHOULD IDEALLY INPUT THIS*/
 ,t.failed_date::TIMESTAMP 
  /*WE SHOULD IDEALLY INPUT THIS*/
 ,NULL AS cancelled_date
 ,t.pending_date   
 /*NOT SURE IF THIS INFO IS STORED SOMEWHERE*/
 ,NULL AS reason
 ,asm.capital_source
 /*WE ARE ADDING HERE A NEW related_payment_type*/
 ,CASE po.paymenttype 
   WHEN 'shipping'
    THEN 'ORDER_PAYMENT'
   WHEN 'subscription'
    THEN 'SUBSCRIPTION_PAYMENT'
   WHEN 'purchase'
     THEN 'ASSET_PAYMENT'
   END AS related_payment_type
 ,CASE  
   WHEN po.paymenttype = 'shipping'
    THEN 'SHIPMENT'
   WHEN po.paymenttype = 'subscription' AND po."period" = 1 
    THEN 'FIRST'
   WHEN po.paymenttype = 'subscription' AND po."period" <> 1 
    THEN 'RECURRENT'
   WHEN po.paymenttype = 'purchase'
    THEN 'PURCHASE'
   END AS related_payment_type_detailed
 ,bts.payment_group_tax_rate AS tax_rate
 ,o.store_country
 ,'billing_service'::TEXT AS src_tbl
FROM oltp_billing.payment_order po
  LEFT JOIN ods_production.ORDER o 
    ON po.ordernumber = o.order_id
  LEFT JOIN ods_production.subscription s
    ON po.contractid = COALESCE(s.subscription_bo_id, s.subscription_id)    
  LEFT JOIN billing_service_transactions t
    ON po."group" = t.account_from
   AND t.rx = 1 
  LEFT JOIN ods_production.asset_subscription_mapping asm
    ON po.contractid = asm.subscription_id_migrated
   AND asm.rx_last_allocation_per_sub = 1 
  LEFT JOIN stg_curated.order_additional_info bts 
    ON po.ordernumber = bts.order_id
   AND po."group" = bts.payment_group_id     
  LEFT JOIN stg_external_apis.discarded_payment_groups dp
    ON po."group" = dp.a
  LEFT JOIN wallet_pm wp 
   ON t.ixopay_uuid = wp.ixopay_reference_id    
 WHERE TRUE 
   AND po.status IN ('partial refund', 'refund')
   AND dp.a IS NULL
   AND IS_VALID_JSON(JSON_SERIALIZE(payment_group_tax_breakdown[0])) 
)
,chargeback_billing_service AS (
SELECT 
  po."group" AS payment_group_id
 ,po.uuid AS refund_payment_id
 ,NULL AS refund_payment_sfid
 ,NULL::TEXT AS transaction_id 
 ,NULL::TEXT AS resource_id
 ,NULL::TEXT AS movement_id
/*WE NEED TO BRING THIS PROBABLY FROM oltp_billing.transaction*/ 
 ,t.psp_reference_id AS psp_reference_id
 ,'CHARGE BACK' AS refund_type 
 ,t.created_at::TIMESTAMP
 ,NULL AS created_by
 ,t.updated_at::TIMESTAMP
 /*ALL THE CHARGEBACK TRANSACTIONS ARE PROBABLY SUCCESSFUL*/
 ,'PAID'::TEXT AS status
 ,CASE 
 	 WHEN po.paymenttype = 'subscription'
 	  THEN po.uuid
 	END AS subscription_payment_id
 ,CASE 
 	 WHEN po.paymenttype = 'purchase'
 	  THEN po.uuid
 	END AS asset_payment_id
 ,asm.asset_id
 ,o.customer_id AS customer_id
 ,po.ordernumber AS order_id
 ,COALESCE(s.subscription_id, po.contractid) AS subscription_id
 ,po.currency AS currency
 ,CASE 
   WHEN t.payment_method IS NULL 
     OR LEN(t.payment_method) = 36 
     OR t.payment_method ='Invalid PM' 
    THEN wp.pm 
   ELSE COALESCE(t.payment_method, t.payment_method2) 
  END AS payment_method
 ,ROUND(po.amount::DECIMAL(22, 6), 2) AS amount
 /*LET'S INVESTIGATE IF WE CAN REMOVE THIS COLUMN*/
 ,ROUND(po.amount::DECIMAL(22, 6), 2) AS amount_due
, ROUND(NULLIF(JSON_EXTRACT_PATH_TEXT(
		JSON_EXTRACT_PATH_TEXT(
				JSON_SERIALIZE(payment_group_tax_breakdown[0])
		,'tax')
	,'in_cents'),'')::DECIMAL(22, 6)/100, 2) AS amount_tax
	/*THIS IS NOT CORRECT IN PARTIAL REFUNDS*/
 ,ROUND(po.amount::DECIMAL(22, 6), 2) AS  amount_refunded
 ,NULL::DECIMAL(22, 2) AS amount_refunded_sum
 ,NULL::DECIMAL(22, 2) AS amount_repaid
 ,NULLIF(t.chargeback_date, '')::TIMESTAMP AS paid_date 
 ,NULL::TIMESTAMP AS money_received_at 
 ,NULL::TIMESTAMP repaid_date
 ,t.failed_date::TIMESTAMP
  /*WE SHOULD IDEALLY INPUT THIS*/
 ,NULL::TIMESTAMP AS cancelled_date
 ,t.pending_date::TIMESTAMP   
 /*NOT SURE IF THIS INFO IS STORED SOMEWHERE*/
 ,NULL AS reason
 ,asm.capital_source
 /*WE ARE ADDING HERE A NEW related_payment_type*/
 ,CASE po.paymenttype 
   WHEN 'shipping'
    THEN 'ORDER_PAYMENT'
   WHEN 'subscription'
    THEN 'SUBSCRIPTION_PAYMENT'
   WHEN 'purchase'
     THEN 'ASSET_PAYMENT'
   END AS related_payment_type
 ,CASE  
   WHEN po.paymenttype = 'shipping'
    THEN 'SHIPMENT'
   WHEN po.paymenttype = 'subscription' AND po."period" = 1 
    THEN 'FIRST'
   WHEN po.paymenttype = 'subscription' AND po."period" <> 1 
    THEN 'RECURRENT'
   WHEN po.paymenttype = 'purchase'
    THEN 'PURCHASE'
   END AS related_payment_type_detailed
 /*WE ARE RENAMING VAT_RATE COLUMN TO TAX_RATE AND ADD SALES TAX AS WELL*/
 ,bts.payment_group_tax_rate AS tax_rate
 ,o.store_country
 ,'billing_service'::TEXT AS src_tbl
FROM oltp_billing.payment_order po
  INNER JOIN billing_service_transactions t
    ON po."group" = t.account_from
    AND t.type = 'chargeback'
    AND t.rx = 1 
  LEFT JOIN ods_production.ORDER o 
    ON po.ordernumber = o.order_id
  LEFT JOIN ods_production.subscription s
    ON po.contractid = COALESCE(s.subscription_bo_id, s.subscription_id)    
  LEFT JOIN ods_production.asset_subscription_mapping asm
    ON po.contractid = asm.subscription_id_migrated
   AND asm.rx_last_allocation_per_sub = 1 
  LEFT JOIN stg_curated.order_additional_info bts 
    ON po.ordernumber = bts.order_id
   AND po."group" = bts.payment_group_id    
  LEFT JOIN stg_external_apis.discarded_payment_groups dp
   ON po."group" = dp.a
  LEFT JOIN wallet_pm wp 
   ON t.ixopay_uuid = wp.ixopay_reference_id    
 WHERE TRUE 
   AND dp.a IS NULL
   AND IS_VALID_JSON(JSON_SERIALIZE(payment_group_tax_breakdown[0])) 
)
-------------------------------------------------------------------------------
------------------------NEW INFRA - LEDGER-------------------------------------
-------------------------------------------------------------------------------
,billing_service_exclusion_list AS (
/*WE WILL USE THIS LIST TO EXCLUDE REFUNDS FROM LEDGER 
 *THAT ARE ALREADY AVAILABLE IN BILLING SERVICE
 *THIS APPROACH IS NOT 100% CORRECT AS BILLING SERVICE QUITE OFTEN SHOWS REFUNDS AS PARTIAL REFUNDS*/
SELECT DISTINCT 
  CASE paymenttype
   WHEN 'subscription' 
    THEN contractid  
   WHEN 'purchase'
    THEN contractid  
   WHEN 'shipping'
    THEN ordernumber 
  END external_id  
 ,paymenttype
 ,period AS payment_number
 ,CASE status
   WHEN 'refund'
    THEN 'REFUNDED'
   WHEN 'partial refund'
    THEN 'PARTIALLY_REFUNDED'
  END status_modified   
 ,duedate::DATE AS due_date
FROM oltp_billing.payment_order po 
WHERE po.status IN ('partial refund', 'refund') 
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
 ,latest_movement_id
 ,psp_reference_id
 ,latest_movement_context
 ,resource_created_at_timestamp
 ,created_at_timestamp
 ,latest_movement_status 
 ,customer_id 
 ,payment_method
 ,ROUND(amount_to_allocate::DECIMAL(22, 6),2) AS amount_to_allocate
 ,latest_movement_created_at_timestamp
 ,money_received_at_timestamp
 ,latest_movement_context_reason
 ,CASE type
 	 WHEN 'shipping'
 	  THEN '1'::TEXT
 	 ELSE payment_number
 END AS payment_number
 ,ROW_NUMBER() OVER (PARTITION BY transaction_id, slug ORDER BY created_at DESC, consumed_at DESC) rx
 ,COUNT(transaction_id) OVER (PARTITION BY slug) nr_of_transactions
FROM ods_production.ledger_curated
WHERE status IN ('REFUNDED', 'PARTIALLY_REFUNDED') 
)
,refund_ledger AS (
SELECT 
  lcs.latest_movement_id::TEXT AS payment_group_id
 ,lcs.slug AS refund_payment_id
 ,NULL AS refund_payment_sfid
 ,lcs.transaction_id 
 ,lcs.id AS resource_id
 ,lcs.latest_movement_id
 ,lcs.psp_reference_id
/*THIS NEEDS TO BE MODIFIED WHEN CHARGEBACKS COME IN*/
 ,CASE 
	 WHEN lcs.status = 'REFUNDED' OR lcs.latest_movement_context = 'REFUND'
	  THEN 'REFUND'
	 WHEN lcs.status = 'PARTIALLY REFUNDED'
	  THEN 'PARTIAL REFUND'
	 ELSE lcs.status 
	END AS refund_type
 ,lcs.resource_created_at_timestamp AS created_at
 ,NULL AS created_by
 ,lcs.created_at_timestamp AS updated_at
 /*WE ARE MAPPIN STATUSES TO OLD INFRA. WE MIGHT EVALUATE TO KEEP THE ORIGINAL ONES*/
 ,CASE lcs.latest_movement_status 
   WHEN 'SUCCESS' 
    THEN 'PAID'
   WHEN 'RUNNING' 
    THEN 'PENDING'
   WHEN 'FAILED' 
    THEN 'FAILED'    
  END AS status
 ,CASE 
 	 WHEN TYPE = 'subscription'
 	  THEN lcs.slug  
 END AS subscription_payment_id
 ,CASE 
 	 WHEN TYPE = 'purchase'
 	  THEN lcs.slug  
  END AS asset_payment_id
 ,asm.asset_id
 ,lcs.customer_id
 ,s.order_id
 ,COALESCE(s.subscription_id, lcs.subscription_id) AS subscription_id
 ,lcs.currency
 ,COALESCE(lcs.payment_method, wt.pm) AS payment_method
 ,lcs.value AS amount
 /*LET'S INVESTIGATE IF WE CAN REMOVE THIS COLUMN*/
 ,lcs.value AS amount_due
 ,lcs.tax_amount AS amount_tax 
 /*MAYBE IT IS BETTER TO GET THIS FROM TRANSACTIONS*/	
 ,lcs.amount_to_allocate * -1 AS amount_refunded
 ,NULL::DECIMAL(22, 2) AS amount_refunded_sum
 ,NULL::DECIMAL(22, 2) AS amount_repaid
 ,CASE lcs.latest_movement_status 
   WHEN 'SUCCESS' 
    THEN lcs.latest_movement_created_at_timestamp
   ELSE NULL
  END AS paid_date 
 ,lcs.money_received_at_timestamp AS money_received_at 
 ,NULL repaid_date
 ,CASE 
   WHEN lcs.latest_movement_status  = 'FAILED' 
    THEN lcs.latest_movement_created_at_timestamp
  END AS failed_date
  /*WE SHOULD IDEALLY INPUT THIS*/
 ,NULL AS cancelled_date
 ,CASE lcs.latest_movement_status 
   WHEN 'RUNNING' 
    THEN lcs.latest_movement_created_at_timestamp 
   ELSE NULL
  END AS pending_date  
 ,lcs.latest_movement_context_reason AS reason
 ,asm.capital_source
 /*WE ARE ADDING HERE A NEW related_payment_type*/
 ,CASE lcs.type 
   WHEN 'shipping'
    THEN 'ORDER_PAYMENT'
   WHEN 'subscription'
    THEN 'SUBSCRIPTION_PAYMENT'
   WHEN 'purchase'
     THEN 'ASSET_PAYMENT'
   END AS related_payment_type
 ,CASE  
   WHEN lcs.type = 'shipping'
    THEN 'SHIPMENT'
   WHEN lcs.type = 'subscription' AND lcs.payment_number = 1
    THEN 'FIRST'
   WHEN lcs.type = 'subscription' AND lcs.payment_number <> 1 
    THEN 'RECURRENT'
   WHEN lcs.type = 'purchase'
    THEN 'PURCHASE'
   END AS related_payment_type_detailed
 /*WE ARE RENAMING VAT_RATE COLUMN TO TAX_RATE AND ADD SALES TAX AS WELL*/
 ,lcs.tax_rate::DECIMAL(22, 6) AS tax_rate
 ,o2.store_country
 ,'ledger'::TEXT AS src_tbl
FROM ledger_combined_source lcs
  LEFT JOIN ods_production.asset_subscription_mapping asm
    ON lcs.subscription_id = asm.subscription_id_migrated
   AND asm.rx_last_allocation_per_sub = 1 
  LEFT JOIN ods_production.subscription s 
    ON lcs.subscription_id = COALESCE(s.subscription_bo_id, s.subscription_id)
  LEFT JOIN ods_production.ORDER o2
    ON s.order_id = o2.order_id  
  LEFT JOIN wallet_transaction wt
    ON lcs.latest_movement_id = wt.external_id   
  LEFT JOIN billing_service_exclusion_list excl
    ON lcs.external_id = excl.external_id
   AND lcs.status = excl.status_modified
   AND CASE 
   	WHEN excl.paymenttype = 'purchase'
   	 THEN lcs.resource_created_at_timestamp::DATE  = excl.due_date
   	ELSE lcs.payment_number = excl.payment_number
   END
WHERE TRUE 
  AND CASE
	 WHEN lcs.nr_of_transactions = 0
	  THEN lcs.rx = 1
	 ELSE lcs.transaction_id IS NOT NULL 
	  AND lcs.rx = 1
	END 
  AND excl.external_id IS NULL
)
-------------------------------------------------------------------------------
-----------------------------UNION ALL-----------------------------------------
-------------------------------------------------------------------------------
/*I BELIEVE THIS WAS CREATED AS A STATIC SOURCE OF CHARGEBACKS- NOT SURE WHAT THE SOURCE WAS. 
 * AMOUNTS DON'T SEEM TO BE CORRECT (COMPARING TO IXOPAY)
 * ONCE WE GET ACTUAL CHARGEBAK DATA, WE PROBABLY NEED TO REMOVE THIS*/
SELECT * 
FROM finance.us_static_chargebacks
  UNION ALL
SELECT * 
FROM refund_billing_service
  UNION ALL
SELECT * 
FROM refund_ledger
  UNION ALL
SELECT * 
FROM refund_old_infra
  UNION ALL
SELECT *
FROM chargeback_billing_service
;

GRANT SELECT ON ods_production.payment_refund TO tableau;