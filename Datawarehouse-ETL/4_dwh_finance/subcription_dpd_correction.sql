/*STEP 1 WE IDENTIFY ALL PROBLEMATIC CASES WHERE THERE ARE PAID PAYMENTS AFTER FAILED PAYMENTS  
 * FOR THOSE WE WANT TO RECALCUATE DPD IN THE NEXT STEPS*/
DELETE FROM dm_finance.subs_with_problematic_dpds
WHERE reporting_date = CURRENT_DATE
;

INSERT INTO dm_finance.subs_with_problematic_dpds
SELECT 
  CURRENT_DATE AS reporting_date
 ,s.subscription_id
 ,s2.subscription_bo_id
 ,s.country_name
 ,s2.status
 ,MIN(CASE
  	WHEN s.status = 'FAILED'
  	 THEN s.due_date
  END) min_failed_due_date
 ,MAX(CASE
  	WHEN s.status = 'PAID'
  	 THEN s.due_date
  END) max_paid_due_date
FROM ods_production.payment_subscription s
  LEFT JOIN ods_production.subscription s2 
    ON s.subscription_id = s2.subscription_id
WHERE TRUE
GROUP BY 1,2,3,4,5
HAVING min_failed_due_date < max_paid_due_date
;


-------------------------------------------------------------------------------
/*STEP 2 LETS CALCULATE ADJUSTED DPDs*/
DELETE FROM dm_finance.dpd_corrections
WHERE reporting_date = CURRENT_DATE
;

INSERT INTO dm_finance.dpd_corrections
WITH raw_ AS (
SELECT distinct
 orderNumber,
 contractId,
 uuid,
 period,
 amount,
 createdat::DATE,
 updatedat::DATE,
 duedate::DATE,
 billingdate::DATE,
 status
FROM oltp_billing.payment_order
WHERE TRUE
  AND NOT (period = 1 AND status IN ('failed','partial paid'))
  AND NOT (paymenttype = 'purchase' AND status = 'failed')
)
,max_payment AS (
SELECT 
  ordernumber
 ,contractid
 ,MAX(period) period
 ,MAX(createdat) createdat
 ,MAX(updatedat) updatedat
 ,MAX(duedate) duedate
 ,MAX(billingdate) billingdate
FROM raw_
WHERE status = 'paid'
GROUP BY 1,2
)
,max_due AS (
 SELECT 
  ordernumber
 ,contractid
 ,MIN(period) period
 ,MIN(createdat) createdat
 ,MIN(updatedat) updatedat
 ,MIN(duedate) duedate
 ,MIN(billingdate) billingdate
FROM raw_ q1
WHERE TRUE 
  AND status = 'failed'
  AND period > (SELECT period 
                FROM max_payment q2
                WHERE q2.contractid = q1.contractid)
GROUP BY 1,2
)
,min_failed AS (
SELECT
  ordernumber
 ,contractid
 ,MIN(period) period
 ,MIN(createdat) createdat
 ,MIN(updatedat) updatedat
 ,MIN(duedate) duedate
 ,MIN(billingdate) billingdate
FROM raw_
WHERE status = 'failed'
GROUP BY 1,2
)
,min_partial AS (
SELECT
 ordernumber
 ,contractid
 ,MIN(period) period
 ,MIN(createdat) createdat
 ,MIN(updatedat) updatedat
 ,MIN(duedate) duedate
 ,MIN(billingdate) billingdate
FROM raw_
WHERE status = 'partial paid'
GROUP BY 1,2
)
,final_q AS (
SELECT 
 max_payment.ordernumber  ordernumber
 ,max_payment.contractid contractid
 ,COALESCE(min_failed.period,0)  min_failed_period
 ,COALESCE(max_due.period,0)  max_paid_period
 ,COALESCE(min_partial.period,0)  min_partial_period
 ,COALESCE(min_failed.createdat,CURRENT_DATE+1)  first_createdat
 ,COALESCE(max_due.createdat,CURRENT_DATE+1)  last_createdat
 ,COALESCE(min_partial.createdat,CURRENT_DATE+1)  partial_createdat
 ,CASE 
   WHEN min_failed.createdat IS NULL 
    THEN -1 else CURRENT_DATE - min_failed.createdat 
  END first_failed
 ,CASE 
   WHEN max_due.createdat IS NULL 
    THEN -1 else CURRENT_DATE - max_due.createdat 
  END last_failed
 ,CASE 
   WHEN min_partial.createdat IS NULL 
    THEN -1 else CURRENT_DATE - min_partial.createdat 
  END partial_payment
 ,CASE 
   WHEN max_payment.createdat IS NULL 
    THEN -1 else CURRENT_DATE - max_payment.createdat 
  END last_payment
 ,COALESCE(min_failed.duedate, CURRENT_DATE +1)  min_duedate
 ,COALESCE(max_due.duedate, CURRENT_DATE +1)   max_duedate
 ,COALESCE(min_failed.billingdate, CURRENT_DATE +1)  min_billingdate
 ,COALESCE(max_due.billingdate, CURRENT_DATE +1)  max_billingdate
FROM max_payment
  LEFT JOIN min_failed
    ON min_failed.contractid = max_payment.contractid
  LEFT JOIN max_due
    ON max_payment.contractid = max_due.contractid
  LEFT JOIN min_partial
    ON max_payment.contractid = min_partial.contractid
)
,contract_status_ AS (
SELECT 
   JSON_EXTRACT_PATH_text(payload, 'order_number') order_number
  ,JSON_EXTRACT_PATH_text(payload, 'id') contract_id
  ,JSON_EXTRACT_PATH_text(payload, 'state') state
  ,event_name event_name_
  ,event_timestamp event_timestamp_
  ,JSON_EXTRACT_PATH_text(payload, 'created_at') created_at
  ,JSON_EXTRACT_PATH_text(payload, 'activated_at') activated_at
  ,JSON_EXTRACT_PATH_text(payload, 'terminated_at') terminated_at
  ,JSON_EXTRACT_PATH_text(payload, 'termination_reason') termination_reason
  ,ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY event_timestamp desc) row_n
FROM stg_kafka_events_full.stream_customers_contracts_v2
WHERE TRUE
  AND JSON_EXTRACT_PATH_text(payload, 'type') = 'flex'
  AND event_name NOT in ('revocation_expired', 'purchase_failed')
)
,raw_6 AS (
SELECT
  ordernumber
 ,contractid
 ,COUNT(DISTINCT CASE 
   WHEN status = 'paid' 
    THEN "group" 
  END) nr_successful_payments
 ,COUNT(*) total_nr_payments
 ,COUNT(DISTINCT period) total_nr_periods
 ,MIN(CASE 
   WHEN status = 'paid' 
    THEN duedate::DATE 
  END) min_paid_due_date
 ,SUM(CASE 
   WHEN status IN ('paid','partial paid') 
    THEN amount 
  END) paid_amount
 ,MIN(CASE 
   WHEN status IN ('failed','partial paid') 
    THEN duedate::DATE 
  END) min_failed_due_date
 ,MAX(CASE 
   WHEN status = 'paid' 
    THEN duedate::DATE 
  END) max_paid_due_date
 ,MAX(CASE 
   WHEN status IN ('failed','partial paid') 
    THEN duedate::DATE 
  END) max_failed_due_date
FROM oltp_billing.payment_order
WHERE TRUE
  AND NOT (period=1 AND status IN ('failed','partial paid'))
  AND NOT (paymenttype = 'purchase' AND status = 'failed')
GROUP BY 1,2
HAVING total_nr_payments > 1
)
,raw_2 AS (
SELECT 
  raw_6.*
 ,contract_status_.state
 ,contract_status_.event_name_
FROM raw_6
  LEFT JOIN contract_status_
    ON raw_6.contractid = contract_status_.contract_id
   AND contract_status_.row_n = 1
WHERE TRUE 
  AND min_failed_due_date < max_paid_due_date
  AND COALESCE(state,'n/a') NOT IN ('ended')
)
,raw_4 AS (
SELECT
  order_.ordernumber
  ,order_.contractid
  ,order_."group"
  ,order_.amount
  ,order_.period
  ,order_.status
  ,order_.duedate::DATE
  ,CASE
    WHEN order_.status IN ( 'failed','partial paid')  
      AND raw_2.nr_successful_payments >= period 
     THEN 'paid'
    WHEN order_.status IN( 'failed','partial paid') 
     THEN order_.status
    WHEN order_.status = 'paid' AND (raw_2.nr_successful_payments) < period 
     THEN 'failed'
    WHEN order_.status = 'paid' 
     THEN 'paid'
    ELSE order_.status 
   END new_status
  ,raw_2.nr_successful_payments
  ,cs.state
  ,cs.event_name_
FROM oltp_billing.payment_order order_
  LEFT JOIN raw_2
    ON order_.contractid = raw_2.contractid
  LEFT JOIN contract_status_ cs
    ON order_.contractid = cs.contract_id
   AND cs.row_n = 1
WHERE TRUE 
  AND NOT (order_.period=1 AND order_.status IN ('failed','partial paid'))
  AND order_.status IN ( 'failed','partial paid','paid')
  AND NOT (paymenttype = 'purchase' AND status = 'failed')
  AND COALESCE(cs.state,'n/a') NOT IN ('ended')
  AND order_.paymenttype != 'purchase'
)
,raw_5 AS (
SELECT 
  raw_4.*
 ,ROW_NUMBER() OVER (PARTITION BY contractid ORDER BY duedate) row_n
FROM raw_4
WHERE new_status NOT IN ('paid','partial refund')
)
,raw_7 AS (
SELECT
  contractid
 ,SUM(amount) paid_amount
FROM raw_
WHERE status in ('paid','partial paid')
GROUP BY 1
)
,raw_8 AS (
SELECT
 o.contractid
 ,MIN(t.created_at::DATE) first_failed_date
 ,MIN(CASE 
   WHEN GREATEST(DATEDIFF('DAY',duedate::DATE ,t.created_at::DATE), 0) >= 15 
    THEN t.created_at::DATE 
  END) first_failed_date_dpd_15
FROM oltp_billing.payment_order o
  INNER JOIN oltp_billing.transaction t
    ON t.account_to = o."group"
WHERE TRUE 
  AND NOT (o.paymenttype = 'purchase' AND o.status = 'failed')
  AND t.status = 'failed'
GROUP BY 1
)
SELECT DISTINCT
  CURRENT_DATE AS reporting_date
 ,q.ordernumber
 ,q.contractid
 ,contract_status_.event_name_
 ,contract_status_.state
 ,q.first_failed
 ,last_failed
 ,raw_8.first_failed_date
 ,raw_8.first_failed_date_dpd_15
 ,raw_7.paid_amount
 ,CASE 
   WHEN COALESCE(duedate, CURRENT_DATE) < CURRENT_DATE
    THEN CURRENT_DATE - duedate 
   ELSE 0 
  END adjusted_dpd
 ,raw_5.duedate updated_due_date
 ,"group"
 ,amount
 ,period
 ,status old_payment_status
 ,new_status new_payment_status
 ,raw_6.nr_successful_payments
 ,raw_6.total_nr_payments
FROM final_q q
  LEFT JOIN raw_5 
    ON q.contractid = raw_5.contractid 
   AND raw_5.row_n = 1
  LEFT JOIN contract_status_ 
    ON q.contractid = contract_status_.contract_id 
   AND contract_status_.row_n = 1
  LEFT JOIN raw_6 
    ON raw_6.contractid = q.contractid
  LEFT JOIN raw_7 
    ON raw_7.contractid = q.contractid
  LEFT JOIN raw_8 
    ON raw_8.contractid = q.contractid
;

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/*STEP 3 LETS HISTORIZE ods_production.asset_last_allocation_details 
 SO WE CAN USE IT IN NEXT STEP */
DELETE FROM dm_finance.last_allocation_details_historical
WHERE reporting_date = CURRENT_DATE
;

INSERT INTO dm_finance.last_allocation_details_historical
SELECT
 CURRENT_DATE AS reporting_date
 ,asset_id
 ,subscription_id
 ,last_allocation_id
 ,last_allocation_status
 ,asset_status_original
 ,asset_status_new
 ,asset_status_detailed
 ,last_allocation_days_in_stock
 ,last_allocation_risk_class_group
 ,last_allocation_default_payment_due_date
 ,last_allocation_dpd
 ,dpd_bucket
FROM ods_production.asset_last_allocation_details 
; 

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------			
/*STEP 4 LETS CREATE THE FINAL OUTPUT*/
DELETE FROM dm_finance.sub_dpd_corrected_final
WHERE reporting_date = CURRENT_DATE
;

INSERT INTO dm_finance.sub_dpd_corrected_final
WITH base AS (
SELECT 
  asset_id 
  ,asset_status_original
  ,last_allocation_dpd 
  ,dpd_bucket 
  ,capital_source_name 
FROM master.asset_historical ah 
WHERE DATE = CURRENT_DATE
)
SELECT 
  CURRENT_DATE AS reporting_date
 ,t1.subscription_id
 ,t1.subscription_bo_id
 ,t1.status
 ,la.asset_id 
 ,b.asset_status_original
 ,b.capital_source_name
 ,t1.country_name
 ,t1.min_failed_due_date
 ,t1.max_paid_due_date
 ,b.last_allocation_dpd AS dpd
 ,t2.adjusted_dpd AS dpd_new
 ,b.dpd_bucket
FROM dm_finance.subs_with_problematic_dpds t1
  LEFT JOIN dm_finance.dpd_corrections t2
    ON COALESCE(t1.subscription_bo_id, t1.subscription_id) = t2.contractid 
   AND t1.reporting_date = t2.reporting_date 
  LEFT JOIN dm_finance.last_allocation_details_historical la
    ON t1.subscription_id = la.subscription_id
   AND t1.reporting_date = la.reporting_date  
  LEFT JOIN base b
    ON la.asset_id = b.asset_id
WHERE TRUE 
  AND t1.reporting_date = CURRENT_DATE
  AND t2.adjusted_dpd IS NOT NULL    
  AND COALESCE(la.dpd_bucket, 'n/a') NOT IN ('n/a', 'SOLD', 'LOST', 'IN STOCK')  
  AND COALESCE(b.last_allocation_dpd ,9999) > COALESCE(t2.adjusted_dpd ,9999)
  AND COALESCE(t2.adjusted_dpd ,9999) <> 0
;