DROP TABLE IF EXISTS ods_production.billing_service_updated_dpd;
CREATE TABLE ods_production.billing_service_updated_dpd AS
WITH raw_ AS (
SELECT DISTINCT
 order_.orderNumber,
 order_.contractId,
 order_.uuid,
 order_.period,
 order_.amount,
 order_.createdAt::DATE,
 order_.updatedAt::DATE,
 order_.dueDate::DATE,
 order_.billingDate::DATE,
 order_.status
FROM oltp_billing.payment_order order_
  LEFT JOIN master.subscription s 
    ON order_.contractid = s.subscription_id
WHERE TRUE
  AND order_.currency = 'USD'
  AND NOT (order_.period = 1 AND order_.status IN ('failed','partial paid'))
  AND NOT (order_.paymenttype = 'purchase' AND order_.status = 'failed')
  AND s.status != 'CANCELLED'
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
DISTINCT 
 r.ordernumber  ordernumber
 ,r.contractid contractid
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
FROM raw_ r
  LEFT JOIN max_payment
    ON r.contractid = max_payment.contractid
  LEFT JOIN min_failed
    ON min_failed.contractid = r.contractid
  LEFT JOIN max_due
    ON r.contractid = max_due.contractid
  LEFT JOIN min_partial
    ON r.contractid = min_partial.contractid
)
,contract_status_ AS (
SELECT 
   JSON_EXTRACT_PATH_text(payload, 'order_number') order_number
  ,JSON_EXTRACT_PATH_text(payload, 'id') contract_id
  ,JSON_EXTRACT_PATH_text(payload, 'state') state
  ,v2.event_name event_name_
  ,v2.event_timestamp event_timestamp_
  ,JSON_EXTRACT_PATH_text(payload, 'created_at') created_at
  ,JSON_EXTRACT_PATH_text(payload, 'activated_at') activated_at
  ,JSON_EXTRACT_PATH_text(payload, 'terminated_at') terminated_at
  ,JSON_EXTRACT_PATH_text(payload, 'termination_reason') termination_reason
  ,ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY event_timestamp desc) row_n
FROM stg_kafka_events_full.stream_customers_contracts_v2 v2
WHERE TRUE
  AND JSON_EXTRACT_PATH_text(payload, 'type') = 'flex'
  AND event_name NOT in ('revocation_expired', 'purchase_failed'))
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
FROM oltp_billing.payment_order order_
WHERE TRUE
  AND currency = 'USD'
  AND NOT (order_.period=1 AND order_.status IN ('failed','partial paid'))
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
  AND COALESCE(state,'n/a') NOT IN ('cancelled','ended')
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
  ,contract_status_.state
  ,contract_status_.event_name_
FROM oltp_billing.payment_order order_
  LEFT JOIN raw_2
    ON order_.contractid = raw_2.contractid
  LEFT JOIN contract_status_
    ON order_.contractid = contract_status_.contract_id
   AND contract_status_.row_n = 1
WHERE TRUE 
  AND currency = 'USD'
  AND NOT (order_.period=1 AND order_.status IN ('failed','partial paid'))
  AND order_.status IN ( 'failed','partial paid','paid')
  AND NOT (paymenttype = 'purchase' AND status = 'failed')
  AND COALESCE(contract_status_.state,'n/a') NOT IN ('cancelled','ended')
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
  order_.contractid
 ,SUM(order_.amount) paid_amount
FROM raw_ order_
WHERE status in ('paid','partial paid')
GROUP BY 1
)
,raw_8 AS (
SELECT
 order_.contractid
 ,MIN(transaction_.created_at::DATE) first_failed_date
 ,MIN(CASE 
   WHEN GREATEST(DATEDIFF('DAY',duedate::DATE ,transaction_.created_at::DATE), 0) >= 15 
    THEN transaction_.created_at::DATE 
  END) first_failed_date_dpd_15
FROM oltp_billing.payment_order order_
  INNER JOIN oltp_billing.transaction transaction_
    ON transaction_.account_to = order_."group"
WHERE TRUE 
  AND order_.currency = 'USD'
  AND NOT (order_.paymenttype = 'purchase' AND order_.status = 'failed')
  AND transaction_.status = 'failed'
GROUP BY 1
)
SELECT 
DISTINCT 
  final_q.ordernumber
 ,final_q.contractid
 ,contract_status_.event_name_
 ,contract_status_.state
 ,first_failed
 ,last_failed
 ,raw_8.first_failed_date
 ,raw_8.first_failed_date_dpd_15
 ,raw_7.paid_amount
 ,CASE 
   WHEN COALESCE(duedate,CURRENT_DATE) < CURRENT_DATE 
    THEN CURRENT_DATE-duedate 
   ELSE 0 
  END adjusted_dpd
 ,duedate updated_due_date
 ,"group", amount, period, status old_payment_status, new_status new_payment_status
 ,raw_6.nr_successful_payments
 ,raw_6.total_nr_payments
FROM final_q
  LEFT JOIN raw_5 
    ON final_q.contractid = raw_5.contractid 
   AND raw_5.row_n = 1
  LEFT JOIN contract_status_ 
    ON final_q.contractid = contract_status_.contract_id 
   AND contract_status_.row_n = 1
  LEFT JOIN raw_6 
    ON raw_6.contractid = final_q.contractid
  LEFT JOIN raw_7 
    ON raw_7.contractid = final_q.contractid
  LEFT JOIN raw_8 
    ON raw_8.contractid = final_q.contractid ;