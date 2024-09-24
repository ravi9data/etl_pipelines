/*I BELIEVE THIS WAS CREATED AS A STATIC SOURCE OF CHARGEBACKS- NOT SURE WHAT THE SOURCE WAS. 
 * AMOUNTS DON'T SEEM TO BE CORRECT (COMPARING TO IXOPAY)*/
DROP TABLE IF EXISTS finance.us_static_chargebacks;
CREATE TABLE finance.us_static_chargebacks AS
WITH static_chargeback_pre AS (
SELECT 
 a.psp_reference,
 a.merchant_reference,
 a.payment_method,
 LAST_VALUE(booking_date) OVER (PARTITION BY merchant_reference ORDER BY booking_date 
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_booking_date,
 FIRST_VALUE (booking_date) OVER (PARTITION BY merchant_reference ORDER BY booking_date
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_booking_date,
 a.main_currency,
 LAST_VALUE(record_type) OVER (PARTITION BY merchant_reference ORDER BY booking_date 
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS final_chargeback_status,
 a.payment_currency,
 SUM(payable_sc + markup_sc) over (PARTITION BY merchant_reference ORDER BY booking_date 
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS final_chargeback_amount,
 a.issuer_country,
 a.modification_merchant_ref,
 a.merchant_account,
 a.user_name,
 ROW_NUMBER() OVER (PARTITION BY psp_reference ORDER BY booking_date DESC) AS idx 
FROM staging.chargeback_us a
)
,static_chargeback AS (
SELECT * 
FROM static_chargeback_pre
WHERE idx= 1
)
,tr AS (
SELECT 
  JSON_EXTRACT_PATH_TEXT(gateway_response,'referenceId') AS reference_id
 ,* 
 FROM oltp_billing.transaction
)
/*SINCE IN THIS SOURCE THERE ARE NO TAX RATES
 * IT IS BEING ADDED ON ORDER LEVEL (QUESTIONABLE APPROACH)*/
,sales_tax_rate AS (
SELECT 
 order_id,
 tax_rate,
 payment_type, 
 ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at ASC) AS idx 
FROM ods_production.payment_subscription
)
,deduping AS (
SELECT * 
FROM sales_tax_rate 
WHERE idx = 1
)
SELECT
 p."group" AS payment_group_id,
 COALESCE(CONCAT(CONCAT(CONCAT(p."group",'_'),p.contractid),'_C'),CONCAT(merchant_reference,'_MISSING_C')) AS refund_payment_id,
 NULL AS refund_payment_sfid,
 NULL::TEXT AS transaction_id,
 NULL::TEXT AS resource_id,
 NULL::TEXT AS movement_id,
 cb.psp_reference AS psp_reference_id,
 'CHARGE BACK' AS refund_type,
 first_booking_date AS created_at,
 cb.user_name AS created_by,
 cb.last_booking_date::TIMESTAMP AS updated_at,
 cb.final_chargeback_status AS status,
 CASE 
  WHEN p.paymenttype = 'subscription' 
   THEN CONCAT(CONCAT(tr.uuid,'_'),p.contractid) 
 END AS subscription_payment_id,
 CASE 
  WHEN p.paymenttype = 'purchase' 
   THEN CONCAT(CONCAT(tr.uuid,'_'),p.contractid) 
 END AS asset_payment_id,
 COALESCE(ps.asset_id, pa.asset_id) AS asset_id,
 COALESCE(ps.customer_id::INT,pa.customer_id::INT) AS customer_id,
 p.ordernumber AS order_id,
 COALESCE(ps.subscription_id,pa.subscription_id) AS subscription_id,
 cb.main_currency AS currency,
 cb.payment_method AS payment_method,
 CASE
  WHEN p.paymenttype LIKE '%subscription%' 
   THEN ps.amount_paid
  WHEN p.paymenttype LIKE '%purchase%' 
   THEN pa.amount_paid
 ELSE cb.final_chargeback_amount*-1
 END AS amount,
 CASE
  WHEN p.paymenttype LIKE '%subscription%' 
   THEN ps.amount_paid
  WHEN p.paymenttype LIKE '%purchase%' 
   THEN pa.amount_paid
 ELSE cb.final_chargeback_amount * -1
 END AS amount_due,
 COALESCE(ps.amount_tax, pa.amount_tax) AS amount_tax,
 CASE
  WHEN p.paymenttype LIKE '%subscription%' 
   THEN ps.amount_paid
  WHEN p.paymenttype LIKE '%purchase%' 
   THEN pa.amount_paid
 ELSE cb.final_chargeback_amount * -1
 END AS amount_refunded,
 cb.final_chargeback_amount*-1 AS amount_refunded_sum,
 0 AS amount_repaid,
 NULL::TIMESTAMP AS paid_date,
 NULL::TIMESTAMP AS money_received_at,
 NULL::TIMESTAMP AS repaid_date,
 NULL::TIMESTAMP AS failed_date,
 NULL::TIMESTAMP AS cancelled_Date,
 NULL::TIMESTAMP AS pending_date,
 NULL AS reason,
 merchant_account AS capital_source,
 CASE
  WHEN p.paymenttype LIKE '%subscription%' 
   THEN 'SUBSCRIPTION_PAYMENT'
  WHEN p.paymenttype LIKE '%purchase%' 
   THEN 'ASSET_PAYMENT'
 ELSE p.paymenttype 
 END AS related_payment_type,
 UPPER(SPLIT_PART(p.paymentpurpose , ';', 1)) AS related_payment_type_detailed,
 d.tax_rate,
 o.store_country,
 'static_chb_source' AS src_tbl
FROM static_chargeback cb
  LEFT JOIN tr 
    ON cb.merchant_reference = tr.reference_id
  LEFT JOIN oltp_billing.payment_order p 
    ON p."group" = tr.account_to
  LEFT JOIN ods_production.payment_subscription ps
    ON ps.subscription_payment_id = p.uuid
  LEFT JOIN ods_production.payment_asset pa 
    ON pa.asset_payment_id = p.uuid
  LEFT JOIN deduping d
    ON p.ordernumber = d.order_id
  LEFT JOIN ods_production.ORDER o
    ON p.ordernumber = o.order_id
;