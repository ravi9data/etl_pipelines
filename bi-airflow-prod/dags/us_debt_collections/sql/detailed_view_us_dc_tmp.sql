DROP TABLE IF EXISTS ods_production.detailed_view_us_dc_tmp;
CREATE TABLE ods_production.detailed_view_us_dc_tmp AS
WITH ord_ AS (
SELECT DISTINCT
 uuid,
 "group",
 price,
 amount,
 period,
 status,
 duedate,
 currency,
 quantity,
 createdat,
 lineitems,
 scopeuuid,
 updatedat,
 contractid,
 walletuuid,
 billingdate,
 countrycode,
 description,
 ordernumber,
 paymenttype,
 taxincluded,
 contracttype,
 shippingcost,
 groupstrategy,
 additionaldata,
 paymentpurpose
FROM oltp_billing.payment_order ord_
)
,order_ AS (
SELECT
 ordernumber,
 "group" payment_group,
 duedate,
 status order_status,
 currency,
 MIN(updatedat) order_min_updatedat,
 MAX(updatedat) order_max_updatedat,
 SUM(amount) order_amount,
 SUM(price) order_price
FROM ord_
GROUP BY 1, 2, 3, 4, 5
)
,transaction_ AS (
SELECT
 account_to,
 gateway_response,
 amount,
 status,
 failed_reason,
 IS_VALID_JSON(REPLACE(gateway_response, '\\\\', '')) is_valid_json_,
 status transaction_status,
 updated_at
FROM oltp_billing.transaction
)
,q9 AS (
SELECT DISTINCT
 order_.ordernumber,
 order_.contractid,
 transaction_.updated_at,
 CASE
  WHEN transaction_.status = 'failed'
    AND gateway_response LIKE '%errors%'
   THEN REPLACE(REGEXP_SUBSTR(
                (JSON_EXTRACT_PATH_TEXT(REPLACE(transaction_.gateway_response, '\\\\', ''), 'errors')),
                'message":"([^"])*'
            ),'message":"','') || '---' || REPLACE(
            REGEXP_SUBSTR((JSON_EXTRACT_PATH_TEXT(REPLACE(transaction_.gateway_response, '\\\\', ''), 'errors')
                ),'adapterMessage":"([^"])*'), 'adapterMessage":"', '')
  WHEN transaction_.status = 'failed'
    AND transaction_.gateway_response LIKE '%error%'
   THEN REPLACE(REGEXP_SUBSTR(
                (JSON_EXTRACT_PATH_TEXT(REPLACE(transaction_.gateway_response, '\\\\', ''), 'error')
                ),'message":"([^"])*'), 'message":"', '') || '---' ||
        REPLACE(REGEXP_SUBSTR(
                (JSON_EXTRACT_PATH_TEXT(REPLACE(transaction_.gateway_response, '\\\\', ''), 'error')
                ), 'adapterMessage":"([^"])*'), 'adapterMessage":"', '')
  WHEN transaction_.status = 'failed'
    AND transaction_.gateway_response LIKE '%message%'
   THEN JSON_EXTRACT_PATH_TEXT(REPLACE(transaction_.gateway_response, '\\\\', ''), 'message')
 END failed_response,
 transaction_.failed_reason,
 transaction_.status transaction_status,
 ROW_NUMBER() OVER (PARTITION BY contractid ORDER BY transaction_.updated_at DESC) last_error,
 ROW_NUMBER() OVER (PARTITION BY contractid ORDER BY transaction_.updated_at ASC) first_error
FROM ord_ order_
  INNER JOIN transaction_
    ON transaction_.account_to = order_."group"
WHERE order_.currency = 'USD'
  AND order_.status != 'new'
  AND COALESCE(IS_VALID_JSON(REPLACE(transaction_.gateway_response, '\\\\', '')), FALSE) IS NOT FALSE
  AND transaction_.status = 'failed'
)
,failed_payments AS (
SELECT
 ordernumber,
 contractid,
 MAX(CASE
  WHEN last_error = 1
   THEN COALESCE(failed_response, failed_reason)
 END) last_error,
 MAX(CASE
  WHEN last_error = 1
   THEN updated_at
 END) last_error_date
FROM q9
WHERE last_error = 1
GROUP BY 1, 2
)
,state_ AS (
SELECT
 JSON_EXTRACT_PATH_TEXT(payload, 'order_number') AS order_number,
 MAX(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'shipping_address'), 'state')) AS shipping_state,
 MAX(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'billing_address'),  'state')) AS billing_state
FROM stg_kafka_events_full.stream_internal_order_placed_v2 siop
WHERE JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'store'), 'country_code') = 'US'
GROUP BY 1
)
,rates_old AS (
SELECT
 JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'line_items'), 0), 'order_number') order_id,
 MAX(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'orders'), 0), 'tax_rate')) tax_rate
FROM trans_dev.internal_billing_payments v
WHERE TRUE
  AND JSON_EXTRACT_PATH_TEXT(payload, 'contract_type') = 'FLEX'
  AND JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'amount_due'), 'currency') = 'USD'
GROUP BY 1
)
, sales_tax AS (
SELECT
 subscription_id,
 MAX(amount_tax) amount_sales_tax
FROM master.subscription_payment
GROUP BY 1
)
, contract_status_ AS (
SELECT
 JSON_EXTRACT_PATH_TEXT(payload, 'order_number') order_number,
 JSON_EXTRACT_PATH_TEXT(payload, 'id') contract_id,
 JSON_EXTRACT_PATH_TEXT(payload, 'state') state,
 event_name event_name_,
 event_timestamp event_timestamp_,
 JSON_EXTRACT_PATH_TEXT(payload, 'created_at') created_at,
 JSON_EXTRACT_PATH_TEXT(payload, 'activated_at') activated_at,
 JSON_EXTRACT_PATH_TEXT(payload, 'terminated_at') terminated_at,
 JSON_EXTRACT_PATH_TEXT(payload, 'termination_reason') termination_reason,
 ROW_NUMBER() OVER (PARTITION BY contract_id ORDER BY event_timestamp DESC) row_n
 FROM stg_kafka_events_full.stream_customers_contracts_v2
 WHERE TRUE
  AND JSON_EXTRACT_PATH_TEXT(payload, 'type') = 'flex'
  AND event_name NOT IN ('revocation_expired', 'purchase_failed')
)
, numbers_ as (
SELECT *
FROM public.numbers
WHERE ordinal < 100
)
,paid_payments AS (
SELECT DISTINCT
 JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(payload, 'line_items'), 0), 'order_number') order_number,
 JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(
   JSON_EXTRACT_PATH_TEXT(payload, 'line_items'), 0), 'contract_ids'), numbers_.ordinal::INT, TRUE) contract_id,
 COUNT(DISTINCT JSON_EXTRACT_PATH_TEXT(payload, 'due_date')) total_number_payments,
 COUNT(DISTINCT CASE
  WHEN event_name = 'paid'
   THEN JSON_EXTRACT_PATH_TEXT(payload, 'due_date')
 END) paid_payments
FROM trans_dev.internal_billing_payments v2
  CROSS JOIN numbers_
WHERE TRUE
  AND numbers_.ordinal < JSON_ARRAY_LENGTH(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(
    JSON_EXTRACT_PATH_TEXT(payload, 'line_items'), 0), 'contract_ids'), TRUE)
  AND payload LIKE '%USD%'
GROUP BY 1, 2
)
,linear_dep AS (
SELECT
  s.subscription_id
 ,a1.allocation_id
 ,a2.asset_id
 ,a2.asset_value_linear_depr
 ,ROW_NUMBER() OVER (PARTITION BY s.subscription_id ORDER BY a1.allocated_at DESC) row_n
FROM master.subscription s
  LEFT JOIN master.allocation a1
    ON s.subscription_id = a1.subscription_id
  LEFT JOIN master.asset a2
    ON a1.asset_id= a2.asset_id
)
,outst_amount AS (
SELECT
 contractid,
 SUM(amount) outstanding_amount
FROM oltp_billing.payment_order
WHERE TRUE
  AND status = 'failed'
  AND NOT (paymenttype = 'purchase'  AND status = 'failed')
GROUP BY 1
)
, most_frequent_amount_due AS (
SELECT DISTINCT
  subscription_id, 
  amount_due, 
  amount_voucher AS discount_amount,
  COUNT(*) nr_
FROM master.subscription_payment sp 
GROUP BY 1,2,3
)
, most_frequent_amount_due_final AS (
SELECT * 
FROM most_frequent_amount_due
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY nr_ DESC) = 1
)
, paid_amount_pre_tax AS (
SELECT 
  ps.subscription_id,
  ps.amount_paid,
  st.amount_sales_tax,
  CASE WHEN ps.amount_paid <> 0 
         THEN ps.amount_paid - st.amount_sales_tax
       ELSE 0
  END AS paid_amount_excl_tax
FROM ods_production.payment_subscription ps 
  LEFT JOIN sales_tax st
    ON ps.subscription_id = st.subscription_id
)
,paid_amount AS (
SELECT 
  subscription_id,
  SUM(paid_amount_excl_tax) AS total_paid_amount_excl_sales_tax,
  SUM(amount_paid) AS total_paid_amount_incl_sales_tax
FROM paid_amount_pre_tax
GROUP BY 1
)
,remaining_purchase_price AS (
SELECT 
  s.subscription_id,
  pa.total_paid_amount_excl_sales_tax,
  pa.total_paid_amount_incl_sales_tax,
  (f.amount_due - st.amount_sales_tax) * (s.months_required_to_own::INT + 1) AS start_purchase_price_excl_sales_tax,
  f.amount_due * (s.months_required_to_own::INT + 1) AS start_purchase_price_incl_sales_tax,
  ROUND(CASE 
          WHEN s.order_created_date::DATE < '2023-07-25' AND start_purchase_price_excl_sales_tax - pa.total_paid_amount_excl_sales_tax < 1
            THEN 1
          WHEN s.order_created_date::DATE < '2023-07-25' THEN start_purchase_price_excl_sales_tax - pa.total_paid_amount_excl_sales_tax
        END, 2) AS remaining_purchase_price_excl_sales_tax,
  ROUND(CASE 
          WHEN s.order_created_date::DATE < '2023-07-25' AND start_purchase_price_incl_sales_tax - pa.total_paid_amount_incl_sales_tax < 1
            THEN 1
          WHEN s.order_created_date::DATE < '2023-07-25' THEN start_purchase_price_incl_sales_tax - pa.total_paid_amount_incl_sales_tax
        END, 2) AS remaining_purchase_price_incl_sales_tax      
FROM master.subscription s
  LEFT JOIN paid_amount pa
    ON s.subscription_id = pa.subscription_id
  LEFT JOIN most_frequent_amount_due_final f
    ON s.subscription_id = f.subscription_id
  LEFT JOIN  sales_tax st
    ON s.subscription_id = st.subscription_id
)
,product_names AS (
SELECT
  s.subscription_id
 ,s.product_sku
 ,a.product_sku
 ,p.product_sku
 ,p.product_name
 ,p.serial_number
 ,a.allocated_at
FROM master.subscription s
  LEFT JOIN master.allocation a
    ON s.subscription_id = a.subscription_id
  LEFT JOIN master.asset p
    ON a.asset_id = p.asset_id 
QUALIFY ROW_NUMBER()OVER(PARTITION BY a.subscription_id ORDER BY a.allocated_at DESC) = 1 
)
SELECT DISTINCT
 a.order_id,
 a.subscription_id,
 a.order_created_date::DATE,
 c.customer_id,
 sc.default_date::DATE due_date,
 CASE
  WHEN sc.default_date::DATE != last_payment.due_date::DATE THEN last_payment.due_date::DATE
 END due_date_new,
 CASE
  WHEN a.outstanding_subscription_revenue > 0 THEN DATE_DIFF('d', sc.default_date::DATE, CURRENT_DATE)
  ELSE 0
 END DPD,
 updated_dpd_.adjusted_dpd dpd_new,
 TRUNC(a.subscription_value, 2) subscription_value,
 sales_tax.amount_sales_tax tax_amount,
 COALESCE(TRUNC(outst_amount.outstanding_amount, 2), 0) outstanding_subscription_revenue,
 pp.total_paid_amount_excl_sales_tax,
 pp.total_paid_amount_incl_sales_tax,
 pp.start_purchase_price_excl_sales_tax,
 pp.start_purchase_price_incl_sales_tax,
 pp.remaining_purchase_price_excl_sales_tax,
 pp.remaining_purchase_price_incl_sales_tax,
 c.first_name::TEXT,
 c.last_name::TEXT,
 c.email,
 c.phone_number,
 o.billingcity AS billing_city,
 o.billingpostalcode AS billing_zip,
 o.billingstreet AS billing_street,
 state_.billing_state::TEXT,
 o.shippingcity AS shipping_city,
 o.shippingpostalcode AS shipping_zip,
 o.shippingstreet AS shipping_street,
 state_.shipping_state::TEXT,
 CASE
  WHEN contract_status_.event_name_ = 'ended'
   THEN 'ENDED'
  WHEN a.status = 'CANCELLED'
   THEN 'CANCELLED'
  WHEN sc.default_date::DATE != last_payment.due_date::DATE
   THEN a.status || '_DPD'
  ELSE a.status
 END order_status,
 contract_status_.event_name_ contract_status,
 a.subscription_plan,
 product_names.product_name AS outstanding_products,
 product_names.serial_number,
 updated_dpd_.total_nr_payments total_nr_payments,
 updated_dpd_.nr_successful_payments paid_payments,
 a.new_recurring AS customer_type,
 a.start_date::DATE AS subscription_start_date,
 a.payment_method AS payment_method,
 linear_dep.asset_value_linear_depr AS "asset_value_linear_depr (3% discount)",
 a.customer_type person_business,
 SUBSTRING(REPLACE(failed_payments.last_error, '-', '_'),  1, 30) AS last_failed_reason,
 SUBSTRING(CONVERT_TIMEZONE('EST', failed_payments.last_error_date::TIMESTAMP), 1, 19) AS last_failed_date,
 CASE
  WHEN (sc.outstanding_subscription_revenue > 0::NUMERIC)
   THEN 1
  ELSE 0
 END show_dc
FROM master.subscription a
  LEFT JOIN ods_data_sensitive.customer_pii c
    ON c.customer_id = a.customer_id
  LEFT JOIN ods_production.customer_scoring cs
    ON a.customer_id = cs.customer_id
  LEFT JOIN ods_production.subscription_cashflow sc
    ON sc.subscription_id = a.subscription_id
  LEFT JOIN ods_production.payment_subscription ps
    ON a.subscription_id = ps.subscription_id
  LEFT JOIN state_
    ON a.order_id::TEXT = state_.order_number::TEXT
  LEFT JOIN failed_payments
    ON a.subscription_id = failed_payments.contractid
  LEFT JOIN contract_status_
    ON a.subscription_id = contract_status_.contract_id
   AND contract_status_.row_n = 1
  LEFT JOIN ods_production.last_payment_event last_payment
    ON a.subscription_id = last_payment.contract_id
  LEFT JOIN linear_dep
    ON a.subscription_id = linear_dep.subscription_id
   AND linear_dep.row_n = 1
  LEFT JOIN ods_production.billing_service_updated_dpd updated_dpd_
    ON a.subscription_id = updated_dpd_.contractid
  LEFT JOIN sales_tax
    ON a.subscription_id = sales_tax.subscription_id
  LEFT JOIN outst_amount
    ON outst_amount.contractid = a.subscription_id
  LEFT JOIN most_frequent_amount_due_final ad
    ON a.subscription_id = ad.subscription_id 
  LEFT JOIN remaining_purchase_price pp
    ON a.subscription_id = pp.subscription_id 
  LEFT JOIN product_names
    ON a.subscription_id = product_names.subscription_id
  LEFT JOIN ods_production.ORDER o
    ON a.order_id = o.order_id
WHERE TRUE
  AND a.country_name = 'United States'
  AND a.created_date::DATE >= '2021-06-01'
  AND sc.default_date = ps.due_date
  AND COALESCE(updated_dpd_.total_nr_payments, 0) > 0
  ;