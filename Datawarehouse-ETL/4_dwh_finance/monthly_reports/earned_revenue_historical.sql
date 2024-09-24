DELETE FROM dwh.earned_revenue_historical WHERE report_date = CURRENT_DATE;
INSERT INTO dwh.earned_revenue_historical
WITH voucher_type AS
(
SELECT
 *,
 CASE 
  WHEN DATE_TRUNC('month', subscription_start_date) = DATE_TRUNC('month', report_date)  AND payment_count < 2 AND voucher_count = 1 
   THEN 'New Subscription - Voucher Applied' 
  WHEN DATE_TRUNC('month', subscription_start_date) = DATE_TRUNC('month', report_date)  AND payment_count < 2 AND voucher_count = 0 
   THEN 'New Subscription - No Voucher Applied' 
  WHEN voucher_count = 1 
   THEN 'One Time Voucher'
  WHEN voucher_count > 1 
   THEN 'Recurring Voucher'
  WHEN voucher_count = 0  
   THEN 'No Voucher'
  ELSE 'LEAK' 
 END AS voucher_type
FROM (
 SELECT 
  sp.date AS report_date,
  sp.subscription_id ,
  MIN(s.start_date ::DATE) AS subscription_start_date,
  COUNT(sp.payment_id) AS payment_count,
  SUM(CASE WHEN amount_voucher < 0 THEN 1 ELSE 0 END) AS voucher_count
 FROM master.subscription_payment_historical sp
   LEFT JOIN master.subscription s
     ON s.subscription_id = sp.subscription_id 
WHERE sp.date = CURRENT_DATE - 1
GROUP BY 1,2
     )
)
,sub AS (
SELECT 
 current_date AS date,
 sp.billing_period_start::DATE AS fact_date,
 COALESCE(sp.country_name,s.country_name,'n/a') AS country_name,
 COALESCE(sp.payment_method_detailed,'n/a') AS payment_method,
 COALESCE(sp.currency,'n/a') AS currency,
 COALESCE(sp.capital_source,'n/a') AS capital_source,
 COALESCE(sp.customer_id::text,'n/a') AS customer_id,
 COALESCE(sp.payment_id) AS payment_id,
 COALESCE(sp.psp_reference) AS psp_reference_id,
 COALESCE(sp.invoice_number,'n/a') AS invoice_number,
 COALESCE(sp.invoice_date,sp.invoice_sent_date)::DATE AS invoice_date,
 COALESCE(sp.invoice_url,'n/a') AS invoice_url,
 COALESCE(s.category_name,sp.category_name,'n/a') AS category_name, 
 COALESCE(s.subcategory_name,sp.subcategory_name,'n/a') AS subcategory_name, 
 COALESCE(s.store_label,'n/a') AS store_label, 
 COALESCE(s.subscription_plan ,'n/a') AS subscription_plan,
 COALESCE(v.voucher_type ,'n/a') AS voucher_type, 
 AVG(CASE 
  WHEN sp.country_name = 'Austria' AND sp.due_date >= '2022-04-01' 
   THEN 0.20
  WHEN sp.country_name = 'Netherlands' AND sp.due_date >= '2022-04-01' 
   THEN 0.21
  WHEN sp.country_name = 'Spain' AND sp.due_date >= '2022-04-01' 
   THEN 0.21
  ELSE sp.tax_rate 
 END) AS vat_rate,
 SUM(sp.amount_due) AS earned_revenue_incl_vat,
 SUM(CASE 
  WHEN sp.country_name = 'United States'
   THEN (sp.amount_subscription * (1 + COALESCE(sp.tax_rate, 1)))
  ELSE sp.amount_subscription 
 END) AS amount_subscription,
 SUM(sp.amount_shipment) AS amount_shipment_sp,
 SUM(CASE 
  WHEN sp.country_name = 'United States'
   THEN (COALESCE(sp.amount_voucher, 0::NUMERIC) * (1 + COALESCE(sp.tax_rate, 1)))
    +  (COALESCE(sp.amount_discount, 0::NUMERIC) * (1 + COALESCE(sp.tax_rate, 1))) 
  ELSE COALESCE(sp.amount_voucher, 0::NUMERIC) + COALESCE(sp.amount_discount, 0::NUMERIC)
 END) AS amount_discount 
FROM master.subscription_payment sp
  LEFT JOIN master.subscription s 
    ON sp.subscription_id=s.subscription_id
  LEFT JOIN voucher_type v
    ON v.subscription_id= s.subscription_id 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
HAVING SUM(sp.amount_due) <> 0
  OR SUM(sp.amount_subscription) <> 0
  OR SUM(sp.amount_shipment) <> 0
  OR SUM(sp.amount_voucher) <> 0
  OR SUM(sp.amount_discount) <> 0 
) 
,overdue_fee AS (
SELECT 
 current_date AS date,
 sp.paid_date::DATE AS fact_date,
 COALESCE(sp.country_name,'n/a') AS country_name,
 COALESCE(sp.payment_method_detailed,'n/a') AS payment_method,
 COALESCE(sp.currency,'n/a') AS currency,
 COALESCE(sp.capital_source,'n/a') AS capital_source,
 COALESCE(sp.customer_id::text,'n/a') AS customer_id,
 COALESCE(sp.payment_id) AS payment_id,
 COALESCE(sp.psp_reference) AS psp_reference_id,
 COALESCE(sp.invoice_number,'n/a') AS invoice_number,
 COALESCE(sp.invoice_date,sp.invoice_sent_date)::DATE AS invoice_date,
 COALESCE(sp.invoice_url,'n/a') AS invoice_url,
 COALESCE(s.category_name,sp.category_name,'n/a') AS category_name,
 COALESCE(s.subcategory_name,sp.subcategory_name,'n/a') AS subcategory_name, 
 COALESCE(s.store_label,'n/a') AS store_label,
 COALESCE(s.subscription_plan ,'n/a') AS subscription_plan, 
 COALESCE(v.voucher_type ,'n/a') AS voucher_type, 
 AVG(CASE 
  WHEN sp.country_name = 'Austria' AND sp.due_date >= '2022-04-01' 
   THEN 0.20
  WHEN sp.country_name = 'Netherlands' AND sp.due_date >= '2022-04-01' 
   THEN 0.21
  WHEN sp.country_name = 'Spain' AND sp.due_date >= '2022-04-01' 
   THEN 0.21
  ELSE sp.tax_rate 
 END) AS vat_rate,
 COALESCE(SUM(sp.amount_overdue_fee),0) AS amount_overdue_fee
FROM master.subscription_payment sp
  LEFT JOIN master.subscription s 
    ON sp.subscription_id=s.subscription_id
  LEFT JOIN ods_production.store st 
    ON st.id=s.store_id
 LEFT JOIN voucher_type v
    ON v.subscription_id= s.subscription_id 
WHERE TRUE 
  AND sp.paid_date::DATE <= CURRENT_DATE - 1
  AND sp.paid_date::DATE IS NOT NULL 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
HAVING COALESCE(SUM(sp.amount_overdue_fee),0) <> 0
)
,refunds AS (
SELECT DISTINCT
 current_date AS date,
 r.paid_date::DATE AS fact_date,
 COALESCE(r.store_country,st.country_name,'n/a') AS country_name,
 COALESCE(r.payment_method,'n/a') AS payment_method,
 COALESCE(r.currency,'n/a') AS currency,
 COALESCE(r.capital_source,'n/a') AS capital_source,
 COALESCE(r.customer_id::text,'n/a') AS customer_id,
 COALESCE(r.refund_payment_id) AS payment_id,
 COALESCE(r.psp_reference_id) AS psp_reference_id,
 COALESCE(ps.invoice_number,pa.invoice_number,'n/a') AS invoice_number,
 COALESCE(ps.invoice_date,ps.invoice_sent_date,pa.invoice_date,pa.invoice_sent_date)::DATE AS invoice_date,
 COALESCE(ps.invoice_url,pa.invoice_url,'n/a') AS invoice_url,
 COALESCE(s.category_name,ps.category_name,'n/a') AS category_name,
 COALESCE(s.subcategory_name,ps.subcategory_name,'n/a') AS subcategory_name, 
 COALESCE(s.store_label,ps.store_label,o.store_label,'n/a') AS store_label,
 COALESCE(s.subscription_plan ,ps.subscription_plan,'n/a') AS subscription_plan, 
 COALESCE(v.voucher_type ,'n/a') AS voucher_type, 
 SUM(CASE 
  WHEN r.status = 'PAID' AND r.refund_type = 'REFUND' 
   THEN -1 * r.amount_refunded 
  ELSE 0 
 END) AS amount_refund_paid,
 SUM(CASE 
  WHEN r.status = 'PAID' AND r.refund_type = 'REFUND' AND r.RELATED_PAYMENT_TYPE = 'SUBSCRIPTION_PAYMENT'  
   THEN -1 * r.amount_refunded 
  ELSE 0 
 END) AS amount_refund_paid_SUB_PAYMENT,
 SUM(CASE 
  WHEN r.status = 'PAID' AND r.refund_type = 'REFUND' AND r.related_payment_type_detailed='SHIPMENT' 
   THEN -1 * r.amount_refunded 
  ELSE 0 
 END) AS amount_refund_paid_shipment_payment,
 SUM(CASE 
  WHEN r.status = 'PAID' AND r.refund_type = 'CHARGE BACK' 
    AND (r.RELATED_PAYMENT_TYPE = 'SUBSCRIPTION_PAYMENT' OR r.related_payment_type_detailed='SHIPMENT') 
   THEN -1 * r.amount_refunded 
  ELSE 0 
 END) AS amount_chb_paid_sub_payment,
 SUM(CASE 
  WHEN r.status = 'PAID' AND r.refund_type = 'REFUND' AND r.RELATED_PAYMENT_TYPE = 'ASSET_PAYMENT' 
    AND r.related_payment_type_detailed NOT IN ('SHIPMENT') 
   THEN -1 * r.amount_refunded 
  ELSE 0 
 END) AS amount_refund_paid_asset_payment,
 SUM(CASE 
  WHEN r.status = 'PAID' AND r.refund_type = 'CHARGE BACK' AND r.RELATED_PAYMENT_TYPE = 'ASSET_PAYMENT' 
    AND r.related_payment_type_detailed NOT IN ('SHIPMENT') 
   THEN -1 * amount_refunded 
  ELSE 0 
 END) AS amount_chb_paid_asset_payment,
 AVG(COALESCE(r.tax_rate,
 (CASE 
   WHEN pa.payment_type in ('COMPENSATION') 
    THEN 0
   WHEN COALESCE(st.country_name,'n/a') IN ('Austria')
     AND pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
    THEN 0.2
   WHEN COALESCE(st.country_name,'n/a') IN ('Netherlands')
     AND pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
    THEN 0.21
   WHEN COALESCE(st.country_name,'n/a') IN ('Spain')
     AND pa.payment_type in ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
    THEN 0.21  
   WHEN r.paid_date::DATE BETWEEN '2020-07-01' AND '2020-12-31'
    THEN 0.16
   WHEN r.paid_date::DATE < '2020-07-01'
    THEN 0.19
   WHEN r.paid_date::DATE >= '2021-01-01'
    THEN 0.19
   WHEN pa.paid_date::DATE BETWEEN '2020-07-01' AND '2020-12-31'
    THEN 0.16
   WHEN pa.paid_date::DATE < '2020-07-01'
    THEN 0.19
   WHEN pa.paid_date::DATE >= '2021-01-01'
    THEN 0.19
 END) 
 )) AS vat_rate
FROM master.refund_payment r 
  LEFT JOIN master.asset_payment pa 
    ON pa.asset_payment_id = r.asset_payment_id
  LEFT JOIN master.subscription_payment ps 
    ON ps.payment_id = r.subscription_payment_id
  LEFT JOIN master.order o 
    ON pa.order_id = o.order_id
  LEFT JOIN master.subscription s 
    ON COALESCE(r.subscription_id,pa.subscription_id,ps.subscription_id)= s.subscription_id
  LEFT JOIN ods_production.store st 
    ON st.id = COALESCE(s.store_id,o.store_id)
  LEFT JOIN voucher_type v
    ON v.subscription_id= s.subscription_id 
WHERE TRUE 
  AND r.paid_date::DATE <= CURRENT_DATE - 1
  AND r.paid_date::DATE IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
HAVING amount_refund_paid <> 0
  OR amount_refund_paid_sub_payment <> 0
  OR amount_refund_paid_shipment_payment <> 0
  OR amount_chb_paid_sub_payment <> 0
  OR amount_refund_paid_asset_payment <> 0
  OR amount_chb_paid_asset_payment <> 0  
)
,asset_due AS (
SELECT 
 current_date AS date,
 pa.due_date::DATE AS fact_date,
 COALESCE(st.country_name,'n/a') AS country_name,
 COALESCE(pa.payment_method,'n/a') AS payment_method,
 COALESCE(pa.currency,'n/a') AS currency,
 COALESCE(pa.capital_source,'n/a') AS capital_source,
 COALESCE(pa.customer_id::text,'n/a') AS customer_id,
 COALESCE(pa.asset_payment_id) AS payment_id,
 COALESCE(pa.psp_reference_id) AS psp_reference_id,
 COALESCE(pa.invoice_number,'n/a') AS invoice_number,
 COALESCE(pa.invoice_date,pa.invoice_sent_date)::DATE AS invoice_date,
 COALESCE(pa.invoice_url,'n/a') AS invoice_url,
 COALESCE(s.category_name,a.category_name,'n/a') AS category_name, 
 COALESCE(s.subcategory_name,a.subcategory_name,'n/a') AS subcategory_name,
 COALESCE(s.store_label,o.store_label,'n/a') AS store_label,
 COALESCE(s.subscription_plan ,'n/a') AS subscription_plan, 
 COALESCE(v.voucher_type,'n/a') AS voucher_type, 
 SUM(CASE
  WHEN pa.payment_type = 'SHIPMENT' 
   THEN pa.amount_due
  ELSE 0::NUMERIC
 END) AS amount_shipment_ap,
 AVG(COALESCE(pa.tax_rate,
 (CASE  
   WHEN pa.payment_type IN ('COMPENSATION') 
    THEN 0
  WHEN COALESCE(st.country_name,'n/a') IN ('United States')
    AND pa.payment_type IN ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
    AND pa.paid_date::DATE > '2021-01-01'
   THEN pa.tax_rate      
   WHEN COALESCE(st.country_name,'n/a') IN ('Austria')
     AND pa.payment_type IN ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
    THEN 0.2
   WHEN COALESCE(st.country_name,'n/a') IN ('Netherlands')
     AND pa.payment_type IN ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
    THEN 0.21
   WHEN COALESCE(st.country_name,'n/a') IN ('Spain')
     AND pa.payment_type IN ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
    THEN 0.21  
   WHEN pa.paid_date::DATE BETWEEN '2020-07-01' AND '2020-12-31'
    THEN 0.16
   WHEN pa.paid_date::DATE < '2020-07-01'
    THEN 0.19
   WHEN pa.paid_date::DATE >= '2021-01-01'
    THEN 0.19
 END) 
 )) AS vat_rate
FROM master.asset_payment pa 
 LEFT JOIN master.asset a 
    ON a.asset_id=pa.asset_id
  LEFT JOIN master.subscription s 
    ON pa.subscription_id=s.subscription_id
  LEFT JOIN master.ORDER o 
    ON pa.order_id = o.order_id
  LEFT JOIN ods_production.store st 
    ON st.id=COALESCE(s.store_id,o.store_id)
  LEFT JOIN voucher_type v
    ON v.subscription_id= s.subscription_id 
WHERE TRUE 
  AND pa.paid_date::DATE <= CURRENT_DATE - 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
HAVING amount_shipment_ap <> 0
)
,asset_paid AS (
SELECT 
 current_date AS date,
 pa.paid_date::DATE AS fact_date,
 CASE 
  WHEN pa.payment_type in ('GROVER SOLD') 
   THEN 'Germany' 
  ELSE COALESCE(st.country_name,'n/a')
   END AS country_name,
 COALESCE(pa.payment_method,'n/a') AS payment_method,
 COALESCE(pa.currency,'n/a') AS currency,
 COALESCE(pa.capital_source,'n/a') AS capital_source,
 COALESCE(pa.customer_id::text,'n/a') AS customer_id,
 COALESCE(pa.asset_payment_id) AS payment_id,
 COALESCE(pa.psp_reference_id) AS psp_reference_id,
 COALESCE(pa.invoice_number,'n/a') AS invoice_number,
 COALESCE(pa.invoice_date,pa.invoice_sent_date)::DATE AS invoice_date,
 COALESCE(pa.invoice_url,'n/a') AS invoice_url,
 COALESCE(s.category_name,a.category_name,'n/a') AS category_name,
 COALESCE(s.subcategory_name,a.subcategory_name,'n/a') AS subcategory_name,
 COALESCE(s.store_label,'n/a') AS store_label,
 COALESCE(s.subscription_plan ,'n/a') AS subscription_plan, 
 COALESCE(v.voucher_type ,'n/a') AS voucher_type, 
 SUM(CASE
  WHEN pa.payment_type::text = 'REPAIR COST'::text 
   THEN pa.amount_paid
  ELSE 0
 END) AS repair_cost_collected_revenue,
 SUM(CASE
  WHEN pa.payment_type IN ('CUSTOMER BOUGHT') 
   THEN pa.amount_paid
  ELSE 0::NUMERIC
 END) AS asset_sales_customer_bought_collected_revenue,
 SUM(CASE
  WHEN pa.payment_type IN ('GROVER SOLD') 
   THEN pa.amount_paid
  ELSE 0::NUMERIC
 SUM(CASE
  WHEN pa.payment_type IN ('ADDITIONAL CHARGE')
   THEN pa.amount_paid
  ELSE 0::NUMERIC
 END) AS additional_charge_collected_revenue,
 SUM(CASE
  WHEN pa.payment_type IN ('COMPENSATION')
   THEN pa.amount_paid
  ELSE 0::NUMERIC
 END) AS compensation_collected_revenue,
 AVG(CASE  
  WHEN payment_type IN ('COMPENSATION') 
   THEN 0
  WHEN COALESCE(st.country_name,'n/a') IN ('United States')
    AND pa.payment_type IN ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
    AND pa.paid_date::DATE > '2021-01-01'
   THEN pa.tax_rate  
  WHEN COALESCE(st.country_name,'n/a') IN ('Austria')
    AND pa.payment_type IN ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
   THEN 0.2
  WHEN COALESCE(st.country_name,'n/a') IN ('Netherlands')
    AND pa.payment_type IN ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
   THEN 0.21
  WHEN COALESCE(st.country_name,'n/a') IN ('Spain')
    AND pa.payment_type IN ('GROVER SOLD','CUSTOMER BOUGHT','DEBT COLLECTION')
   THEN 0.21  
  WHEN pa.paid_date::DATE BETWEEN '2020-07-01' AND '2020-12-31'
   THEN 0.16
  WHEN pa.paid_date::DATE < '2020-07-01'
   THEN 0.19
  WHEN pa.paid_date::DATE >= '2021-01-01'
   THEN 0.19
 END) AS vat_rate
FROM master.asset_payment pa 
  LEFT JOIN master.asset a 
    ON a.asset_id=pa.asset_id
  LEFT JOIN master.subscription s 
    ON pa.subscription_id=s.subscription_id
  LEFT JOIN master.order o 
    ON pa.order_id = o.order_id
  LEFT JOIN ods_production.store st 
    ON st.id=COALESCE(s.store_id,o.store_id)
  LEFT JOIN voucher_type v
    ON v.subscription_id= s.subscription_id 
WHERE TRUE 
  AND pa.paid_date::DATE <= CURRENT_DATE - 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
HAVING repair_cost_collected_revenue <> 0 
  OR asset_sales_customer_bought_collected_revenue <> 0 
  OR additional_charge_collected_revenue <> 0 
  OR compensation_collected_revenue <> 0
  )
,addon AS (
SELECT 
 current_date AS date,
 pa.paid_date::DATE AS fact_date,
 pa.country_name,
 COALESCE(pa.payment_method,'n/a') AS payment_method,
 COALESCE(pa.currency,'n/a') AS currency,
 null AS capital_source,
 COALESCE(pa.customer_id::text,'n/a') AS customer_id,
 COALESCE(pa.payment_id) AS payment_id,
 COALESCE(pa.psp_reference_id) AS psp_reference_id,
 COALESCE(pa.invoice_number,'n/a') AS invoice_number,
 COALESCE(pa.invoice_date)::DATE AS invoice_date,
 COALESCE(pa.invoice_url,'n/a') AS invoice_url,
 COALESCE(a.category_name,'n/a') AS category_name,
 COALESCE(a.subcategory_name,'n/a') AS subcategory_name, 
 COALESCE(o.store_label,'n/a') AS store_label,
 'n/a' AS subscription_plan, 
 'n/a' AS voucher_type, 
 SUM(CASE
  WHEN pa.payment_type::text = 'ADDON-PURCHASE'::text 
   THEN pa.amount_paid
  ELSE 0
 END) AS amount_addon,
tax_rate AS vat_rate
FROM ods_production.payment_addon pa 
  LEFT JOIN ods_production.addon a 
    ON a.addon_id=pa.addon_id
  LEFT JOIN master.order o 
    ON pa.order_id = o.order_id
WHERE TRUE 
  AND pa.paid_date::DATE <= CURRENT_DATE - 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,19
HAVING amount_addon <>0
)
SELECT 
 current_date AS date,
 gc.paid_date::DATE AS fact_date,
 gc.country_name,
 COALESCE(gc.payment_method,'n/a') AS payment_method,
 COALESCE(gc.currency,'n/a') AS currency,
 NULL AS capital_source,
 COALESCE(gc.customer_id::text,'n/a') AS customer_id,
 COALESCE(gc.payment_id) AS payment_id,
 COALESCE(gc.psp_reference_id) AS psp_reference_id,
 COALESCE(gc.invoice_number,'n/a') AS invoice_number,
 COALESCE(gc.invoice_date)::DATE AS invoice_date,
 COALESCE(gc.invoice_url,'n/a') AS invoice_url,
 'n/a' AS category_name,
 'n/a' AS subcategory_name, 
 COALESCE(o.store_label,'n/a') AS store_label,
 'n/a' AS subscription_plan, 
 'n/a' AS voucher_type, 
 gc.tax_rate AS vat_rate
  LEFT JOIN master.order o 
    ON gc.order_id = o.order_id
WHERE TRUE 
  AND gc.paid_date::DATE <= CURRENT_DATE - 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,19
)
,thirtyfiveup AS (
SELECT 
 current_date AS date,
 th.paid_date::DATE AS fact_date,
 th.country_name,
 COALESCE(th.payment_method,'n/a') AS payment_method,
 COALESCE(th.currency,'n/a') AS currency,
 NULL AS capital_source,
 COALESCE(th.customer_id::text,'n/a') AS customer_id,
 COALESCE(th.payment_id) AS payment_id,
 COALESCE(th.psp_reference_id) AS psp_reference_id,
 COALESCE(th.invoice_number,'n/a') AS invoice_number,
 COALESCE(th.invoice_date)::DATE AS invoice_date,
 COALESCE(th.invoice_url,'n/a') AS invoice_url,
 'n/a' AS category_name,
 'n/a' AS subcategory_name, 
 COALESCE(o.store_label,'n/a') AS store_label,
 'n/a' AS subscription_plan, 
 'n/a' AS voucher_type, 
 SUM(th.amount_paid) AS amount_thirtyfiveup,
 th.tax_rate AS vat_rate
FROM ods_production.payment_addon_35up AS th 
  LEFT JOIN master.order o 
    ON th.order_id = o.order_id
WHERE TRUE 
  AND th.paid_date::DATE <= CURRENT_DATE - 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,19
HAVING amount_thirtyfiveup <> 0  
)
SELECT 
 COALESCE(s.date, a2.date, a.date, r.date, o.date, ad.date , gc.date, th.date) AS report_date,
 COALESCE(s.fact_date, a2.fact_date, a.fact_date, r.fact_Date, o.fact_date, ad.fact_date, gc.fact_date, th.fact_date) AS fact_date,
 COALESCE(s.country_name, a2.country_name,a.country_name,r.country_name,o.country_name, ad.country_name, gc.country_name, th.country_name, 'n/a') AS country_name,
 COALESCE(s.currency, a2.currency,a.currency,r.currency,o.currency, ad.currency, gc.currency, th.currency, 'n/a') AS currency,
 COALESCE(s.capital_source, a2.capital_source,a.capital_source,r.capital_source,o.capital_source, ad.capital_source, gc.capital_source, th.capital_source,'n/a') AS capital_source,
 COALESCE(s.category_name, a2.category_name,a.category_name,r.category_name,o.category_name, ad.category_name, gc.category_name, th.subcategory_name,'n/a') AS category_name,
 COALESCE(s.subcategory_name, a2.subcategory_name,a.subcategory_name,r.subcategory_name,o.subcategory_name, ad.subcategory_name, gc.subcategory_name, th.subcategory_name,'n/a') AS subcategory_name,
 COALESCE(s.payment_method, a2.payment_method,a.payment_method,r.payment_method,o.payment_method, ad.payment_method, gc.payment_method, th.payment_method, 'n/a') AS payment_method,
 COALESCE(s.customer_id, a2.customer_id,a.customer_id,r.customer_id,o.customer_id, ad.customer_id, gc.customer_id, th.customer_id,'n/a') AS customer_id,
 COALESCE(s.payment_id, a2.payment_id,a.payment_id,r.payment_id,o.payment_id, ad.payment_id, gc.payment_id, th.payment_id, 'n/a') AS payment_id,
 COALESCE(s.psp_reference_id, a2.psp_reference_id,a.psp_reference_id,r.psp_reference_id,o.psp_reference_id, ad.psp_reference_id, gc.psp_reference_id, th.psp_reference_id, 'n/a') AS psp_reference_id,
 COALESCE(s.invoice_number, a2.invoice_number,a.invoice_number,r.invoice_number,o.invoice_number, ad.invoice_number, gc.invoice_number, th.invoice_number, 'n/a') AS invoice_number,
 COALESCE(s.invoice_date, a2.invoice_date,a.invoice_date,r.invoice_date,o.invoice_date, ad.invoice_date, gc.invoice_date, th.invoice_date) AS invoice_date,
 COALESCE(s.invoice_url, a2.invoice_url,a.invoice_url,r.invoice_url,o.invoice_url, ad.invoice_url, gc.invoice_url, th.invoice_url, 'n/a') AS invoice_url,
 COALESCE(ch.customer_type,'normal_customer') AS customer_type,
 COALESCE(ch.company_name,'normal_customer') AS company_name,
 COALESCE(pii.tax_id,'n/a') AS tax_id,
 COALESCE(s.earned_revenue_incl_vat, 0) AS earned_sub_revenue_incl_vat,
 COALESCE(s.amount_subscription, 0) AS amount_subscription,
 COALESCE(s.amount_shipment_sp, 0)+COALESCE(a.amount_shipment_ap, 0) AS amount_shipment,
 COALESCE(s.amount_discount, 0) AS amount_discount,
 COALESCE(o.amount_overdue_fee, 0) AS amount_overdue_fee,
 COALESCE(ad.amount_addon, 0) AS addon_earned_revenue,
 COALESCE(th.amount_thirtyfiveup, 0) AS thirtyfiveup_earned_revenue, 
 COALESCE(a2.repair_cost_collected_revenue,0) AS repair_cost_earned_revenue,
 COALESCE(a2.asset_sales_customer_bought_collected_revenue, 0) AS asset_sales_customer_bought_earned_revenue,
 COALESCE(a2.additional_charge_collected_revenue, 0) AS additional_charge_earned_revenue,
 COALESCE(a2.compensation_collected_revenue,0) AS compensation_earned_revenue,
 COALESCE(r.amount_refund_paid, 0) AS refund_at_fact_Date,
 COALESCE(r.amount_refund_paid_sub_payment, 0) AS refund_sub_payment_at_fact_Date,
 COALESCE(r.amount_refund_paid_shipment_payment, 0) AS refund_shipment_payment_at_fact_Date,
 COALESCE(r.amount_refund_paid_asset_payment, 0) AS refund_asset_payment_at_fact_Date,
 COALESCE(r.amount_chb_paid_sub_payment, 0) AS chb_sub_payment_at_fact_date,
 COALESCE(r.amount_chb_paid_asset_payment, 0) AS chb_asset_payment_at_fact_date,
 COALESCE(s.vat_rate,a.vat_rate,a2.vat_rate,r.vat_rate,o.vat_rate, ad.vat_rate, gc.vat_rate, th.vat_rate) AS vat_rate,
 COALESCE(s.store_label, a2.store_label, a.store_label, r.store_label, o.store_label, ad.store_label, gc.store_label, th.store_label) AS store_label,
 COALESCE(s.subscription_plan, a2.subscription_plan, a.subscription_plan, r.subscription_plan, o.subscription_plan, ad.subscription_plan, gc.subscription_plan, th.subscription_plan) AS subscription_plan,
 COALESCE(s.voucher_type, a2.voucher_type, a.voucher_type, r.voucher_type, o.voucher_type, ad.voucher_type, gc.voucher_type, th.voucher_type) AS voucher_type
FROM sub s
  FULL JOIN asset_due a 
    ON s.fact_date = a.fact_date 
   AND s.currency::text = a.currency::text 
   AND s.capital_source::text = a.capital_source::text
   AND s.payment_method = a.payment_method
   AND s.customer_id = a.customer_id
   AND s.payment_id = a.payment_id
   AND s.invoice_number = a.invoice_number
   AND s.invoice_date = a.invoice_Date
   AND s.invoice_url = a.invoice_url
   AND s.country_name = a.country_name
   AND s.psp_reference_id = a.psp_reference_id
   AND s.category_name = a.category_name
   AND s.subcategory_name = a.subcategory_name
  FULL JOIN asset_paid a2 
    ON s.fact_date = a2.fact_date 
   AND s.currency::text = a2.currency::text 
   AND s.capital_source::text = a2.capital_source::text
   AND s.payment_method=a2.payment_method
   AND s.customer_id=a2.customer_id
   AND s.payment_id=a2.payment_id
   AND s.invoice_number=a2.invoice_number
   AND s.invoice_date=a2.invoice_Date
   AND s.invoice_url=a2.invoice_url
   AND s.country_name=a2.country_name
   AND s.psp_reference_id=a2.psp_reference_id
   AND s.category_name=a2.category_name
   AND s.subcategory_name = a2.subcategory_name
  FULL JOIN refunds r 
    ON r.fact_date = s.fact_date 
   AND r.currency = s.currency::text 
   AND r.capital_source::text = s.capital_source::text
   AND r.payment_method = s.payment_method
   AND r.customer_id = s.customer_id
   AND s.payment_id = r.payment_id
   AND s.invoice_number = r.invoice_number
   AND s.invoice_date = r.invoice_Date
   AND s.invoice_url = r.invoice_url
   AND s.country_name = r.country_name
   AND s.psp_reference_id = r.psp_reference_id
   AND s.category_name = r.category_name
   AND s.subcategory_name = r.subcategory_name
  FULL JOIN overdue_fee o 
    ON r.fact_date = o.fact_date 
   AND r.currency = o.currency::text 
   AND r.capital_source::text = o.capital_source::text
   AND r.payment_method = o.payment_method
   AND r.customer_id = o.customer_id
   AND s.payment_id = o.payment_id
   AND s.invoice_number = o.invoice_number
   AND s.invoice_date = o.invoice_Date
   AND s.invoice_url = o.invoice_url
   AND s.country_name = o.country_name
   AND s.psp_reference_id = o.psp_reference_id
   AND s.category_name = o.category_name
   AND s.subcategory_name = o.subcategory_name
  FULL JOIN addon ad
    ON s.fact_date = ad.fact_date 
   AND s.currency::text = ad.currency::text 
   AND s.capital_source::text = ad.capital_source::text
   AND s.payment_method = ad.payment_method
   AND s.customer_id = ad.customer_id
   AND s.payment_id = ad.payment_id
   AND s.invoice_number = ad.invoice_number
   AND s.invoice_date = ad.invoice_Date
   AND s.invoice_url = ad.invoice_url
   AND s.country_name = ad.country_name
   AND s.psp_reference_id = ad.psp_reference_id
   AND s.category_name = ad.category_name
   AND s.subcategory_name = ad.subcategory_name
    ON s.fact_date = gc.fact_date 
   AND s.currency::text = gc.currency::text 
   AND s.capital_source::text = gc.capital_source::text
   AND s.payment_method = gc.payment_method
   AND s.customer_id = gc.customer_id
   AND s.payment_id = gc.payment_id
   AND s.invoice_number = gc.invoice_number
   AND s.invoice_date = gc.invoice_Date
   AND s.invoice_url = gc.invoice_url
   AND s.country_name = gc.country_name
   AND s.psp_reference_id = gc.psp_reference_id
   AND s.category_name = gc.category_name
   AND s.subcategory_name = gc.subcategory_name 
  FULL JOIN thirtyfiveup th 
    ON s.fact_date = th.fact_date 
   AND s.currency::text = th.currency::text 
   AND s.capital_source::text = th.capital_source::text
   AND s.payment_method = th.payment_method
   AND s.customer_id = th.customer_id
   AND s.payment_id = th.payment_id
   AND s.invoice_number = th.invoice_number
   AND s.invoice_date = th.invoice_Date
   AND s.invoice_url = th.invoice_url
   AND s.country_name = th.country_name
   AND s.psp_reference_id = th.psp_reference_id
   AND s.category_name = th.category_name
   AND s.subcategory_name = th.subcategory_name   
  LEFT JOIN master.customer ch 
    ON COALESCE(ch.customer_id::text,'n/a') = COALESCE(a.customer_id, a2.customer_id,s.customer_id, r.customer_id, o.customer_id, ad.customer_id, gc.customer_id, th.customer_id, 'n/a')
  LEFT JOIN ods_data_sensitive.customer_pii pii 
    ON COALESCE(pii.customer_id::text,'n/a') = COALESCE(a.customer_id, a2.customer_id,s.customer_id, r.customer_id, o.customer_id, ad.customer_id, gc.customer_id, th.customer_id, 'n/a')
;