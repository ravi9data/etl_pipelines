CREATE OR REPLACE VIEW dm_payments.v_po5_missing_invoices_ratio AS
WITH all_payments AS (
SELECT 
  customer_id,
  order_id,
  payment_group_id,
  subscription_payment_id AS payment_id, 
  due_date::DATE,
  status, 
  payment_type,
  invoice_number,
  invoice_url,
  invoice_date,
  invoice_sent_date,
  amount_due,
  currency,
  amount_tax,
  tax_rate,
  src_tbl
FROM ods_production.payment_subscription 
WHERE TRUE  
  AND due_date::DATE < CURRENT_DATE - 1
  AND status <> 'CANCELLED'
UNION  ALL 
SELECT 
  customer_id,
  order_id,
  payment_group_id,
  asset_payment_id AS payment_id, 
  due_date::DATE,
  status, 
  payment_type,
  invoice_number,
  invoice_url,
  invoice_date,
  invoice_sent_date,
  amount_due,
  currency,
  amount_tax,
  tax_rate,
  src_tbl
FROM ods_production.payment_asset
WHERE TRUE  
  AND due_date::DATE < CURRENT_DATE - 1
  AND status <> 'CANCELLED'
  AND payment_type = 'CUSTOMER BOUGHT'
)
,final_raw AS (
SELECT 
  a.*,
  CASE
    WHEN invoice_url IS NULL 
      THEN 'invoice_url_null'
    WHEN invoice_number IS NULL 
      THEN  'invoice_nr_null' 
    ELSE 'invoice_not_null' 
  END AS invoice_flag,
  CASE 
    WHEN a.payment_type IN ('CUSTOMER BOUGHT', 'FIRST') 
     AND a.status IN ('PAID', 'PENDING', 'CHARGED BACK','REFUND','PARTIAL PAID') 
     OR a.payment_type IN ('RECURRENT','ADDITIONAL CHARGE')
      THEN 'include' 
    ELSE 'exclude' 
  END AS payment_flag,
  b.store_country,
  c.customer_type 
FROM all_payments a
  LEFT JOIN master.order b 
    ON a.order_id = b.order_id
  LEFT JOIN master.customer c 
    ON a.customer_id = c.customer_id 
)
SELECT 
  due_date, 
  store_country,
  customer_type,
  payment_type,
  status,
  src_tbl,
  COUNT(DISTINCT CASE 
                   WHEN invoice_flag IN ('invoice_nr_null', 'invoice_url_null') 
                     THEN payment_group_id 
                 END) AS cnt_null_invoices,
  COUNT(DISTINCT payment_group_id) AS cnt_payments
FROM final_raw 
WHERE TRUE  
  AND payment_flag = 'include'
  AND store_country IS NOT NULL 
GROUP BY 1,2,3,4,5,6 
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_payments.v_po5_missing_invoices_ratio TO tableau;