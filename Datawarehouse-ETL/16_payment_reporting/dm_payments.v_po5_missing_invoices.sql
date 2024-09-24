CREATE OR REPLACE VIEW dm_payments.v_po5_missing_invoices AS 
WITH all_payments AS  (
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
  AND (invoice_url IS NULL OR invoice_number IS NULL)
UNION ALL 
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
  AND payment_type = 'CUSTOMER BOUGHT'
  AND (invoice_url IS NULL OR invoice_number IS NULL)
)
SELECT 
  a.*,
  b.store_country,
  c.customer_type 
FROM all_payments a
  LEFT JOIN master.order b 
    ON a.order_id = b.order_id
  LEFT JOIN master.customer c 
    ON a.customer_id = c.customer_id 
WHERE a.status <> 'CANCELLED'
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_payments.v_po5_missing_invoices TO tableau;