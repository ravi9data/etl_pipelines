DROP TABLE IF EXISTS stg_curated.order_additional_info;
CREATE TABLE stg_curated.order_additional_info AS
WITH payment_base AS (
SELECT DISTINCT
 "group" AS payment_group_id
 ,ordernumber AS order_id
 ,MAX(updatedat) as max_updated_at
FROM oltp_billing.payment_order
GROUP BY 1,2
)
,payload_unnested AS (
SELECT
  c."group" AS payment_group_id
  /*SOME PAYMENT GROUPS HAVE SOME PAYMENTS WITH MISSING/0 TAX RATE. TO PREVENT IT, WE TAKE MAX*/ 
 ,MAX(JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(each_item), 'taxRate'))::DECIMAL(10,6) AS payment_group_tax_rate
 ,JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(each_item), 'tax'), 'currency') AS payment_group_tax_currency
 ,MAX(JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(each_item), 'shipping'), 'in_cents')) AS payment_group_shipping_in_cents
FROM oltp_billing.payment_order c
      ,c.payment_group_tax_breakdown AS each_item
GROUP BY 1,3
)
SELECT
  p.payment_group_id
 ,p.order_id
 ,u.payment_group_tax_rate
 ,u.payment_group_tax_currency
 ,u.payment_group_shipping_in_cents
 ,p.max_updated_at
FROM payment_base p
  LEFT JOIN payload_unnested u
    ON p.payment_group_id = u.payment_group_id
;
GRANT SELECT ON stg_curated.order_additional_info TO GROUP bi_finance;
