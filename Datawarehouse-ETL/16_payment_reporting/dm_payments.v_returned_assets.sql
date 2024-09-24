CREATE OR REPLACE VIEW dm_payments.v_returned_assets AS
WITH asset_allocation AS (
SELECT
  a.subscription_id,
  COUNT(*) AS cnt_allocation 
FROM ods_production.allocation a
  INNER JOIN master.subscription s 
    ON a.subscription_id = s.subscription_id
WHERE s.status = 'ACTIVE' 
GROUP BY 1
)
,returned_asset as (
SELECT
  a.subscription_id,
  COUNT(*) AS cnt_return
FROM ods_production.allocation a
  INNER JOIN master.subscription s 
    ON a.subscription_id = s.subscription_id
WHERE s.status = 'ACTIVE' 
  AND a.status_new = 'RETURNED'
GROUP BY 1 
)
,subscription_exclusion_list AS (
/*LETS EXCLUDE ACTIVE SUBS WITH MULTIPLE ALLOCATIONS BUT NOT RETURNED ALL ASSETS*/
SELECT DISTINCT a.subscription_id
FROM asset_allocation a 
  LEFT JOIN returned_asset b 
    ON a.subscription_id = b.subscription_id
WHERE TRUE
  AND a.cnt_allocation > 1 
  AND a.cnt_allocation <> NVL(b.cnt_return, 0)   
)
,last_payment AS (
SELECT 
  subscription_id,
  due_date,
  status AS payment_status
FROM master.subscription_payment
WHERE TRUE 
QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY due_date DESC) = 1
)
SELECT  
  s.customer_id,
  s.customer_type,
  s.store_type,
  s.country_name,
  a.order_id,
  s.subscription_id,
  s.subscription_sf_id,
  s.start_date,
  a.asset_id,
  a.allocation_id,
  a.allocation_sf_id,
  s.status AS subscription_status,
  a.allocation_status_original,
  a.status_new,
  a.asset_status,
  a.return_delivery_date,
  a.failed_delivery_at,
  a.widerruf_claim_date,
  a.refurbishment_end_at AS cancellation_approved_at,
  s.payment_method,
  s.subscription_value,
  s.currency,
  s.max_payment_number,
  s.minimum_cancellation_date,
  s.minimum_term_months,
  s.first_asset_delivery_date,
  s.first_asset_delivery_date + 14 as calculated_wiederruf_date,
  CASE 
   WHEN a.return_delivery_date::DATE - s.first_asset_delivery_date::DATE < 15 
    THEN 'YES' 
   ELSE 'NO' 
  END AS in_wiederruf_period,
  p.due_date::DATE AS last_due_date,
  p.payment_status AS last_payment_status
FROM ods_production.allocation a 
  INNER JOIN master.subscription s 
    ON a.subscription_id = s.subscription_id 
  LEFT JOIN subscription_exclusion_list sel
    ON a.subscription_id = sel.subscription_id
  LEFT JOIN last_payment p 
    ON a.subscription_id = p.subscription_id 
WHERE TRUE 
  AND s.status='ACTIVE' 
  AND a.return_delivery_date IS NOT NULL 
  AND sel.subscription_id IS NULL
ORDER BY s.start_date DESC
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_payments.v_returned_assets TO tableau;