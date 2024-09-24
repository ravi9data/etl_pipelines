CREATE OR REPLACE VIEW dm_payments.v_outstanding_payments as
WITH sub AS (
SELECT 
  subscription_id 
 ,order_id
 ,subscription_sf_id
 ,currency
 ,customer_id 
 ,subscription_value 
 ,outstanding_assets 
 ,payment_count 
 ,status
 ,customer_type
 ,paid_subscriptions
 ,DATEDIFF('MONTH', start_date , CURRENT_DATE) AS actual_payment_count
 ,start_date AS subscription_start_date
 ,first_asset_delivery_date
 ,debt_collection_handover_date 
 ,minimum_term_months 
 ,minimum_cancellation_date
FROM master.subscription
WHERE status = 'ACTIVE'
)
,payments AS (
SELECT
  subscription_id
 ,MAX(due_date) AS last_due_date
 ,COUNT(CASE
   WHEN paid_date IS NOT NULL 
     OR Status = 'PAID' 
	THEN subscription_payment_id 
  END) AS paid_payments
FROM ods_production.payment_subscription 
GROUP BY 1
)
,last_payments AS (
SELECT DISTINCT 
  subscription_id 
 ,LAST_VALUE(subscription_payment_id) OVER (PARTITION BY subscription_id 
   ORDER BY due_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_payment_id
 ,LAST_VALUE(status) OVER (PARTITION BY subscription_id 
   ORDER BY due_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_payment_status
FROM ods_production.payment_subscription 
),
allocation_pre AS (
SELECT 
  a.subscription_id
 ,a.allocation_id
 ,a2.asset_id 
 ,a.allocation_status_original
 ,status_new
 ,a2.asset_status_original
 ,a2.asset_status_new
 ,a.delivered_at
 ,a.allocated_at
 ,a.refurbishment_end_at AS cancellation_approved_at
 ,COALESCE(a.return_delivery_date, a.return_delivery_date_old, a.return_processed_at
          ,a.refurbishment_start_at, a.in_stock_at) AS returned_date
 ,a2.residual_value_market_price
 ,a2.initial_price
 ,ROW_NUMBER() OVER (PARTITION BY a.subscription_id ORDER BY a.allocated_at DESC) idx
FROM ods_production.allocation a 
  LEFT JOIN master.asset a2 
    ON a.asset_id = a2.asset_id
WHERE TRUE 
  AND a.customer_id <>'29216' --Berit
  AND a2.warehouse NOT IN ('office_de')
)
,allocation AS (
SELECT * 
FROM allocation_pre 
WHERE TRUE 
  AND idx = 1
  AND returned_date IS NULL 
  AND delivered_at IS NOT NULL
)
SELECT 
  s.order_id
 ,s.subscription_id 
 ,a.allocation_id
 ,a.asset_id
 ,lp.last_payment_id
 ,s.customer_id 
 ,s.customer_type
 ,ord.store_country
 ,s.subscription_start_date
 ,s.first_asset_delivery_date
 ,a.delivered_at
 ,a.returned_date
 ,a.cancellation_approved_at
 ,a.asset_status_new
 ,a.asset_status_original
 ,s.status
 ,s.minimum_term_months
 ,s.minimum_cancellation_date
 ,s.debt_collection_handover_date
 ,s.subscription_value 
 ,s.payment_count
 ,p.last_due_date
 ,lp.last_payment_status
 ,s.paid_subscriptions 
 ,p.paid_payments
 ,s.actual_payment_count
 ,DATE_DIFF('DAY', p.last_due_date, CURRENT_DATE) AS payment_stopped_since_day
 ,DATE_DIFF('MONTH', p.last_due_date, CURRENT_DATE) AS payment_stopped_since_month
 ,a.allocation_status_original
 ,a.status_new
 ,s.currency
 ,payment_stopped_since_month * s.subscription_value AS sub_value_lost
 ,a.residual_value_market_price
 ,a.initial_price
 ,CASE 
   WHEN s.paid_subscriptions = 1 
    THEN '1-Paid subscription' 
   ELSE 
    CASE 
     WHEN a.delivered_at IS NOT NULL 
	   AND returned_date IS NULL 
	  THEN 'ACTIVE,PAYMENT BROKEN,OUTSTANDING ASSET' 
     WHEN a.delivered_at IS NOT NULL 
	   AND a.returned_date IS NOT NULL 
	  THEN 'ACTIVE,PAYMENT BROKEN,ASSET RETURNED' 
     WHEN a.allocated_at IS NOT NULL 
	   AND a.delivered_at IS NULL 
	  THEN 'ACTIVE, PAYMENT BROKEN,ALLOCATED,NOT DELIVERED'
     ELSE 'DATA ISSUE'
    END
   END AS subs_label
FROM sub s 
  INNER JOIN payments p 
    ON s.subscription_id = p.subscription_id
  INNER JOIN last_payments lp 
    ON s.subscription_id = lp.subscription_id
  INNER JOIN allocation a
    ON s.subscription_id = a.subscription_id 
  LEFT JOIN ods_production.order ord 
    ON s.order_id = ord.order_id 	 
WHERE payment_stopped_since_day > 40 
WITH NO SCHEMA BINDING;
          
GRANT SELECT ON dm_payments.v_outstanding_payments TO tableau; 