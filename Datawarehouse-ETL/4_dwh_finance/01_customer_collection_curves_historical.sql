DELETE FROM dm_finance.customer_collection_curves_historical
WHERE repoting_date = CURRENT_DATE - 1
  OR (repoting_date = CURRENT_DATE - 3 AND repoting_date <> LAST_DAY(repoting_date));


INSERT INTO dm_finance.customer_collection_curves_historical
WITH payment_all_historical AS (
SELECT *
FROM master.payment_all_historical
WHERE date = CURRENT_DATE - 1
)
SELECT DISTINCT 
 CURRENT_DATE - 1 AS repoting_date, 
 c.customer_acquisition_cohort::DATE AS customer_acquisition_cohort,
 s2.start_date::DATE AS first_due_date,
 s2.store_short AS customer_acquisition_store,
 s2.store_label AS customer_acquisition_store_label,
 s2.country_name AS customer_acquisition_country,
 s2.category_name AS customer_acquisition_category_name,
 s2.rental_period AS acquired_subscription_plan,
 c.customer_id,
 c.customer_type,
 COALESCE(s.cancellation_reason_churn, 'active') AS cancellation_reason_churn,
 p.order_id,
 o.status AS order_status,
 o.new_recurring,
 p.payment_id,
 p.status AS payment_status,
 p.payment_type,
 p.due_date,
 p.paid_date,
 p.amount_due,
 p.amount_paid,
 p.tax_rate,
 p.discount_amount AS discount_amount,
 GREATEST(p.due_date::DATE - c.customer_acquisition_cohort::DATE,0) AS days_between_acquisition_due,
 GREATEST(DATEDIFF(MONTH, c.customer_acquisition_cohort::DATE, p.due_date::DATE),0) AS months_between_acquisition_due,
 GREATEST(p.paid_date::DATE - c.customer_acquisition_cohort::DATE,0) AS days_between_acquisition_paid,
 s.subscription_value,
 (s.committed_sub_value + s.additional_committed_sub_value) as committed_sub_value,
 s.avg_asset_purchase_price,
 s.subscription_id,
 s.subscription_plan,
 CASE 
  WHEN payment_type IN ('FIRST','RECURRENT') AND p.status = 'PAID' 
   THEN p.amount_paid  
  ELSE 0 
 END AS subscription_revenue_paid,
 CASE 
  WHEN payment_type IN ('SHIPMENT') AND p.status = 'PAID' 
   THEN p.amount_paid
  ELSE 0 
 END AS shipment_cost_paid,
 CASE 
  WHEN payment_type = 'REPAIR COST' AND p.status = 'PAID' 
   THEN p.amount_paid
  ELSE 0 
 END AS repair_cost_paid,
 CASE 
  WHEN payment_type IN ('CUSTOMER BOUGHT') AND p.status = 'PAID' 
   THEN p.amount_paid
  ELSE 0 
 END AS customer_bought_paid,
 CASE 
  WHEN payment_type IN ('GROVER SOLD') AND p.status = 'PAID' 
   THEN p.amount_paid
  ELSE 0 
 CASE 
  WHEN payment_type IN ('DEBT COLLECTION') AND p.status = 'PAID' 
   THEN p.amount_paid
  ELSE 0 
 END AS debt_collection_paid,
 CASE 
  WHEN payment_type IN ('ADDITIONAL CHARGE','COMPENSATION') AND p.status = 'PAID' 
   THEN p.amount_paid
  ELSE 0 
 END AS additional_charge_paid,
 CASE 
  WHEN payment_type LIKE ('%CHARGE BACK%') AND p.status = 'PAID' 
   THEN p.amount_paid  ELSE 0 
 END AS chargeback_paid,
 CASE 
  WHEN payment_type LIKE '%REFUND%' AND p.status = 'PAID' 
   THEN p.amount_paid
  ELSE 0 
 END AS refunds_paid,
 CASE 
  ELSE 'others' 
 p.is_double_charge,
 s2.brand 
FROM master.customer c
  LEFT JOIN ods_data_sensitive.customer_pii c2 
    ON c.customer_id = c2.customer_id 
  INNER JOIN payment_all_historical p
    ON p.customer_id = c.customer_id
  LEFT JOIN master.subscription s
    ON s.subscription_id = p.subscription_id
  LEFT JOIN master.subscription s2 
    ON c.customer_acquisition_subscription_id = s2.subscription_id
  LEFT JOIN master.order o 
    ON o.order_id = p.order_id
WHERE TRUE 
  AND (p.status NOT IN ('CANCELLED')
       AND NOT (p.payment_type = 'FIRST' AND p.status IN ('FAILED','FAILED FULLY'))
       AND p.due_date::DATE < CURRENT_DATE
      )
;