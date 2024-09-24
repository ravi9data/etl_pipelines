DROP TABLE IF EXISTS dwh.customer_collection_curves;
CREATE TABLE dwh.customer_collection_curves AS
WITH customers AS (
--get customer data
SELECT 
 c.first_due_date::DATE AS first_due_date,
 c.customer_acquisition_cohort::date as customer_acquisition_cohort,
 c.customer_id,
 c.customer_acquisition_store,
 s.subscription_value AS acquired_sub_value,
 s.subscription_plan AS acquired_subscription_plan,
 s.brand
FROM ods_production.customer_acquisition_cohort c
  LEFT JOIN ods_production.subscription s 
    ON s.subscription_id = c.subscription_id     
)
SELECT DISTINCT 
 c.customer_acquisition_cohort::DATE AS customer_acquisition_cohort,
 c.first_due_date::DATE AS first_due_date,
 c.customer_acquisition_store,
 c.acquired_subscription_plan,
 c.brand,
 voucher.paid_orders_with_voucher,
 voucher.unique_vouchers_redeemed,
 voucher.total_voucher_discount,
 voucher.unique_special_vouchers_redeemed,
 voucher.special_voucher_of_first_order,
 c.customer_id,
 COALESCE(d.is_customer_default,0) AS is_customer_default,
 COALESCE(d.is_subscription_default,0) AS is_subscription_default,
 COALESCE(cs.cancellation_reason_churn,'active') AS cancellation_reason_churn,
 p.order_id,
 o.status AS order_status,
 o.new_recurring,
 p.payment_id,
 p.status AS payment_status,
 p.payment_type,
 ore.retention_group,
 p.due_date,
 p.paid_date,
 p.amount_due,
 p.amount_paid,
 COALESCE(ps.amount_discount, 0) + COALESCE(ps.amount_voucher, 0) AS discount_amount,
 GREATEST(p.due_date::DATE - c.customer_acquisition_cohort::DATE, 0) AS days_between_acquisition_due,
 GREATEST(datediff(month, c.customer_acquisition_cohort::DATE, p.due_date::DATE), 0) AS months_between_acquisition_due,
 GREATEST(p.paid_date::DATE- c.customer_acquisition_cohort::DATE,0) AS days_between_acquisition_paid,
 s.subscription_value,
 s.committed_sub_value,
 s.subscription_id,
 s.subscription_plan,
 cust.customer_type,
 CASE 
	 WHEN p.payment_type = 'REPAIR COST' AND p.status = 'PAID' 
	  THEN p.amount_paid 
	 ELSE 0 
 END AS repair_cost_paid,
 CASE 
	 WHEN p.payment_type IN ('FIRST','RECURRENT') AND p.status = 'PAID' 
	  THEN p.amount_paid  
	 ELSE 0 
 END AS subscription_revenue_paid,
 CASE 
	 WHEN p.payment_type IN ('SHIPMENT') AND p.status = 'PAID' 
	  THEN p.amount_paid  
	 ELSE 0 
 END AS shipment_cost_paid,
 CASE 
	 WHEN p.payment_type IN ('CUSTOMER BOUGHT') AND p.status = 'PAID' 
	  THEN p.amount_paid  
	 ELSE 0 
 END AS customer_bought_paid,
 CASE 
	 WHEN p.payment_type IN ('GROVER SOLD') AND p.status = 'PAID' 
	  THEN p.amount_paid  
	 ELSE 0 
 CASE 
	 WHEN p.payment_type IN ('ADDITIONAL CHARGE') AND p.status = 'PAID' 
	  THEN p.amount_paid  
	 ELSE 0 
 END AS additional_charge_paid,
 CASE 
	 WHEN p.payment_type LIKE ('%CHARGE BACK%') AND p.status = 'PAID' 
	  THEN p.amount_paid  
	 ELSE 0 
 END AS chargeback_paid,
 CASE 
	 WHEN p.payment_type LIKE ('%REFUND%') AND p.status = 'PAID' 
	  THEN p.amount_paid  
	 ELSE 0 
 END AS refunds_paid
FROM customers c
  INNER JOIN ods_production.payment_all p
    ON p.customer_id = c.customer_id
  LEFT JOIN ods_production.subscription_cancellation_reason cs 
    ON p.subscription_id = cs.subscription_id
  LEFT JOIN ods_production.subscription s 
    ON s.subscription_id = p.subscription_id 
  LEFT JOIN ods_production.payment_subscription ps
    ON ps.subscription_payment_id = p.payment_id
  LEFT JOIN master.order o 
    ON o.order_id = p.order_id
  LEFT JOIN ods_production.subscription_assets sa 
    ON p.subscription_id = sa.subscription_id
  LEFT JOIN ods_production.customer cust 
    ON cust.customer_id = p.customer_id
  LEFT JOIN ods_production.subscription_default d 
    ON d.subscription_id = s.subscription_id
  LEFT JOIN ods_production.customer_voucher_usage voucher
    ON voucher.customer_id = c.customer_id
  LEFT JOIN ods_production.order_retention_group ore 
    ON o.order_id = ore.order_id
 WHERE TRUE 
   AND p.status NOT IN ('CANCELLED')
   AND NOT (p.payment_type = 'FIRST' 
   AND p.status IN ('FAILED','FAILED FULLY'))
   AND p.due_date::DATE < CURRENT_DATE - 1
;

GRANT SELECT ON dwh.customer_collection_curves TO tableau;
GRANT SELECT ON dwh.customer_collection_curves TO redash_pricing;

