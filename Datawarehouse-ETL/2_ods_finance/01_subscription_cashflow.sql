DROP TABLE IF EXISTS ods_production.subscription_cashflow;
CREATE TABLE ods_production.subscription_cashflow AS 
WITH sp AS (
SELECT 
  sp.subscription_id
 ,MAX(sp.paid_date) AS max_paid_date
 ,MAX(sp.due_date) AS max_created_payment_due_date
 ,MIN(next_due_date) AS default_date
 ,COUNT(DISTINCT 
   CASE
    WHEN sp.due_date <= CURRENT_DATE 
     THEN sp.subscription_payment_id
    ELSE NULL::CHARACTER VARYING
   END) AS payment_count
 ,MAX(CASE
   WHEN sp.paid_date IS NOT NULL 
     AND sp2.subscription_payment_category <> 'PAID_REFUNDED'
     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'
    THEN sp.payment_number::INT
   ELSE NULL
 END) AS max_payment_number
 ,MAX(CASE
   WHEN sp.paid_date IS NOT NULL
     AND sp2.subscription_payment_category <> 'PAID_REFUNDED'
     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'
    THEN sp.paid_date
   ELSE NULL
  END) AS max_paid_date_excl_refunds
 ,MAX(CASE
   WHEN sp.paid_date IS NOT NULL
    THEN sp.payment_number::INT
   ELSE NULL
  END) AS max_payment_number_with_refunds
 ,COUNT(DISTINCT CASE
   WHEN sp.paid_date IS NOT NULL 
     AND sp2.subscription_payment_category <> 'PAID_REFUNDED'
     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'
    THEN sp.subscription_payment_id
   ELSE NULL::CHARACTER VARYING
  END) AS paid_subscriptions
 ,COUNT(DISTINCT CASE
   WHEN sp.paid_date IS NOT NULL 
     AND sp2.subscription_payment_category = 'PAID_REFUNDED'
     AND sp2.subscription_payment_category = 'PAID_CHARGEBACK'
    THEN sp.subscription_payment_id
ELSE NULL::CHARACTER VARYING
END) AS refunded_subscriptions
 ,COUNT(DISTINCT CASE
   WHEN sp.status IN ('FAILED','FAILED FULLY')
     AND sp.failed_date IS NOT NULL
    THEN sp.subscription_payment_id
   ELSE NULL::CHARACTER VARYING
  END) AS failed_subscriptions
 ,COUNT(DISTINCT CASE
   WHEN sp.status = 'PLANNED'
    THEN sp.subscription_payment_id
   ELSE NULL::CHARACTER VARYING
  END) AS planned_subscriptions
 ,COUNT(DISTINCT CASE
   WHEN sp2.subscription_payment_category <> 'PAID_REFUNDED'
     AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'
     AND sp.paid_date IS NOT NULL 
     AND (sp.amount_paid < sp.amount_due) 
    THEN sp.subscription_payment_id
   ELSE NULL::CHARACTER VARYING
  END) AS partial_paid_subscriptions
 ,SUM(sp.chargeback_transactions) AS chargeback_subscriptions
 ,MAX(CASE
   WHEN sp.due_date = sp2.max_due_date
    THEN sp.status
   ELSE NULL
  END) AS last_payment_status
 ,MAX(CASE
   WHEN sp.due_date = sp2.next_due_date 
    THEN sp2.subscription_payment_category
   ELSE NULL
  END) AS last_valid_payment_category
 ,MAX(CASE
   WHEN sp.due_date <= CURRENT_DATE
    THEN sp.billing_period_start
   ELSE NULL
  END) AS last_billing_period_start
 ,MAX(CASE
   WHEN sp.due_date = sp2.next_due_date
    THEN sp2.dpd
   ELSE NULL
  END) AS dpd
 ,MAX(CASE 
   WHEN sp.due_date = sp2.next_due_date
    THEN sp.status
   ELSE NULL
  END) AS last_valid_payment_status
 ,MAX(CASE
   WHEN sp.due_date = sp2.next_due_date
     AND sp2.is_not_triggered_payments 
    THEN 1 
   ELSE 0
  END) AS is_not_triggered_payments
 ,SUM(CASE
   WHEN sp.due_date::DATE <= CURRENT_DATE 
    THEN COALESCE(sp.amount_due, 0::NUMERIC)
   ELSE NULL::NUMERIC
  END) AS amount_due
 ,SUM(COALESCE(sp.amount_paid, 0::NUMERIC)) AS amount_paid
 ,SUM(CASE
   WHEN sp.due_date <= CURRENT_DATE
    THEN COALESCE(sp.refund_amount, 0::NUMERIC)
   ELSE NULL::NUMERIC
  END) AS amount_refund
 ,SUM(CASE
   WHEN sp.due_date <= CURRENT_DATE 
    THEN COALESCE(sp.chargeback_amount, 0::NUMERIC)
   ELSE NULL::NUMERIC
  END) AS amount_chargeback
 ,NULL AS payment_history
 ,MAX(sp.updated_at) AS updated_at
FROM ods_production.payment_subscription sp 
  LEFT JOIN ods_production.payment_subscription_details sp2 
    ON sp.subscription_payment_id = sp2.subscription_payment_id 
   AND COALESCE(sp.paid_date, '1990-05-22') = COALESCE(sp2.paid_date, '1990-05-22')
WHERE sp.status NOT IN ('CANCELLED')
GROUP BY 1      
)
,ap AS (
SELECT 
  subscription_id
 ,MAX(paid_date) AS max_paid_date
 ,COALESCE(SUM(CASE
   WHEN payment_type::TEXT = 'CUSTOMER BOUGHT'
    THEN COALESCE(COALESCE(amount_paid, CASE 
     WHEN amount_due = 1 
      THEN 1 
   END, 0::NUMERIC) - 
    COALESCE(chargeback_amount, 0::NUMERIC) - 
    COALESCE(refund_amount, 0::NUMERIC))
   ELSE NULL::NUMERIC
  END), 0::NUMERIC) AS amount_paid_asset_purchase
 ,COUNT(CASE 
   WHEN payment_type = 'CUSTOMER BOUGHT'
     AND status = 'PAID' 
     AND COALESCE(chargeback_amount, 0::NUMERIC) = 0
    THEN asset_payment_id 
  END) AS customer_bought_payments
 ,COALESCE(SUM(CASE
   WHEN payment_type = 'CUSTOMER BOUGHT'
    THEN COALESCE(chargeback_amount, 0::NUMERIC)
   ELSE NULL::NUMERIC
  END), 0::NUMERIC) AS amount_chargeback_asset_purchase
 ,COALESCE(SUM(CASE
   WHEN payment_type= 'ADDITIONAL CHARGE'
    THEN COALESCE(amount_paid, 0::NUMERIC) - COALESCE(chargeback_amount, 0::NUMERIC) - COALESCE(refund_amount, 0::NUMERIC)
   ELSE NULL::NUMERIC
  END), 0::NUMERIC) AS amount_paid_additional_charge
 ,COALESCE(SUM(CASE
   WHEN payment_type = 'REPAIR COST'
    THEN COALESCE(amount_paid, 0::NUMERIC) - COALESCE(chargeback_amount, 0::NUMERIC) - COALESCE(refund_amount, 0::NUMERIC)
   ELSE NULL::NUMERIC
  END), 0::NUMERIC) AS amount_paid_repair_cost
 ,MAX(updated_at) AS updated_at
FROM ods_production.payment_asset
WHERE status NOT IN ('CANCELLED')
GROUP BY 1
)
, subscription_warning AS ( 
SELECT DISTINCT
  subscription_id
 ,SUM(amount_overdue_fee) AS overdue_fee
 ,GREATEST(MAX(x1st_warning_sent_date), MAX(x2nd_warning_sent_date), MAX(x3rd_warning_sent_date)) AS last_warning_date
 ,CASE
   WHEN MAX(x3rd_warning_sent_date) IS NOT NULL
    THEN '3x'::TEXT
   WHEN MAX(x2nd_warning_sent_date) IS NOT NULL
    THEN '2x'::TEXT
   WHEN MAX(x1st_warning_sent_date) IS NOT NULL
    THEN '1x'::TEXT
   ELSE NULL::TEXT
  END AS warning_sent
FROM ods_production.payment_subscription
GROUP BY subscription_id
HAVING GREATEST(MAX(x1st_warning_sent_date), MAX(x2nd_warning_sent_date), MAX(x3rd_warning_sent_date)) IS NOT NULL
)
,old_cashflow AS (
SELECT DISTINCT 
   s.subscription_id AS subscription_id
  ,a.asset_id
  ,a.rank_allocations_per_asset
  ,a.allocated_at
  ,s.start_date::DATE AS std
  ,a.return_delivery_date
  ,SUM(CASE
    WHEN p.paid_date::DATE < s.start_date::DATE 
     THEN p.amount_paid 
    ELSE 0 
  END) AS asset_cashflow_from_old_subscriptions
FROM ods_production.subscription s
  LEFT JOIN ods_production.allocation a 
    ON s.subscription_id = a.subscription_id
  LEFT JOIN ods_production.payment_all p 
    ON p.asset_id = a.asset_id
WHERE first_asset_delivery_date IS NOT NULL
GROUP BY 1,2,3,4,5,6
)
,old_cashflow_final AS (
SELECT DISTINCT  
  subscription_id
 ,LAST_VALUE(asset_cashflow_from_old_subscriptions) OVER (PARTITION BY subscription_id 
   ORDER BY allocated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
    AS asset_cashflow_from_old_subscriptions
FROM old_cashflow
)
,overdue AS (						
SELECT 						
  subscription_id
 ,SUM(CASE
   WHEN due_date <= CURRENT_DATE 
    THEN
     CASE
      WHEN COALESCE(amount_overdue_fee, 0) <= 0 
       THEN (COALESCE(amount_due, 0) - COALESCE(amount_paid, 0) + COALESCE(chargeback_amount, 0))
      ELSE 
       CASE 
        WHEN COALESCE(amount_paid, 0) >= (COALESCE(amount_subscription, 0) + COALESCE(amount_voucher, 0))
         THEN (0+ COALESCE(chargeback_amount, 0))
        ELSE (COALESCE(amount_subscription, 0)+COALESCE(amount_voucher, 0) - COALESCE(amount_paid, 0) + COALESCE(chargeback_amount, 0))
       END 
      END	   
   ELSE NULL::NUMERIC	
  END) AS outstanding_sub_revenue_excl_overdue
 ,SUM(CASE
   WHEN due_date <= CURRENT_DATE
    THEN
     CASE
      WHEN COALESCE(amount_overdue_fee, 0) <= 0 
       THEN 0
      ELSE 
       CASE
        WHEN COALESCE(amount_paid, 0) >= (COALESCE(amount_subscription, 0) + COALESCE(amount_voucher, 0))
         THEN COALESCE(amount_due, 0) -COALESCE(amount_paid, 0)
        ELSE COALESCE(amount_overdue_fee, 0)
       END 
      END	   
    ELSE NULL::NUMERIC	
  END) AS outstanding_sub_overdue
FROM ods_production.payment_subscription
WHERE status NOT IN ('CANCELLED')
GROUP BY 1
)
 SELECT
  COALESCE(s.subscription_id, sp.subscription_id, ap.subscription_id) AS subscription_id
 ,GREATEST(sp.max_paid_date, ap.max_paid_date) AS max_cashflow_date
 ,sp.max_paid_date_excl_refunds
 ,sp.default_date AS default_date
 ,COALESCE(sp.payment_count, 0) AS payment_count
 ,COALESCE(sp.max_payment_number, 0) AS max_payment_number
 ,COALESCE(sp.max_payment_number_with_refunds, 0) AS max_payment_number_with_refunds
 ,COALESCE(sp.paid_subscriptions, 0) AS paid_subscriptions
 ,COALESCE(sp.refunded_subscriptions, 0) AS refunded_subscriptions
 ,COALESCE(sp.failed_subscriptions, 0) AS failed_subscriptions
 ,COALESCE(sp.planned_subscriptions, 0) AS planned_subscriptions
 ,COALESCE(sp.partial_paid_subscriptions, 0) AS partial_paid_subscriptions
 ,COALESCE(sp.chargeback_subscriptions, 0) AS chargeback_subscriptions
 ,COALESCE(sp.last_payment_status,'N/A') AS last_payment_status
 ,COALESCE(sp.last_valid_payment_category,'N/A') AS last_valid_payment_category
 ,COALESCE(sp.last_billing_period_start, NULL) AS last_billing_period_start
 ,sp.dpd
 ,CASE
   WHEN s.status = 'CANCELLED' 
    THEN 0 
   WHEN s.status = 'ACTIVE' 
     AND CASE
      WHEN (s.committed_sub_value - COALESCE(sp.amount_paid, 0) + COALESCE(sp.amount_refund, 0)+ COALESCE(sp.amount_chargeback, 0)) < s.subscription_value 
       THEN 0::DOUBLE PRECISION
      END = 0 
    THEN s.subscription_value
   ELSE CASE
    WHEN (s.committed_sub_value - COALESCE(sp.amount_paid, 0)) < s.subscription_value 
     THEN 0
    ELSE (s.committed_sub_value - COALESCE(sp.amount_paid, 0) + COALESCE(sp.amount_refund, 0) + COALESCE(sp.amount_chargeback, 0))
   END
  END AS outstanding_committed_sub_value
 ,COALESCE(sp.amount_paid, 0) - 
  COALESCE(sp.amount_refund, 0) - 
  COALESCE(sp.amount_chargeback, 0) +
  COALESCE(ap.amount_paid_asset_purchase, 0::NUMERIC) + 
  COALESCE(ap.amount_paid_additional_charge, 0::NUMERIC) +
  COALESCE(ap.amount_paid_repair_cost, 0::NUMERIC) AS total_cashflow
 ,COALESCE(sp.amount_due, 0::NUMERIC) - 
  COALESCE(sp.amount_paid, 0::NUMERIC) + 
  COALESCE(sp.amount_chargeback, 0::NUMERIC) AS outstanding_subscription_revenue
 ,ovd.outstanding_sub_revenue_excl_overdue
 ,ovd.outstanding_sub_overdue
 ,COALESCE(sp.amount_due, 0) AS subscription_revenue_due
 ,COALESCE(sp.amount_paid, 0) AS subscription_revenue_paid
 ,COALESCE(sp.amount_paid, 0) -
  COALESCE(sp.amount_refund, 0) -
  COALESCE(sp.amount_chargeback, 0) AS net_subscription_revenue_paid
 ,COALESCE(sp.amount_refund, 0) AS subscription_revenue_refunded
 ,COALESCE(sp.amount_chargeback, 0) AS subscription_revenue_chargeback
 ,COALESCE(ap.amount_paid_asset_purchase, 0::NUMERIC) AS asset_purchase_amount_paid
 ,COALESCE(ap.amount_chargeback_asset_purchase, 0::NUMERIC) AS asset_purchase_amount_chargeback
 ,COALESCE(ap.amount_paid_additional_charge, 0::NUMERIC) AS additional_charge_amount_paid
 ,COALESCE(ap.amount_paid_repair_cost, 0::NUMERIC) AS repair_cost_amount_paid
 ,COALESCE(ap.customer_bought_payments,0::NUMERIC) AS customer_bought_payments
 ,sw.overdue_fee
 ,sw.last_warning_date
 ,sw.warning_sent
 ,sp.payment_history
 ,cf.asset_cashflow_from_old_subscriptions
 ,sa.avg_asset_purchase_price - cf.asset_cashflow_from_old_subscriptions AS exposure_to_default
 ,sp.max_created_payment_due_date
 ,sp.is_not_triggered_payments
 ,GREATEST(s.updated_date, sp.updated_at, ap.updated_at, sa.updated_at) AS updated_at
 FROM ods_production.subscription s 
   FULL OUTER JOIN sp 
     ON s.subscription_Id = sp.subscription_id 
   FULL OUTER JOIN ap 
     ON s.subscription_id = ap.subscription_id
   LEFT JOIN subscription_warning sw 
     ON s.subscription_id = sw.subscription_id
   LEFT JOIN old_cashflow_final cf 
     ON cf.subscription_id=s.subscription_id
   LEFT JOIN ods_production.subscription_assets sa 
     ON s.subscription_id = sa.subscription_id
   LEFT JOIN overdue ovd 
     ON ovd.subscription_id = s.subscription_id    
 WHERE COALESCE(s.subscription_id, sp.subscription_id, ap.subscription_id) IS NOT NULL
;

GRANT SELECT ON ods_production.subscription_cashflow TO basri_oz, redash_growth;
GRANT SELECT ON ods_production.subscription_cashflow TO tableau;