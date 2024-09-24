DROP TABLE IF EXISTS dm_debt_collection.recovery_over_dpd_report;
CREATE TABLE dm_debt_collection.recovery_over_dpd_report AS
WITH merkmal AS (
SELECT DISTINCT 
  user_id AS customer_id
FROM ods_production.order_scoring os
WHERE order_scoring_comments iLIKE '%Merkmal%'
)
,former_non_paid_payments AS (
SELECT 
  p1.payment_id 
 ,SUM(CASE
 	 WHEN p2.payment_id IS NOT NULL 
 	   AND p2.paid_date IS NULL
 	  THEN 1
 	 ELSE 0
 END) AS nr_of_non_paid_former_payment
FROM master.subscription_payment p1
  LEFT JOIN master.subscription_payment p2
    ON p1.subscription_id = p2.subscription_id 
    AND p1.due_date > p2.due_date
WHERE p1.subscription_payment_category <> 'NYD'
GROUP BY 1
)
SELECT 
  DATE_TRUNC('MONTH', sp.due_date)::DATE AS month_of_due_date
 ,DATE_TRUNC('MONTH', s.start_date)::DATE AS subscription_cohort
 ,sp.country_name
 ,sp.asset_was_returned
 ,sp.payment_method
 ,sp.payment_type
 ,s.new_recurring
 ,CASE 
   WHEN m.customer_id IS NOT NULL 
    THEN 'MerkmalTest' 
   ELSE 'Not MerkmalTest'
  END AS merkmal_category
 ,s.store_label  
 ,sp.capital_source
 ,sp.category_name 
 ,sp.customer_type
 ,sc.schufa_class
 ,sp.subscription_payment_category
 ,CASE  
   WHEN sp.dpd <= 1 
    THEN 1
   WHEN sp.dpd BETWEEN 2 AND 90 
    THEN sp.dpd
   WHEN sp.dpd BETWEEN 91 AND 120 
    THEN 120
   WHEN sp.dpd BETWEEN 121 AND 150 
    THEN 150
   WHEN sp.dpd BETWEEN 151 AND 180 
    THEN 180
   WHEN sp.dpd BETWEEN 181 AND 210 
    THEN 210
   WHEN sp.dpd BETWEEN 211 AND 240 
    THEN 240
   WHEN sp.dpd BETWEEN 241 AND 270 
    THEN 270
   WHEN sp.dpd BETWEEN 271 AND 300 
    THEN 300
   WHEN sp.dpd BETWEEN 301 AND 330 
    THEN 330
   WHEN sp.dpd BETWEEN 331 AND 360 
    THEN 360
   WHEN sp.dpd BETWEEN 361 AND 390 
    THEN 390
   WHEN sp.dpd BETWEEN 391 AND 420 
    THEN 420
   WHEN sp.dpd BETWEEN 421 AND 450 
    THEN 450
   WHEN sp.dpd BETWEEN 451 AND 480 
    THEN 480
   WHEN sp.dpd BETWEEN 481 AND 510 
    THEN 510
   WHEN sp.dpd BETWEEN 511 AND 540 
    THEN 540
   WHEN sp.dpd BETWEEN 441 AND 570 
    THEN 570
   WHEN sp.dpd BETWEEN 571 AND 600 
    THEN 600
   WHEN sp.dpd BETWEEN 601 AND 630 
    THEN 630
   WHEN sp.dpd BETWEEN 631 AND 660 
    THEN 660
   WHEN sp.dpd BETWEEN 661 AND 690 
    THEN 690
   WHEN sp.dpd >= 691 
    THEN 720
  END dpd_brackets 
 ,fn.nr_of_non_paid_former_payment 
 ,COALESCE(SUM(sp.amount_due),0) AS amount_due
 ,COALESCE(SUM(sp.amount_paid),0) AS amount_paid
 ,COALESCE(SUM(sp.amount_refund),0) AS amount_refund
 ,COALESCE(SUM(sp.amount_chargeback),0) AS amount_chargeback 
 ,COALESCE(SUM(sp.amount_due),0) - COALESCE(SUM(sp.amount_refund),0) AS amount_due_net_of_refunds
 ,COALESCE(SUM(sp.amount_paid),0) - COALESCE(SUM(sp.amount_refund),0) - COALESCE(SUM(sp.amount_chargeback),0) AS amount_paid_net_of_refunds
 ,amount_due_net_of_refunds - amount_paid_net_of_refunds AS outstanding_amount
 ,COALESCE(SUM(sp.amount_refund),0) + COALESCE(SUM(sp.amount_chargeback),0) AS total_refunds_and_charge_back
 ,SUM(CASE 
   WHEN sp.dpd >= 2 
    THEN COALESCE(sp.amount_due, 0) - COALESCE(sp.amount_refund, 0)
  END) AS amount_overdue_net_of_refunds  
 ,SUM(CASE
   WHEN sp.dpd >= 2 
    THEN COALESCE(sp.amount_paid, 0) - COALESCE(sp.amount_refund, 0) - COALESCE(sp.amount_chargeback, 0)
  END) AS amount_recovered_from_overdue_net_of_refunds_and_chb 
FROM master.subscription_payment sp
  LEFT JOIN master.subscription s
    ON s.subscription_id = sp.subscription_id 
  LEFT JOIN merkmal m
    ON s.customer_id = m.customer_id
  LEFT JOIN	ods_production.customer_scoring sc
    ON sc.customer_id = s.customer_id
  LEFT JOIN former_non_paid_payments fn
    ON sp.payment_id = fn.payment_id
WHERE TRUE 
  AND payment_type = 'RECURRENT'
  AND sp.subscription_payment_category  <> 'NYD' 
  AND sp.due_date < CURRENT_DATE
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;

GRANT USAGE ON SCHEMA dm_debt_collection to tableau;

GRANT SELECT ON dm_debt_collection.recovery_over_dpd_report to debt_management_redash;
GRANT SELECT ON dm_debt_collection.recovery_over_dpd_report to elene_tsintsadze;
GRANT SELECT ON dm_debt_collection.recovery_over_dpd_report TO tableau;
