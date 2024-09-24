
DROP TABLE IF EXISTS ods_production.asset_last_allocation_details;
CREATE TABLE ods_production.asset_last_allocation_details AS 
WITH qa_payments AS (
SELECT 
  s.allocation_id
 ,s.asset_id
 ,s.subscription_id
 ,SUM(s.amount_due) / COUNT(*) AS amount_due
 ,SUM(COALESCE(s.amount_paid, 0::NUMERIC) - COALESCE(s.refund_amount, 0::NUMERIC)) / COUNT(*) AS amount_paid_net
 ,s.due_date
 ,COUNT(*) AS payments
 ,MIN(s.payment_number) AS first_payment_number
 ,MAX(s.paid_date) AS max_paid_date
FROM ods_production.payment_subscription s
  LEFT JOIN ods_production.payment_subscription_details sd 
    ON sd.subscription_payment_id = s.subscription_payment_id
   AND COALESCE(sd.paid_date,'2012-01-01') = COALESCE(s.paid_date,'2012-01-01')
WHERE TRUE 
   AND s.due_date = sd.next_due_date_allocation
GROUP BY s.allocation_id, s.asset_id, s.subscription_id, s.due_date
)
,asset_risk_class AS (
SELECT DISTINCT 
  aa.asset_id
 ,a.subscription_id 
 ,sp.allocation_id AS last_allocation_id
 ,sd.subscription_payment_category AS risk_class
 ,CASE 
  WHEN sd.subscription_payment_category = 'NYD'::TEXT 
    OR sd.subscription_payment_category ~~ '%RECOVERY%'::TEXT 
    OR sd.subscription_payment_category ~~ '%PAID%'::TEXT 
    OR (sd.subscription_payment_category IN ('ARREARS', 'DELINQUENT')) 
   THEN 'PERFORMING'::text
  WHEN sd.subscription_payment_category ~~ '%DEFAULT%'::TEXT 
   THEN 'AT RISK'::text
  ELSE 'NOT AVAILABLE'::text
 END AS risk_class_group
 ,sp.due_date AS next_due_date
 ,sd.dpd
 ,a.amount_due
 ,a.amount_paid_net
FROM ods_production.allocation aa
  LEFT JOIN ods_production.payment_subscription sp 
    ON sp.allocation_id = aa.allocation_id
  LEFT JOIN ods_production.payment_subscription_details sd 
    ON sd.subscription_payment_id=sp.subscription_payment_id
   AND COALESCE(sd.paid_date,'2012-01-01')=COALESCE(sp.paid_date,'2012-01-01')
  INNER JOIN qa_payments a 
    ON a.allocation_id::TEXT = sp.allocation_id::TEXT 
   AND aa.asset_id=sp.asset_id
   AND a.due_date = sp.due_date 
   AND a.first_payment_number = sp.payment_number 
   AND (a.payments = 1 OR sd.subscription_payment_category NOT IN ('PAID_REFUNDED','PAID_CHARGEBACK'))
   AND COALESCE(a.max_paid_date, '2222-12-31 00:00:00') = COALESCE(sp.paid_date, '2222-12-31 00:00:00')
WHERE aa.is_last_allocation_per_asset
)
,stock_level AS (
SELECT DISTINCT 
  a.asset_id
 ,aa.allocation_id AS last_allocation_id
 ,aa.allocation_status_original AS last_allocation_status
 ,COALESCE(stock.days_in_stock::double precision, a.days_in_stock)::integer AS days_in_stock
 ,CASE
   WHEN COALESCE(stock.days_in_stock::double precision, a.days_in_stock) IS NULL 
    THEN 'NOT AVAILABLE'::text
   WHEN COALESCE(stock.days_in_stock::double precision, a.days_in_stock) <= 2::double precision 
    THEN '48h'::text
   WHEN COALESCE(stock.days_in_stock::double precision, a.days_in_stock) <= (7 * 2)::double precision 
    THEN '1-2 weeks'::text
   WHEN COALESCE(stock.days_in_stock::double precision, a.days_in_stock) <= (7 * 4)::double precision 
    THEN '2-4 weeks'::text
   WHEN COALESCE(stock.days_in_stock::double precision, a.days_in_stock) <= (7 * 8)::double precision 
    THEN '4-8 weeks'::text
   WHEN COALESCE(stock.days_in_stock::double precision, a.days_in_stock) > (7 * 8)::double precision 
    THEN '8+ weeks'::TEXT 
   ELSE 'NOT AVAILABLE'::text
  END AS days_in_stock_bucket
 ,a.asset_status_grouped
 ,a.days_in_warehouse AS asset_days_in_warehouse
 ,a.days_in_stock AS asset_days_in_stock
 ,stock.days_in_stock AS last_allocation_days_in_stock
 ,s.cancellation_date AS last_cancellation_date
 ,s.debt_collection_handover_date AS last_debt_collection_handover_date
FROM ods_production.asset a
  LEFT JOIN ods_production.allocation aa 
    ON a.asset_id = aa.asset_id
  LEFT JOIN ods_production.allocation_days_in_stock stock 
    ON stock.allocation_id = aa.allocation_id
  LEFT JOIN ods_production.subscription s 
    ON aa.subscription_id = s.subscription_id
WHERE aa.is_last_allocation_per_asset 
  OR aa.asset_id IS NULL
)
,pre_dpd_calc AS (
SELECT 
  MAX(due_date) AS proposed_dpd_date,
  subscription_id
FROM ods_production.payment_subscription   
GROUP BY subscription_id 
)
,asset_subscription_map AS (
SELECT DISTINCT 
  asset_id,
  subscription_id,
  is_last_allocation_per_asset
FROM ods_production.allocation 
WHERE is_last_allocation_per_asset IS TRUE 
)
,checking_count_payments AS (
SELECT 
  SUM(CASE 
	    WHEN paid_date IS NULL 
	     THEN 1 
	     ELSE 0 
	    END) AS unpaid_subscriptions,
  SUM(CASE 
	    WHEN paid_date IS NOT NULL 
	     THEN 1 
	     ELSE 0 
	    END) AS paid_subscriptions,
  COUNT(*) AS payment_count,
  subscription_id 
FROM ods_production.payment_subscription   
GROUP BY subscription_id
)
,if_asset_is_returned AS (
SELECT DISTINCT 
  asset_id, 
  subscription_id, 
  return_delivery_date
FROM ods_production.allocation   
WHERE return_delivery_date IS NOT NULL 
)
SELECT DISTINCT 
  a.asset_id
 ,r.subscription_id
 ,COALESCE(r.last_allocation_id,s.last_allocation_id) AS last_allocation_id
 ,last_allocation_status
 ,a.asset_status_original
 ,CASE
   WHEN a.asset_status_original = 'IN DEBT COLLECTION' 
    THEN 'AT RISK'::text
   WHEN a.asset_status_grouped = 'ON RENT' AND replace(COALESCE(r.risk_class,'NOT AVAILABLE'),'_',' ') IN ('ARREARS','DELINQUENT') 
    THEN 'PERFORMING'
   WHEN a.asset_status_original = 'OFFICE' 
    THEN 'PERFORMING' 
   WHEN a.asset_status_grouped = 'ON RENT' 
    THEN replace(COALESCE(r.risk_class_group,'NOT AVAILABLE'),'_',' ')
   WHEN a.asset_status_grouped = 'ON RENT' 
    THEN 'NEVER ALLOCATED'
   WHEN a.asset_status_grouped IN ('LOST','LOST SOLVED') 
    THEN 'LOST'
   ELSE COALESCE(a.asset_status_grouped,'NOT AVAILABLE')
  END AS asset_status_new
 ,CASE
   WHEN a.asset_status_original = 'OFFICE' 
    THEN 'OFFICE'
   WHEN a.asset_status_original = 'WRITTEN OFF OPS' 
    THEN 'WRITTEN OFF OPS'
   WHEN a.asset_status_original = 'WRITTEN OFF DC' 
    THEN 'WRITTEN OFF RISK'
   WHEN REPLACE(COALESCE(r.risk_class,'NOT AVAILABLE'),'_',' ') = 'PAID CHARGEBACK' 
     AND asset_status_new = 'AT RISK' 
    THEN 'PAID CHARGEBACK RISK'
   WHEN a.asset_status_grouped = 'ON RENT' 
    THEN REPLACE(COALESCE(r.risk_class,'NOT AVAILABLE'),'_',' ')
   WHEN a.asset_status_grouped IN ('IN STOCK','REFURBISHMENT') 
    THEN COALESCE(s.days_in_stock_bucket,'NOT AVAILABLE')
   WHEN a.asset_status_grouped IN ('SOLD') 
     AND COALESCE(sold_price,0) > 1 
     AND COALESCE(cf.customer_bought_paid,0) > 1 
    THEN 'SOLD to Customer'
   WHEN a.asset_status_grouped IN ('SOLD') 
     AND COALESCE(sold_price,0) > 0 
     AND COALESCE(cf.customer_bought_paid,0) = 0 
    THEN 'SOLD to 3rd party'
   WHEN a.asset_status_grouped IN ('SOLD') AND COALESCE(sold_price,0) <=1 
    THEN 'SOLD 1-euro' 
   WHEN a.asset_status_grouped IN ('SOLD') 
     AND COALESCE(sold_price,0) > 1 
     AND COALESCE(cf.customer_bought_paid,0) = 0 
     AND a.is_sold_by_dc = TRUE 
    THEN 'SOLD by DC'
   WHEN a.asset_status_grouped IN ('SOLD') 
    THEN 'SOLD'
   ELSE a.asset_status_original
  END AS asset_status_detailed
 ,CASE 
   WHEN a.asset_status_grouped IN ('IN STOCK','REFURBISHMENT') 
    THEN COALESCE(s.days_in_stock,a.days_in_stock) 
  END AS last_allocation_days_in_stock
 ,r.risk_class_group AS last_allocation_risk_class_group
 ,r.next_due_date AS last_allocation_default_payment_due_date
 ---------------------------------------------------------------------------------------
  ,CASE 
   WHEN a.asset_status_original = 'IN DEBT COLLECTION' 
     AND last_debt_collection_handover_date IS NOT NULL 
     AND iair.subscription_id IS NULL
     AND ccp.paid_subscriptions >= ccp.payment_count
      THEN  (DATEDIFF('d',dcc.proposed_dpd_date::TIMESTAMP,CURRENT_DATE))
   WHEN a.asset_status_original = 'IN DEBT COLLECTION' 
     AND COALESCE(dpd,0) <= 0
    THEN  
     CASE 
      WHEN last_debt_collection_handover_date IS NOT NULL 
       THEN DATEDIFF('d',last_debt_collection_handover_date::TIMESTAMP,CURRENT_DATE)
      ELSE DATEDIFF('d',last_cancellation_date::TIMESTAMP,CURRENT_DATE) 
     END 
   WHEN  a.asset_status_grouped = 'ON RENT' 
     AND iair.subscription_id IS NULL 
     AND ccp.paid_subscriptions >= ccp.payment_count
    THEN COALESCE (DATEDIFF('d',dcc.proposed_dpd_date::TIMESTAMP,CURRENT_DATE),GREATEST(r.dpd,0))
   WHEN a.asset_status_grouped = 'ON RENT' 
     AND iair.subscription_id IS NOT NULL 
    THEN 0 
   WHEN a.asset_status_grouped = 'ON RENT' 
    THEN  GREATEST(r.dpd,0) 
  END AS last_allocation_dpd
   ---------------------------------------------------------------------------------------
 ,CASE
   WHEN a.asset_status_original IN  ('SOLD') 
    THEN 'SOLD'
   WHEN a.asset_status_original IN ('LOST','LOST SOLVED') 
    THEN 'LOST'
   WHEN asset_status_new IN ('IN STOCK', 'REFURBISHMENT') 
    THEN 'IN STOCK'
   WHEN a.asset_status_grouped = 'WRITTEN OFF' 
    THEN 'WRITTEN OFF'
   WHEN last_allocation_dpd>360 
    THEN 'PENDING WRITEOFF'
   WHEN last_allocation_dpd=0 
    THEN 'PERFORMING'
   WHEN last_allocation_dpd>0 AND last_allocation_dpd<31 
    THEN 'OD 1-30'
   WHEN last_allocation_dpd>30 AND last_allocation_dpd<61 
    THEN 'OD 31-60'
   WHEN last_allocation_dpd>60 AND last_allocation_dpd<91 
    THEN 'OD 61-90'
   WHEN last_allocation_dpd>90 AND last_allocation_dpd<121 
    THEN 'OD 91-120'
   WHEN last_allocation_dpd>120 AND last_allocation_dpd<151 
    THEN 'OD 121-150'
   WHEN last_allocation_dpd>150 AND last_allocation_dpd<180 
    THEN 'OD 151-180'
   WHEN last_allocation_dpd>=180 AND last_allocation_dpd<361 
    THEN 'OD 180+'
   WHEN asr.active_subscriptions>0 
    THEN 'PERFORMING'
   WHEN a.asset_status_original IN ('LOST','IRREPARABLE','RECOVERED','LOST SOLVED','DELETED','RETURNED TO SUPPLIER') 
    THEN 'LOST'
   ELSE 'IN STOCK' 
  END AS dpd_bucket
FROM ods_production.asset a 
  LEFT JOIN asset_risk_class r 
    ON a.asset_id = r.asset_id
  LEFT JOIN ods_production.asset_cashflow cf 
    ON cf.asset_id = a.asset_id
  LEFT JOIN stock_level s 
    ON s.asset_id = a.asset_id 
  LEFT JOIN ods_production.asset_subscription_reconciliation  asr 
    ON asr.asset_id = a.asset_id
 ---------------------------------------------------------------------------------------
  LEFT JOIN asset_subscription_map lash
    ON a.asset_id = lash.asset_id    
  LEFT JOIN pre_dpd_calc dcc
    ON lash.subscription_id = dcc.subscription_id  
  LEFT JOIN checking_count_payments ccp
    ON lash.subscription_id = ccp.subscription_id 
  LEFT JOIN if_asset_is_returned iair
    ON lash.asset_id = iair.asset_id 
   AND lash.subscription_id = iair.subscription_id 
    ;

GRANT SELECT ON ods_production.asset_last_allocation_details TO tableau;
