DROP TABLE IF EXISTS dm_b2b.shipment_overview;
CREATE TABLE dm_b2b.shipment_overview as
WITH pre AS (
SELECT		
  s.order_id 
 ,c.company_name
 ,s.subscription_id
 ,s.subscription_name
 ,a.allocation_id
 ,a.serial_number
 ,ROW_NUMBER () OVER (PARTITION BY s.subscription_id ORDER BY a.allocated_at desc) idx
 ,s.status
 ,a.allocation_status_original
 ,CASE		
   WHEN a.allocation_status_original  = 'SOLD' 
    THEN 'SOLD'
   WHEN a.return_delivery_date IS NOT NULL or a.allocation_status_original = 'RETURNED' 
    THEN 'RETURNED'						
   WHEN a.delivered_at IS NOT NULL or a.allocation_status_original = 'DELIVERED' 
    THEN 
	 CASE 
	  WHEN s.status= 'CANCELLED' 
	   THEN 'RETURNED' 
	  ELSE 'DELIVERED' 
	 END					
   WHEN COALESCE(a.failed_delivery_at, a.failed_delivery_candidate ) IS NOT NULL 
    THEN 'FAILED DELIVERY/CANDIDATE'					
   WHEN a.allocation_status_original in ('CANCELLED', 'TO BE FIXED') 
    THEN a.allocation_status_original 						
   WHEN a.shipment_at IS NOT NULL 
    THEN 'SHIPPED'						
   WHEN a.allocated_at IS NOT NULL 
    THEN						
	 CASE 
	  WHEN a.shipment_id IS NOT NULL OR a.allocation_status_original = 'READY TO SHIP'					
	   THEN 'READY TO SHIP'				
	  ELSE 'PENDING PACKING' 
	 END				
   WHEN s.status = 'CANCELLED' 
    THEN s.status
   ELSE 'PENDING ALLOCATION'						
  END AS bucket
 ,s.start_date
 ,a.allocated_at
 ,a.shipment_at
 ,a.delivered_at
 ,COALESCE(a.failed_delivery_at, a.failed_delivery_candidate ) AS failed_delivery_at
 ,a.return_delivery_date							
FROM ods_production.subscription s							
  LEFT JOIN ods_production.customer c 
    ON s.customer_id = c.customer_id							
  LEFT JOIN ods_production.allocation a 
    ON s.subscription_id = a.subscription_id							
WHERE c.customer_type = 'business_customer'								
)
SELECT 	 
  order_id
 ,company_name
 ,subscription_id
 ,CASE 
   WHEN subscription_name IS NULL 
    THEN 'New Infra' 
   ELSE 'Legacy' 
  END AS infrastructure
 ,allocation_id
 ,serial_number
 ,status
 ,allocation_status_original
 ,bucket
 ,start_date
 ,allocated_at
 ,shipment_at
 ,delivered_at
 ,failed_delivery_at
 ,return_delivery_date 
FROM pre 
WHERE idx = 1;

GRANT SELECT ON dm_b2b.shipment_overview TO tableau;



----------------- B2B core metrics used in weekly report -----------------
DROP TABLE IF EXISTS dm_b2b.weekly_core_metrics;
CREATE TABLE dm_b2b.weekly_core_metrics as
WITH company_info AS (
SELECT DISTINCT 
 c2.customer_id
 ,c2.company_id
 ,c2.company_name
 ,c2.created_at::DATE AS created_at
 ,CASE 
   WHEN c2.billing_country = 'Germany' 
    THEN 'DE'
   WHEN c2.billing_country = 'Austria' 
    THEN 'AT'
   WHEN c2.billing_country = 'Netherlands' 
    THEN 'NL'
   WHEN c2.billing_country = 'Spain' 
    THEN 'ES'
   WHEN c2.billing_country = 'United States' 
    THEN 'US'
   ELSE 'Others' 
  END AS country
 ,u.full_name AS account_owner
 ,c2.company_type_name
 ,CASE 
   WHEN u.full_name <> 'B2B Support Grover' 
    THEN 'Sales'
   ELSE 'Self-service' 
  END AS segment_
 ,COALESCE(fm.is_freelancer, 0) AS is_freelancer
 ,c2.start_date_of_first_subscription::DATE AS start_date_of_first_subscription
FROM master.customer c2 
 LEFT JOIN ods_b2b.account a 
   ON c2.customer_id = a.customer_id
 LEFT JOIN ods_b2b."user" u
   ON u.user_id = a.account_owner
 LEFT JOIN dm_risk.b2b_freelancer_mapping fm
   ON c2.company_type_name = fm.company_type_name
WHERE customer_type = 'business_customer'
)   
,fact_date AS (
SELECT DISTINCT 
  d.datum
 ,CASE  
   WHEN d.week_day_number = 7 
    THEN 1
   WHEN d.datum = CURRENT_DATE 
    THEN 1
   ELSE 0 
  END AS day_is_end_of_week
 ,CASE   
   WHEN d.day_is_last_of_month = 1 OR d.datum = CURRENT_DATE - 1 
    THEN 1 --Monthly info should be yesterday
   ELSE 0 
  END AS day_is_last_of_month
 ,ci.customer_id
 ,ci.start_date_of_first_subscription
 ,ci.country
 ,CASE 
   WHEN ci.is_freelancer = TRUE AND segment_ = 'Self-service' 
    THEN 'Freelancer' 
   WHEN ci.is_freelancer = FALSE AND segment_ = 'Self-service' 
    THEN 'Non-freelancers' 
   ELSE 'All'
  END AS is_freelancer
 ,ci.segment_ AS segment
 ,CASE 
   WHEN ci.start_date_of_first_subscription IS NULL or datum::DATE < ci.start_date_of_first_subscription::DATE 
    THEN 'No subscriptions'
   WHEN DATE_TRUNC('WEEK', ci.start_date_of_first_subscription)::DATE = DATE_TRUNC('WEEK', datum)::DATE 
    THEN 'New customer'
   WHEN DATE_TRUNC('QUARTER', ci.start_date_of_first_subscription)::DATE = DATE_TRUNC('QUARTER', datum)::DATE 
    THEN 'Upsell new'
   ELSE 'Upsell existing'
  END AS new_upsell
FROM public.dim_dates d
  LEFT JOIN company_info ci 
    ON datum >= ci.created_at::DATE 
WHERE TRUE 
  AND datum <= CURRENT_DATE
  AND datum >= DATE_TRUNC('WEEK', DATEADD('WEEK', -7, CURRENT_DATE))
)
,active_info AS (SELECT DISTINCT
  d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.segment
 ,d.new_upsell
 ,d.country
 ,SUM(COALESCE(active_subscriptions, 0)) AS active_subscriptions
 ,SUM(COALESCE(active_subscription_value, 0)) AS active_subscription_value
 ,SUM(COALESCE(active_committed_subscription_value, 0)) AS active_committed_subscription_value
 ,SUM(COALESCE(active_subscription_value, 0)) * 12 AS annualized_asv
FROM fact_date d
  LEFT JOIN ods_finance.active_subscriptions_overview s
    ON d.datum = s.fact_date  
   AND d.customer_id = s.customer_id
GROUP BY 1,2,3,4,5,6,7,8
)
,new_subs AS (
SELECT 
  d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.segment
 ,d.new_upsell
 ,d.country
 ,COUNT(DISTINCT s.subscription_id) AS new_subscriptions
 ,SUM(s.subscription_value_euro) AS new_subscription_value
 ,SUM(s.subscription_value_euro * s.rental_period) AS committed_subscription_revenue
FROM fact_date d
LEFT JOIN ods_production.subscription s
  ON d.datum = s.start_date::DATE
 AND d.customer_id = s.customer_id
GROUP BY 1,2,3,4,5,6,7,8
)
, cancellation AS (
SELECT 
  d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.segment
 ,d.new_upsell
 ,d.country
 ,COUNT(DISTINCT s.subscription_id) AS cancelled_subscriptions
 ,SUM(s.subscription_value_euro) AS cancelled_subscription_value
FROM fact_date d 
  LEFT JOIN ods_production.subscription s
    ON d.datum =  s.cancellation_date::DATE
   AND d.customer_id = s.customer_id
GROUP BY 1,2,3,4,5,6,7,8)
SELECT DISTINCT
  c.datum AS fact_date
 ,c.day_is_last_of_month
 ,c.day_is_end_of_week
 ,c.is_freelancer
 ,c.segment
 ,CASE 
   WHEN c.segment= 'Self-service' 
    THEN 'Self-service'
   WHEN c.segment <> 'Self-service' 
    THEN 'Sales'
  END AS segment_grouped
 ,c.new_upsell
 ,CASE 
   WHEN c.new_upsell= 'New customer' 
    THEN 'New business'
   WHEN c.new_upsell in ('Upsell new', 'Upsell existing') 
    THEN 'Upsell existing business'
   ELSE 'NA'
  END AS new_upsell_grouped
 ,c.country
 ,COALESCE(SUM(active_subscriptions), 0) AS active_subscriptions
 ,COALESCE(SUM(active_subscription_value), 0) AS active_subscription_value
 ,COALESCE(SUM( active_committed_subscription_value), 0) AS active_committed_subscription_value
 ,COALESCE(SUM(annualized_asv), 0) AS annualized_asv
 ,COALESCE(SUM(new_subscriptions), 0) AS new_subscriptions
 ,COALESCE(SUM(new_subscription_value), 0) AS new_subscription_value
 ,COALESCE(SUM(committed_subscription_revenue), 0) AS committed_subscription_value_new
 ,COALESCE(SUM(cancelled_subscriptions), 0) AS cancelled_subscriptions
 ,COALESCE(SUM(cancelled_subscription_value), 0) AS cancelled_subscription_value  
FROM fact_date c
  LEFT JOIN active_info ai
    ON ai.datum = c.datum
   AND ai.customer_id = c.customer_id
  LEFT JOIN new_subs ns
    ON ns.datum = c.datum
   AND ns.customer_id = c.customer_id
    LEFT JOIN cancellation can
    ON can.datum = c.datum
   AND can.customer_id = c.customer_id
GROUP BY 1,2,3,4,5,6,7,8,9;

GRANT SELECT ON dm_b2b.weekly_core_metrics TO tableau;