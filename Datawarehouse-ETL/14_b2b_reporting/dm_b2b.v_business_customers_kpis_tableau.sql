CREATE OR REPLACE VIEW dm_b2b.v_business_customers_kpis_tableau AS
WITH customers AS (
SELECT DISTINCT 
  datum
 ,d.day_is_last_of_month
 ,CASE	
	 WHEN d.week_day_number = 7 
	  THEN 1
	 WHEN d.datum = CURRENT_DATE 
	  THEN 1
	 ELSE 0 
  END AS day_is_end_of_week
 ,o.customer_id
 ,cac.customer_acquisition_cohort::DATE AS customer_acquisition_cohort
 ,fm.is_freelancer
 ,CASE 
  WHEN COALESCE(paid_orders, 0) > 1 
   THEN 'Recurring Customer'
	ELSE 'New Customer' 
 END AS new_recurring_customer
FROM public.dim_dates d
  LEFT JOIN 
   (SELECT 
		 d.datum AS fact_date,
		 o.customer_id, 
		 COALESCE(COUNT(CASE 
		 	WHEN paid_date IS NOT NULL 
		 	 THEN order_id 
		 END),0) AS paid_orders
	  FROM public.dim_dates d
	    LEFT JOIN master.order o
	      ON d.datum >= o.created_date::DATE 
	  WHERE customer_type = 'business_customer'
	  GROUP BY 1, 2) AS o 
	 LEFT JOIN ods_production.companies c2
	   ON c2.customer_id = o.customer_id
   LEFT JOIN ods_production.customer_acquisition_cohort cac 
	   ON o.customer_id = cac.customer_id
   LEFT JOIN dm_risk.b2b_freelancer_mapping fm
     ON c2.company_type_name = fm.company_type_name
	   ON fact_date = datum
WHERE TRUE
  AND datum <= CURRENT_DATE
  AND datum >= DATE_TRUNC('MONTH', DATEADD('MONTH', -13, CURRENT_DATE))
)
,orders AS (
SELECT 
  d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.new_recurring_customer
 ,COUNT(DISTINCT order_id) AS completed_orders
 ,COUNT(DISTINCT CASE WHEN status ='PAID' THEN order_id END) AS paid_orders
FROM customers d 
LEFT JOIN master."order" o
  ON d.datum = created_date::DATE
 AND d.customer_id = o.customer_id
 AND submitted_date::DATE IS NOT NULL
GROUP BY 1,2,3,4,5,6
)
, active_info AS (
SELECT 
	d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.new_recurring_customer
 ,SUM(COALESCE(active_subscriptions,0)) AS active_subscriptions
 ,COUNT(DISTINCT s.customer_id) AS active_customers
 ,SUM(COALESCE(active_subscription_value,0)) AS active_subscription_value
 ,SUM(COALESCE(active_subscription_value,0)) * 12 AS active_subscription_value_yearly 
FROM customers d
  LEFT JOIN ods_finance.active_subscriptions_overview s
    ON d.datum = s.fact_date  
   AND d.customer_id = s.customer_id
GROUP BY 1,2,3,4,5,6
)
,arr_periods_monthly AS (
SELECT DISTINCT
  datum
 ,customer_id
 ,CASE 
   WHEN active_subscription_value_yearly >= 100000 
    THEN 'arr_above_100k'
   WHEN active_subscription_value_yearly >= 5000 AND active_subscription_value_yearly < 100000
    THEN 'arr_bet_5k_100k'
   WHEN active_subscription_value_yearly > 0 AND active_subscription_value_yearly < 5000 
    THEN 'arr_below_5k' 
  END AS arr_class
FROM active_info
WHERE datum = LAST_DAY(datum) OR datum = CURRENT_DATE 
GROUP BY 1,2,3
)
,arr_periods_weekly AS (
SELECT DISTINCT
  datum
 ,customer_id
 ,CASE 
   WHEN active_subscription_value_yearly >= 100000 
    THEN 'arr_above_100k'
   WHEN active_subscription_value_yearly >= 5000 AND active_subscription_value_yearly < 100000
    THEN 'arr_bet_5k_100k'
   WHEN active_subscription_value_yearly > 0 AND active_subscription_value_yearly < 5000 
    THEN 'arr_below_5k'    
  END AS arr_class
FROM active_info
WHERE datum = DATEADD(DAY,6, DATE_TRUNC('week', datum)::date) OR datum = CURRENT_DATE 
GROUP BY 1,2,3
) 
,arr_periods_quarterly AS (
SELECT DISTINCT
  datum 
 ,customer_id 
 ,CASE 
   WHEN active_subscription_value_yearly >= 100000 
    THEN 'arr_above_100k'
   WHEN active_subscription_value_yearly >= 5000 AND active_subscription_value_yearly < 100000
    THEN 'arr_bet_5k_100k'
   WHEN active_subscription_value_yearly > 0 AND active_subscription_value_yearly < 5000 
    THEN 'arr_below_5k'    
  END AS arr_class
FROM active_info
WHERE datum = LAST_DAY(DATEADD('month',2,DATE_TRUNC('quarter', datum)::DATE)) OR datum = CURRENT_DATE 
GROUP BY 1,2,3
)
, new_subs AS (
SELECT 
  d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.new_recurring_customer
 ,COUNT(DISTINCT s.subscription_id) AS new_subscriptions
 ,SUM(s.subscription_value) AS new_subscription_value
 ,SUM(s.committed_sub_value + s.additional_committed_sub_value) AS committed_subscription_revenue
FROM customers d 
LEFT JOIN master.subscription s
  ON d.datum = s.start_date::DATE
 AND d.customer_id = s.customer_id
GROUP BY 1,2,3,4,5,6
)
, cancellation AS (
SELECT 
	d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.new_recurring_customer
 ,COUNT(DISTINCT s.subscription_id) AS cancelled_subscriptions
 ,SUM(s.subscription_value) AS cancelled_subscription_value
 ,COUNT(DISTINCT CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS cancelled_subscriptions_failed_delivery
 ,SUM(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value ELSE 0 END) AS cancelled_subscription_value_failed_delivery
FROM customers d 
  LEFT JOIN master.subscription s
    ON d.datum =  s.cancellation_date::DATE
   AND d.customer_id = s.customer_id
  LEFT JOIN ods_production.subscription_cancellation_reason c 
    ON s.subscription_id = c.subscription_id
GROUP BY 1,2,3,4,5,6
)
,revenue AS (
SELECT 
  d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.new_recurring_customer
 ,SUM(amount_paid) collected_subscription_revenue
FROM customers d
  LEFT JOIN master.subscription_payment sp 
    ON d.customer_id = sp.customer_id
    AND d.datum = sp.paid_date::DATE
GROUP BY 1,2,3,4,5,6
)
SELECT DISTINCT
  c.datum AS fact_date
 ,c.day_is_last_of_month
 ,c.day_is_end_of_week
 ,c.is_freelancer
 ,c.new_recurring_customer
 ,COALESCE(com.company_type_name, 'Null') AS company_type_name
 ,CASE
   WHEN COALESCE(c.is_freelancer, 0) = 1 AND active_subscriptions >= 5 
    THEN 'freelancer >= 5'
   WHEN COALESCE(c.is_freelancer, 0) = 1 AND  active_subscriptions  < 5 
    THEN 'freelancer < 5'
   WHEN COALESCE(c.is_freelancer, 0) = 0 AND  active_subscriptions  >= 5 
    THEN 'non freelancer >= 5'
   WHEN COALESCE(c.is_freelancer, 0) = 0 AND  active_subscriptions  < 5 
    THEN 'non freelancer < 5'
   ELSE 'validate'
  END AS b2b_classification
 ,COALESCE(SUM(active_subscriptions),0) AS active_subscriptions
 ,COALESCE(SUM(active_subscription_value),0) AS active_subscription_value
 ,COALESCE(SUM(active_customers), 0) AS active_customers
 ,COALESCE(COUNT(DISTINCT CASE 
   WHEN monthly_arr.arr_class = 'arr_above_100k' 
    THEN  monthly_arr.customer_id 
  END), 0) AS monthly_customer_with_100k_arr
 ,COALESCE(COUNT(DISTINCT CASE 
   WHEN monthly_arr.arr_class = 'arr_bet_5k_100k' 
    THEN  monthly_arr.customer_id 
  END), 0) AS monthly_customer_with_5k_and_100k_arr
 ,COALESCE(COUNT(DISTINCT CASE
   WHEN monthly_arr.arr_class = 'arr_below_5k' 
    THEN monthly_arr.customer_id 
  END), 0) AS monthly_customer_with_below_5k_arr
 ,COALESCE(COUNT(DISTINCT CASE 
   WHEN weekly_arr.arr_class = 'arr_above_100k'
    THEN weekly_arr.customer_id 
  END), 0) AS weekly_customer_with_100k_arr
 ,COALESCE(COUNT(DISTINCT CASE 
   WHEN weekly_arr.arr_class = 'arr_bet_5k_100k' 
    THEN weekly_arr.customer_id 
  END), 0) AS weekly_customer_with_5k_and_100k_arr
 ,COALESCE(COUNT(DISTINCT CASE 
   WHEN weekly_arr.arr_class = 'arr_below_5k'
    THEN weekly_arr.customer_id 
  END), 0) AS weekly_customer_with_below_5k_arr
 ,COALESCE(COUNT(DISTINCT CASE 
   WHEN quarterly_arr.arr_class = 'arr_above_100k' 
    THEN quarterly_arr.customer_id 
  END), 0) AS quarterly_customer_with_100k_arr 
 ,COALESCE(COUNT(DISTINCT CASE 
   WHEN quarterly_arr.arr_class = 'arr_bet_5k_100k' 
    THEN quarterly_arr.customer_id 
  END), 0) AS quarterly_customer_with_5k_and_100k_arr
 ,COALESCE(COUNT(DISTINCT CASE 
   WHEN quarterly_arr.arr_class = 'arr_below_5k'
    THEN quarterly_arr.customer_id 
  END), 0) AS quarterly_customer_below_5k_arr
 ,COALESCE(COUNT(DISTINCT CASE
   WHEN c.datum = c.customer_acquisition_cohort
    THEN c.customer_id
  END),0) AS new_customers
 ,COALESCE(SUM(completed_orders), 0) AS completed_orders
 ,COALESCE(SUM(paid_orders), 0) AS paid_orders
 ,COALESCE(SUM(cancelled_subscriptions), 0) AS cancelled_subscriptions
 ,COALESCE(SUM(cancelled_subscription_value), 0) AS cancelled_subscription_value
 ,COALESCE(SUM(cancelled_subscriptions_failed_delivery), 0) AS cancelled_subscriptions_failed_delivery
 ,COALESCE(SUM(cancelled_subscription_value_failed_delivery), 0) AS cancelled_subscription_value_failed_delivery    
 ,COALESCE(SUM(new_subscriptions), 0) AS new_subscriptions
 ,COALESCE(SUM(new_subscription_value), 0) AS new_subscription_value
 ,COALESCE(SUM(ns.committed_subscription_revenue), 0) AS committed_subscription_revenue
 ,COALESCE(SUM(r.collected_subscription_revenue), 0) AS collected_subscription_revenue
FROM customers c
  LEFT JOIN ods_production.companies com
    ON c.customer_id = com.customer_id
  LEFT JOIN orders o 
    ON o.datum = c.datum
   AND o.customer_id = c.customer_id
   AND o.new_recurring_customer = c.new_recurring_customer
  LEFT JOIN active_info ai
    ON ai.datum = c.datum
   AND ai.customer_id = c.customer_id
   AND ai.new_recurring_customer = c.new_recurring_customer
  LEFT JOIN arr_periods_monthly monthly_arr 
    ON monthly_arr.datum = c.datum
   AND monthly_arr.customer_id = c.customer_id
  LEFT JOIN arr_periods_weekly weekly_arr
    ON weekly_arr.datum = c.datum
   AND weekly_arr.customer_id = c.customer_id
  LEFT JOIN arr_periods_quarterly quarterly_arr
    ON quarterly_arr.datum = c.datum
   AND quarterly_arr.customer_id = c.customer_id
  LEFT JOIN cancellation can 
    ON can.datum = c.datum 
   AND can.customer_id = c.customer_id
   AND can.new_recurring_customer = c.new_recurring_customer
  LEFT JOIN revenue r 
    ON r.datum = c.datum
   AND r.customer_id = c.customer_id
   AND r.new_recurring_customer = c.new_recurring_customer
  LEFT JOIN new_subs ns
    ON ns.datum = c.datum
   AND ns.customer_id = c.customer_id
   AND ns.new_recurring_customer = c.new_recurring_customer
GROUP BY 1,2,3,4,5,6,7
WITH NO SCHEMA BINDING
;