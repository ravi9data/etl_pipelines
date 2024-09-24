DROP VIEW IF EXISTS dm_finance.v_asv_historical_report;
CREATE VIEW dm_finance.v_asv_historical_report AS
WITH dates AS (
SELECT DISTINCT
 datum AS fact_date
FROM public.dim_dates
WHERE (datum BETWEEN '2019-01-01' AND current_date -1)
AND (datum = last_day(datum) OR datum = current_date -1)
)
,acquisition_cohort AS (
SELECT DISTINCT 
  cac.customer_id,
  date_trunc('year',(cac.customer_acquisition_cohort)::DATE) AS customer_acquisition_cohort_year
FROM ods_production.customer_acquisition_cohort cac 
)
,active_metrics AS (
SELECT
  dat.fact_date
 ,ss.country_name
 ,CASE
  WHEN ss.country_name in ('Austria','Netherlands','Spain')
  THEN 'Rest of Europe'
  WHEN ss.country_name = 'United States'
  THEN 'United States Region'
  WHEN ss.country_name = 'Germany'
  THEN 'Germany Region'
  ELSE ss.country_name END AS region
 ,CASE
  WHEN ss.store_commercial = 'Grover International'
  THEN 'Grover' || ' ' || ss.country_name
  WHEN ss.store_commercial = 'Partnerships International'
  THEN 'Partnerships' || ' ' || ss.country_name
  ELSE ss.store_commercial END AS store_commercial_split
  ,ss.new_recurring
  ,ss.category_name
  ,ac.customer_acquisition_cohort_year
  ,ss.subscription_plan 
  ,ss.customer_type
  ,COALESCE((CASE WHEN ss.cancellation_reason_churn = 'customer request' AND ss.cancellation_reason_new = 'SOLD 1-EUR' THEN 'Customer Request - Sold 1 EUR'
                WHEN ss.cancellation_reason_churn = 'customer request' AND ss.cancellation_reason_new = 'SOLD EARLY' THEN 'Customer Request - Sold Early'
                WHEN ss.cancellation_reason_churn = 'customer request' AND ss.cancellation_reason_new <> 'SOLD 1-EUR' AND ss.cancellation_reason_new <> 'SOLD EARLY'
                        THEN 'Customer Request - Other'
            ELSE ss.cancellation_reason_churn END)
            , 'not cancelled') AS cancellation_reason_churn
  ,COALESCE(SUM(ss.subscription_value_eur),0) AS active_subscription_value
  ,COUNT(distinct ss.subscription_id) AS active_subscriptions
FROM dates dat
LEFT JOIN ods_production.subscription_phase_mapping ss
          ON dat.fact_date::date >= ss.fact_day::date
          AND dat.fact_date::date <= coalesce(ss.end_date::date, dat.fact_date::date+1)
LEFT JOIN acquisition_cohort ac          
          ON ss.customer_id = ac.customer_id
WHERE TRUE
 AND ss.store_label NOT ilike '%old%'
 AND ss.country_name <> 'United Kingdom'
 AND ss.new_recurring IS NOT NULL
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
,cancelled_metrics as(
  SELECT
  dat.fact_date
 ,s.country_name
 ,CASE
  WHEN s.country_name in ('Austria','Netherlands','Spain')
  THEN 'Rest of Europe'
  WHEN s.country_name = 'United States'
  THEN 'United States Region'
  WHEN s.country_name = 'Germany'
  THEN 'Germany Region'
  ELSE s.country_name END AS region
 ,CASE
  WHEN s.store_commercial = 'Grover International'
  THEN 'Grover' || ' ' || s.country_name
  WHEN s.store_commercial = 'Partnerships International'
  THEN 'Partnerships' || ' ' || s.country_name
  ELSE s.store_commercial END AS store_commercial_split
  ,o.new_recurring
  ,s.category_name
  ,ac.customer_acquisition_cohort_year
  ,s.subscription_plan
  ,s.customer_type
  ,COALESCE((CASE WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new = 'SOLD 1-EUR' THEN 'Customer Request - Sold 1 EUR'
                WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new = 'SOLD EARLY' THEN 'Customer Request - Sold Early'
                WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new <> 'SOLD 1-EUR' AND cr.cancellation_reason_new <> 'SOLD EARLY'
                        THEN 'Customer Request - Other'
            ELSE cr.cancellation_reason_churn END), 'not cancelled') AS cancellation_reason_churn
  ,COALESCE(SUM(s.subscription_value_euro),0) AS cancelled_subscription_value
  ,COALESCE(COUNT(s.subscription_id),0) AS nr_cancellations
FROM dates dat
 INNER JOIN master.subscription s
  ON dat.fact_date = s.cancellation_date::date
 LEFT JOIN ods_production.order_retention_group o
  ON s.order_id = o.order_id
 LEFT JOIN ods_production.subscription_cancellation_reason cr
  ON s.subscription_id = cr.subscription_id
 LEFT JOIN  acquisition_cohort ac          
  ON s.customer_id = ac.customer_id
WHERE TRUE
 AND s.store_label NOT ILIKE '%old%'
 AND s.country_name <> 'United Kingdom'
 AND o.new_recurring IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,acquired_metrics as(
  SELECT
  dat.fact_date
 ,s.country_name
 ,CASE
  WHEN s.country_name in ('Austria','Netherlands','Spain')
  THEN 'Rest of Europe'
  WHEN s.country_name = 'United States'
  THEN 'United States Region'
  WHEN s.country_name = 'Germany'
  THEN 'Germany Region'
  ELSE s.country_name END AS region
 ,CASE
  WHEN s.store_commercial = 'Grover International'
  THEN 'Grover' || ' ' || s.country_name
  WHEN s.store_commercial = 'Partnerships International'
  THEN 'Partnerships' || ' ' || s.country_name
  ELSE s.store_commercial END AS store_commercial_split
  ,o.new_recurring
  ,s.category_name
  ,ac.customer_acquisition_cohort_year
  ,s.subscription_plan
  ,s.customer_type
  ,COALESCE((CASE WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new = 'SOLD 1-EUR' THEN 'Customer Request - Sold 1 EUR'
                WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new = 'SOLD EARLY' THEN 'Customer Request - Sold Early'
                WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new <> 'SOLD 1-EUR' AND cr.cancellation_reason_new <> 'SOLD EARLY'
                        THEN 'Customer Request - Other'
            ELSE cr.cancellation_reason_churn END)
            , 'not cancelled') AS cancellation_reason_churn
  ,COALESCE(COUNT(s.subscription_id),0) AS acquired_subscriptions
  ,COALESCE(SUM(s.subscription_value_euro),0) AS acquired_subscription_value
FROM dates dat
 INNER JOIN master.subscription s
  ON dat.fact_date = s.start_date::date
 LEFT JOIN ods_production.order_retention_group o
  ON s.order_id = o.order_id
 LEFT JOIN ods_production.subscription_cancellation_reason cr
  ON s.subscription_id = cr.subscription_id
 LEFT JOIN  acquisition_cohort ac          
  ON s.customer_id = ac.customer_id
WHERE TRUE
 AND s.store_label NOT ILIKE '%old%'
 AND s.country_name <> 'United Kingdom'
 AND o.new_recurring IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,metrics as (
SELECT
    fact_date
   ,country_name
   ,region
   ,store_commercial_split
   ,new_recurring
   ,category_name
   ,cancellation_reason_churn
   ,customer_acquisition_cohort_year
   ,subscription_plan
   ,customer_type
   ,COALESCE(SUM(act.active_subscription_value),0) AS active_subscription_value
   ,COALESCE(SUM(acq.acquired_subscription_value),0) AS acquired_subscription_value
   ,COALESCE(SUM(cm.cancelled_subscription_value),0) AS cancelled_subscription_value
   ,COALESCE(SUM(cm.nr_cancellations),0) AS nr_cancellations
FROM active_metrics act
FULL OUTER JOIN cancelled_metrics cm USING (fact_date,country_name,region,store_commercial_split,new_recurring,category_name,cancellation_reason_churn,customer_acquisition_cohort_year,subscription_plan,customer_type)
FULL OUTER JOIN acquired_metrics acq USING (fact_date,country_name,region,store_commercial_split,new_recurring,category_name,cancellation_reason_churn,customer_acquisition_cohort_year,subscription_plan,customer_type)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)
,metrics_region_aggregated AS (
SELECT
  fact_date
  ,region AS country_name
  ,region
  ,region || ' ' || 'Total'::TEXT AS store_commercial_split
  ,new_recurring
  ,category_name
  ,cancellation_reason_churn
  ,customer_acquisition_cohort_year
  ,subscription_plan
  ,customer_type			
  ,SUM(active_subscription_value) AS active_subscription_value
  ,SUM(acquired_subscription_value) AS acquired_subscription_value
  ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
  ,SUM(nr_cancellations) AS nr_cancellations
FROM metrics
GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,metrics_country_aggregated AS (
SELECT
  fact_date
  ,country_name
  ,region
  ,country_name || ' ' || 'Total'::TEXT AS store_commercial_split
  ,new_recurring
  ,category_name
  ,cancellation_reason_churn
  ,customer_acquisition_cohort_year
  ,subscription_plan
  ,customer_type
  ,SUM(active_subscription_value) AS active_subscription_value
  ,SUM(acquired_subscription_value) AS acquired_subscription_value
  ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
  ,SUM(nr_cancellations) AS nr_cancellations
FROM metrics
GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,metrics_total_aggregated AS (
SELECT
  fact_date
  ,'Total'::TEXT as country_name
  ,'Total'::TEXT as region
  ,'Total'::TEXT as store_commercial_split
  ,new_recurring
  ,category_name
  ,cancellation_reason_churn
  ,customer_acquisition_cohort_year
  ,subscription_plan
  ,customer_type
  ,SUM(active_subscription_value) AS active_subscription_value
  ,SUM(acquired_subscription_value) AS acquired_subscription_value
  ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
  ,SUM(nr_cancellations) AS nr_cancellations
FROM metrics_region_aggregated
GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT * FROM metrics
UNION ALL
SELECT * FROM metrics_country_aggregated
UNION ALL
SELECT * FROM metrics_region_aggregated
UNION ALL
SELECT * FROM metrics_total_aggregated
WITH NO SCHEMA BINDING;