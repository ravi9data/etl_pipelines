DROP TABLE IF EXISTS dwh.daily_kpi_subscription_value_metrics;
CREATE TABLE dwh.daily_kpi_subscription_value_metrics AS
WITH dates AS (
    SELECT DISTINCT
        datum AS fact_date
    FROM public.dim_dates
    WHERE datum BETWEEN current_date - 65 AND current_date -1
)
   ,account_data AS (
    SELECT DISTINCT c.customer_id
          ,ka.account_owner
          ,COALESCE(segment,'Self Service') AS segment
    FROM master.customer c
             LEFT JOIN dm_b2b.v_key_accounts ka ON ka.customer_id = c.customer_id
    WHERE customer_type = 'business_customer'
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
              WHEN ss.store_commercial = 'B2B International' 
                  THEN 'B2B ' || ss.country_name
              ELSE ss.store_commercial END AS store_commercial_split
         ,ss.new_recurring
         ,ss.category_name
         ,COALESCE((CASE WHEN ss.cancellation_reason_churn = 'customer request' AND ss.cancellation_reason_new = 'SOLD 1-EUR' THEN 'Customer Request - Sold 1 EUR'
                         WHEN ss.cancellation_reason_churn = 'customer request' AND ss.cancellation_reason_new = 'SOLD EARLY' THEN 'Customer Request - Sold Early'
                         WHEN ss.cancellation_reason_churn = 'customer request' AND ss.cancellation_reason_new <> 'SOLD 1-EUR' AND ss.cancellation_reason_new <> 'SOLD EARLY'
                             THEN 'Customer Request - Other'
                         ELSE ss.cancellation_reason_churn END)
         ,'not cancelled') AS cancellation_reason_churn
         ,ad.segment
         ,COALESCE(SUM(ss.subscription_value_eur),0) AS active_subscription_value
         ,COUNT(distinct ss.subscription_id) AS active_subscriptions
    FROM dates dat
             LEFT JOIN ods_production.subscription_phase_mapping ss
                       ON dat.fact_date::date >= ss.fact_day::date
                           AND dat.fact_date::date <= coalesce(ss.end_date::date, dat.fact_date::date+1)
             LEFT JOIN account_data ad on ad.customer_id = ss.customer_id
    WHERE TRUE
      AND ss.store_label NOT ilike '%old%'
      AND ss.country_name <> 'United Kingdom'
      AND ss.new_recurring IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7,8
)
, cancelled_subs_metrics_rented_not_rented AS (
	SELECT 
		dat.fact_date,
		country_name,
		CASE
              WHEN country_name in ('Austria','Netherlands','Spain')
                  THEN 'Rest of Europe'
              WHEN country_name = 'United States'
                  THEN 'United States Region'
              WHEN country_name = 'Germany'
                  THEN 'Germany Region'
              ELSE country_name END AS region,
        CASE
              WHEN s.store_commercial = 'Grover International'
                  THEN 'Grover' || ' ' || s.country_name
              WHEN s.store_commercial = 'Partnerships International'
                  THEN 'Partnerships' || ' ' || s.country_name
              WHEN s.store_commercial = 'B2B International' 
                  THEN 'B2B ' || s.country_name
              ELSE s.store_commercial END AS store_commercial_split,
         new_recurring,
         category_name,
         COALESCE((CASE WHEN cancellation_reason_churn = 'customer request' AND cancellation_reason_new = 'SOLD 1-EUR' THEN 'Customer Request - Sold 1 EUR'
                         WHEN cancellation_reason_churn = 'customer request' AND cancellation_reason_new = 'SOLD EARLY' THEN 'Customer Request - Sold Early'
                         WHEN cancellation_reason_churn = 'customer request' AND cancellation_reason_new <> 'SOLD 1-EUR' AND cancellation_reason_new <> 'SOLD EARLY'
                             THEN 'Customer Request - Other'
                         ELSE cancellation_reason_churn END), 'not cancelled') AS cancellation_reason_churn,
         segment,
         SUM(new_subscriptions_rented_again) AS new_subscriptions_rented_again,
         SUM(new_subscription_value_rented_again) AS new_subscription_value_rented_again,
         SUM(not_rented_again_subscriptions) AS not_rented_again_subscriptions,
         SUM(not_rented_again_subscriptions_value) AS not_rented_again_subscriptions_value,
         SUM(rented_again_subscriptions) AS rented_again_subscriptions,
         SUM(rented_again_subscriptions_value) AS rented_again_subscriptions_value		
	FROM dates dat
             INNER JOIN dwh.daily_kpi_rented_not_rented_again_subscriptions s 
             	ON dat.fact_date = s.cancellation_date::date
    GROUP BY 1,2,3,4,5,6,7,8
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
              WHEN s.store_commercial = 'B2B International' 
                  THEN 'B2B ' || s.country_name
              ELSE s.store_commercial END AS store_commercial_split
         ,o.new_recurring
         ,s.category_name
         ,COALESCE((CASE WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new = 'SOLD 1-EUR' THEN 'Customer Request - Sold 1 EUR'
                         WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new = 'SOLD EARLY' THEN 'Customer Request - Sold Early'
                         WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new <> 'SOLD 1-EUR' AND cr.cancellation_reason_new <> 'SOLD EARLY'
                             THEN 'Customer Request - Other'
                         ELSE cr.cancellation_reason_churn END), 'not cancelled') AS cancellation_reason_churn
         ,ad.segment
         ,COALESCE(SUM(s.subscription_value_euro),0) AS cancelled_subscription_value
         ,COALESCE(COUNT(s.subscription_id),0) AS nr_cancellations
    FROM dates dat
             INNER JOIN ods_production.subscription s
                        ON dat.fact_date = s.cancellation_date::date
             LEFT JOIN ods_production.order_retention_group o
                       ON s.order_id = o.order_id
             LEFT JOIN ods_production.subscription_cancellation_reason cr
                       ON s.subscription_id = cr.subscription_id
             LEFT JOIN account_data ad on ad.customer_id = s.customer_id
    WHERE TRUE
      AND s.store_label NOT ILIKE '%old%'
      AND s.country_name <> 'United Kingdom'
      AND o.new_recurring IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8
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
              WHEN s.store_commercial = 'B2B International' 
                  THEN 'B2B ' || s.country_name
              ELSE s.store_commercial END AS store_commercial_split
         ,o.new_recurring
         ,s.category_name
         ,COALESCE((CASE WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new = 'SOLD 1-EUR' THEN 'Customer Request - Sold 1 EUR'
                         WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new = 'SOLD EARLY' THEN 'Customer Request - Sold Early'
                         WHEN cr.cancellation_reason_churn = 'customer request' AND cr.cancellation_reason_new <> 'SOLD 1-EUR' AND cr.cancellation_reason_new <> 'SOLD EARLY'
                             THEN 'Customer Request - Other'
                         ELSE cr.cancellation_reason_churn END)
         ,'not cancelled') AS cancellation_reason_churn
         ,ad.segment
         ,COALESCE(COUNT(s.subscription_id),0) AS acquired_subscriptions
         ,COALESCE(SUM(s.subscription_value_euro),0) AS acquired_subscription_value
    FROM dates dat
             INNER JOIN ods_production.subscription s
                        ON dat.fact_date = s.start_date::date
             LEFT JOIN ods_production.order_retention_group o
                       ON s.order_id = o.order_id
             LEFT JOIN ods_production.subscription_cancellation_reason cr
                       ON s.subscription_id = cr.subscription_id
             LEFT JOIN account_data ad on ad.customer_id = s.customer_id
    WHERE TRUE
      AND s.store_label NOT ILIKE '%old%'
      AND s.country_name <> 'United Kingdom'
      AND o.new_recurring IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8
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
         ,segment
         ,COALESCE(SUM(act.active_subscription_value),0) AS active_subscription_value
         ,COALESCE(SUM(acq.acquired_subscriptions),0) AS acquired_subscriptions
         ,COALESCE(SUM(acq.acquired_subscription_value),0) AS acquired_subscription_value
         ,COALESCE(SUM(cm.cancelled_subscription_value),0) AS cancelled_subscription_value
         ,COALESCE(SUM(cm.nr_cancellations),0) AS nr_cancellations
         ,COALESCE(SUM(crn.new_subscriptions_rented_again),0) AS new_subscriptions_rented_again
         ,COALESCE(SUM(crn.new_subscription_value_rented_again),0) AS new_subscription_value_rented_again
         ,COALESCE(SUM(crn.not_rented_again_subscriptions),0) AS not_rented_again_subscriptions
         ,COALESCE(SUM(crn.not_rented_again_subscriptions_value),0) AS not_rented_again_subscriptions_value
         ,COALESCE(SUM(crn.rented_again_subscriptions),0) AS rented_again_subscriptions
         ,COALESCE(SUM(crn.rented_again_subscriptions_value),0) AS rented_again_subscriptions_value
    FROM active_metrics act
             FULL OUTER JOIN cancelled_metrics cm USING (fact_date,country_name,region,store_commercial_split,new_recurring,category_name,cancellation_reason_churn, segment)
             FULL OUTER JOIN acquired_metrics acq USING (fact_date,country_name,region,store_commercial_split,new_recurring,category_name,cancellation_reason_churn, segment)
             FULL OUTER JOIN cancelled_subs_metrics_rented_not_rented crn USING (fact_date,country_name,region,store_commercial_split,new_recurring,category_name,cancellation_reason_churn, segment)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
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
         ,segment
         ,SUM(active_subscription_value) AS active_subscription_value
         ,SUM(acquired_subscriptions) AS acquired_subscriptions
         ,SUM(acquired_subscription_value) AS acquired_subscription_value
         ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
         ,SUM(nr_cancellations) AS nr_cancellations
         ,SUM(new_subscriptions_rented_again) AS new_subscriptions_rented_again
         ,SUM(new_subscription_value_rented_again) AS new_subscription_value_rented_again
         ,SUM(not_rented_again_subscriptions) AS not_rented_again_subscriptions
         ,SUM(not_rented_again_subscriptions_value) AS not_rented_again_subscriptions_value
         ,SUM(rented_again_subscriptions) AS rented_again_subscriptions
         ,SUM(rented_again_subscriptions_value) AS rented_again_subscriptions_value
    FROM metrics
    GROUP BY 1,2,3,4,5,6,7,8
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
         ,segment
         ,SUM(active_subscription_value) AS active_subscription_value
         ,SUM(acquired_subscriptions) AS acquired_subscriptions
         ,SUM(acquired_subscription_value) AS acquired_subscription_value
         ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
         ,SUM(nr_cancellations) AS nr_cancellations
         ,SUM(new_subscriptions_rented_again) AS new_subscriptions_rented_again
         ,SUM(new_subscription_value_rented_again) AS new_subscription_value_rented_again
         ,SUM(not_rented_again_subscriptions) AS not_rented_again_subscriptions
         ,SUM(not_rented_again_subscriptions_value) AS not_rented_again_subscriptions_value
         ,SUM(rented_again_subscriptions) AS rented_again_subscriptions
         ,SUM(rented_again_subscriptions_value) AS rented_again_subscriptions_value
    FROM metrics
    GROUP BY 1,2,3,4,5,6,7,8
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
         ,segment
         ,SUM(active_subscription_value) AS active_subscription_value
         ,SUM(acquired_subscriptions) AS acquired_subscriptions
         ,SUM(acquired_subscription_value) AS acquired_subscription_value
         ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
         ,SUM(nr_cancellations) AS nr_cancellations
         ,SUM(new_subscriptions_rented_again) AS new_subscriptions_rented_again
         ,SUM(new_subscription_value_rented_again) AS new_subscription_value_rented_again
         ,SUM(not_rented_again_subscriptions) AS not_rented_again_subscriptions
         ,SUM(not_rented_again_subscriptions_value) AS not_rented_again_subscriptions_value
         ,SUM(rented_again_subscriptions) AS rented_again_subscriptions
         ,SUM(rented_again_subscriptions_value) AS rented_again_subscriptions_value
    FROM metrics_region_aggregated
    GROUP BY 1,2,3,4,5,6,7,8
)

SELECT * FROM metrics
UNION ALL
SELECT * FROM metrics_country_aggregated
UNION ALL
SELECT * FROM metrics_region_aggregated
UNION ALL
SELECT * FROM metrics_total_aggregated;

DROP TABLE IF EXISTS dwh.daily_kpi_subscription_value_metrics_combined;
CREATE TABLE dwh.daily_kpi_subscription_value_metrics_combined AS
with metrics_new_rec_aggregated AS (
    SELECT
        fact_date
         ,country_name
         ,region
         ,store_commercial_split
         ,'TOTAL'::TEXT AS new_recurring
         ,category_name
         ,cancellation_reason_churn
         ,segment
         ,SUM(active_subscription_value) AS active_subscription_value
         ,SUM(acquired_subscriptions) AS acquired_subscriptions
         ,SUM(acquired_subscription_value) AS acquired_subscription_value
         ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
         ,SUM(nr_cancellations) AS nr_cancellations
         ,SUM(new_subscriptions_rented_again) AS new_subscriptions_rented_again
         ,SUM(new_subscription_value_rented_again) AS new_subscription_value_rented_again
         ,SUM(not_rented_again_subscriptions) AS not_rented_again_subscriptions
         ,SUM(not_rented_again_subscriptions_value) AS not_rented_again_subscriptions_value
         ,SUM(rented_again_subscriptions) AS rented_again_subscriptions
         ,SUM(rented_again_subscriptions_value) AS rented_again_subscriptions_value
    FROM dwh.daily_kpi_subscription_value_metrics
    GROUP BY 1,2,3,4,5,6,7,8
)
   ,metrics_cat_aggregated AS (
    SELECT
        fact_date
         ,country_name
         ,region
         ,store_commercial_split
         ,'TOTAL'::TEXT AS new_recurring
         ,category_name
         ,'TOTAL'::TEXT AS cancellation_reason_churn
         ,segment
         ,SUM(active_subscription_value) AS active_subscription_value
         ,SUM(acquired_subscriptions) AS acquired_subscriptions
         ,SUM(acquired_subscription_value) AS acquired_subscription_value
         ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
         ,SUM(nr_cancellations) AS nr_cancellations
         ,SUM(new_subscriptions_rented_again) AS new_subscriptions_rented_again
         ,SUM(new_subscription_value_rented_again) AS new_subscription_value_rented_again
         ,SUM(not_rented_again_subscriptions) AS not_rented_again_subscriptions
         ,SUM(not_rented_again_subscriptions_value) AS not_rented_again_subscriptions_value
         ,SUM(rented_again_subscriptions) AS rented_again_subscriptions
         ,SUM(rented_again_subscriptions_value) AS rented_again_subscriptions_value
    FROM dwh.daily_kpi_subscription_value_metrics
    GROUP BY 1,2,3,4,5,6,7,8
)
   ,metrics_cat_can_aggregated AS (
    SELECT
        fact_date
         ,country_name
         ,region
         ,store_commercial_split
         ,'TOTAL'::TEXT AS new_recurring
         ,category_name
         ,'TOTAL'::TEXT AS cancellation_reason_churn
         ,'TOTAL'::TEXT AS segment
         ,SUM(active_subscription_value) AS active_subscription_value
         ,SUM(acquired_subscriptions) AS acquired_subscriptions
         ,SUM(acquired_subscription_value) AS acquired_subscription_value
         ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
         ,SUM(nr_cancellations) AS nr_cancellations
         ,SUM(new_subscriptions_rented_again) AS new_subscriptions_rented_again
         ,SUM(new_subscription_value_rented_again) AS new_subscription_value_rented_again
         ,SUM(not_rented_again_subscriptions) AS not_rented_again_subscriptions
         ,SUM(not_rented_again_subscriptions_value) AS not_rented_again_subscriptions_value
         ,SUM(rented_again_subscriptions) AS rented_again_subscriptions
         ,SUM(rented_again_subscriptions_value) AS rented_again_subscriptions_value
    FROM dwh.daily_kpi_subscription_value_metrics
    GROUP BY  1,2,3,4,5,6,7,8
)

   ,metrics_segment_aggregated AS (
    SELECT
        fact_date
         ,country_name
         ,region
         ,store_commercial_split
         ,'TOTAL'::TEXT AS new_recurring
         ,'TOTAL'::TEXT AS category_name
         ,'TOTAL'::TEXT AS cancellation_reason_churn
         ,'TOTAL'::TEXT AS segment
         ,SUM(active_subscription_value) AS active_subscription_value
         ,SUM(acquired_subscriptions) AS acquired_subscriptions
         ,SUM(acquired_subscription_value) AS acquired_subscription_value
         ,SUM(cancelled_subscription_value) AS cancelled_subscription_value
         ,SUM(nr_cancellations) AS nr_cancellations
         ,SUM(new_subscriptions_rented_again) AS new_subscriptions_rented_again
         ,SUM(new_subscription_value_rented_again) AS new_subscription_value_rented_again
         ,SUM(not_rented_again_subscriptions) AS not_rented_again_subscriptions
         ,SUM(not_rented_again_subscriptions_value) AS not_rented_again_subscriptions_value
         ,SUM(rented_again_subscriptions) AS rented_again_subscriptions
         ,SUM(rented_again_subscriptions_value) AS rented_again_subscriptions_value
    FROM dwh.daily_kpi_subscription_value_metrics
    GROUP BY 1,2,3,4,5,6,7,8
)


SELECT *
FROM dwh.daily_kpi_subscription_value_metrics
UNION ALL
SELECT *
FROM metrics_new_rec_aggregated
UNION ALL
SELECT *
FROM metrics_cat_aggregated
UNION ALL
SELECT *
FROM metrics_cat_can_aggregated
UNION ALL
SELECT *
FROM metrics_segment_aggregated;

DROP TABLE IF EXISTS dwh.daily_kpi_subscription_value_targets;
CREATE TABLE dwh.daily_kpi_subscription_value_targets AS
WITH targets_new_rec_aggregated AS (
    SELECT
        datum
         ,country AS country_name
         ,region
         ,store AS store_commercial_split
         ,'TOTAL'::TEXT AS new_recurring
         ,categories AS category_name
         ,'TOTAL'::TEXT AS cancellation_reason_churn
         ,'TOTAL'::TEXT AS segment
         ,SUM(active_subs_value) AS active_subs_value_target
         ,SUM(incremental_subs_value) AS incremental_subs_value_target
         ,SUM(acquired_subs_value) AS acquired_subs_value_target
         ,SUM(cancelled_sub_value) AS cancelled_subs_value_target
    FROM marketing.marketing_daily_asv_targets
    GROUP BY  1,2,3,4,5,6,7,8
)
   ,targets_cat_aggregated AS (
    SELECT
        datum
         ,country AS country_name
         ,region
         ,store AS store_commercial_split
         ,'TOTAL'::TEXT AS new_recurring
         ,'TOTAL'::TEXT AS category_name
         ,'TOTAL'::TEXT AS cancellation_reason_churn
         ,'TOTAL'::TEXT AS segment
         ,SUM(active_subs_value) AS active_subs_value_target
         ,SUM(incremental_subs_value) AS incremental_subs_value_target
         ,SUM(acquired_subs_value) AS acquired_subs_value_target
         ,SUM(cancelled_sub_value) AS cancelled_subs_value_target
    FROM marketing.marketing_daily_asv_targets
    GROUP BY  1,2,3,4,5,6,7,8
)

  SELECT *
  FROM targets_new_rec_aggregated
  UNION ALL
  SELECT *
  FROM targets_cat_aggregated;