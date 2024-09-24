DROP TABLE IF EXISTS tmp_cancellation_prediction_results;
CREATE TEMP TABLE tmp_cancellation_prediction_results
SORTKEY(reporting_date, country, store, subcategory, category_name, cancelled_subscriptions_measure,
	cancelled_subscriptions_value, cancelled_asv_measure, cancelled_asv_value)
DISTKEY(reporting_date)
AS
WITH forecast_cancellation AS (
	SELECT
		cancellation_date::date AS reporting_date,
		subcategory,
		country,
		store,
		customer_type,
		cancelled_subscriptions AS cancelled_subscriptions_forecast,
		cancelled_asv AS cancelled_asv_forecast,
		update_date,
        ROW_NUMBER() OVER (PARTITION BY cancellation_date::date, subcategory, country, store, customer_type ORDER BY update_date DESC) AS row_num
	FROM data_science.subscription_cancellations_forecasting
)
, forecast_cancellation_non_official_targets AS (
	SELECT 
		cancellation_date::date AS reporting_date,
		subcategory,
		country,
		store,
		customer_type,
		cancelled_subscriptions AS cancelled_subscriptions_forecast,
		cancelled_asv AS cancelled_asv_forecast,
		update_date,
		ROW_NUMBER() OVER (PARTITION BY cancellation_date::date, subcategory, country, store, customer_type ORDER BY update_date DESC) AS row_num
	FROM data_science.subscription_cancellations_forecasting_1
)
, actual_values_cancellation AS (
	SELECT 
		date_trunc('month',cancellation_date)::date AS reporting_date,
		subcategory_name AS subcategory,
		country_name ,
		store_commercial ,
		customer_type,
		count(DISTINCT subscription_id) AS cancelled_subscriptions,
		SUM(subscription_value) AS cancelled_asv
	FROM dm_risk.v_subscription_data_cancellation_rate_forecast 
	WHERE cancellation_date IS NOT NULL	
		AND subcategory IN (SELECT DISTINCT subcategory FROM forecast_cancellation)
		AND cancellation_date::date >= '2022-01-01'
	GROUP BY 1,2,3,4,5
)
, targets AS (
	SELECT 
		date_trunc('month',datum)::date AS reporting_date,
		subcategory ,
		country,
		CASE WHEN customer_type = 'B2B' AND country = 'Germany' THEN 'B2B Germany'
	 		 WHEN customer_type = 'B2B'  THEN 'B2B International'
	 		 WHEN customer_type = 'B2C' AND country = 'Germany' THEN 'Grover Germany'
	 		 WHEN customer_type = 'B2C' THEN 'Grover International'
	 		 WHEN customer_type = 'Retail' AND country = 'Germany' THEN 'Partnerships Germany'
	 		 WHEN customer_type = 'Retail'  THEN 'Partnerships International'
	 		END AS store,
		CASE WHEN customer_type = 'B2C' THEN 'normal_customer' 
			 WHEN customer_type = 'B2B' THEN 'business_customer'
			 WHEN customer_type = 'Retail' THEN 'normal_customer' 
			END AS customer_type,
		SUM(cancelled_subscriptions) AS cancelled_subscriptions_target,
		SUM(cancelled_subvalue_daily) AS cancelled_asv_target
	FROM  dm_commercial.v_target_data_cancellation_rate_forecast
	WHERE subcategory IN (SELECT DISTINCT subcategory FROM forecast_cancellation)
	GROUP BY 1,2,3,4,5
)
, dates AS (
	SELECT DISTINCT 
		DATE_TRUNC('month',datum) AS reporting_date,
		subcategory,
		store,
		country,
		customer_type
	FROM public.dim_dates
	CROSS JOIN 
	(SELECT DISTINCT subcategory FROM forecast_cancellation)
	CROSS JOIN 
	(SELECT DISTINCT store FROM forecast_cancellation)
	CROSS JOIN 
	(SELECT DISTINCT country FROM forecast_cancellation)
	CROSS JOIN 
	(SELECT DISTINCT customer_type FROM forecast_cancellation)
	WHERE reporting_date >= '2022-01-01'
)
, total_before_pivoting AS (
	SELECT 
		d.reporting_date::date, 
		d.country,
		d.store,
		d.subcategory,
		pp.category_name,
		f.cancelled_subscriptions_forecast,
		f.cancelled_asv_forecast,
		ft.cancelled_subscriptions_forecast AS cancelled_subscriptions_forecast_not_official,
		ft.cancelled_asv_forecast AS cancelled_asv_forecast_not_official,
		a.cancelled_subscriptions AS cancelled_subscriptions_actual,
		a.cancelled_asv AS cancelled_asv_actual,
		t.cancelled_subscriptions_target,
		t.cancelled_asv_target
	FROM dates d
	LEFT JOIN actual_values_cancellation a
		ON d.reporting_date = a.reporting_date
		AND a.subcategory = d.subcategory
		AND d.country = a.country_name
		AND d.store = a.store_commercial
		AND d.customer_type = a.customer_type
	LEFT JOIN  forecast_cancellation f
		ON f.reporting_date = d.reporting_date
		AND f.subcategory = d.subcategory
		AND d.country = f.country
		AND d.store = f.store
		AND d.customer_type = f.customer_type
		AND f.row_num = 1
	LEFT JOIN targets t 
		ON t.reporting_date = d.reporting_date
		AND t.subcategory = d.subcategory
		AND d.country = t.country
		AND d.store = t.store
		AND d.customer_type = t.customer_type
	LEFT JOIN forecast_cancellation_non_official_targets ft 
		ON ft.reporting_date = d.reporting_date
		AND ft.subcategory = d.subcategory
		AND d.country = ft.country
		AND d.store = ft.store
		AND d.customer_type = ft.customer_type
		AND ft.row_num = 1
	LEFT JOIN (SELECT DISTINCT subcategory_name, category_name FROM ods_production.product) pp
		ON d.subcategory = pp.subcategory_name
	WHERE date_trunc('year',d.reporting_date) >= '2022-01-01'
)
SELECT 
	reporting_date,
	country,
	store,
	subcategory,
	category_name,
	'Cancelled Subscriptions Forecast'  AS cancelled_subscriptions_measure,
	cancelled_subscriptions_forecast AS cancelled_subscriptions_value,
	'Cancelled Asv Forecast' AS cancelled_asv_measure,
	cancelled_asv_forecast AS cancelled_asv_value
FROM total_before_pivoting
WHERE cancelled_subscriptions_forecast IS NOT NULL OR cancelled_asv_forecast IS NOT NULL 
UNION ALL 
SELECT 
	reporting_date,
	country,
	store,
	subcategory,
	category_name,
	'Cancelled Subscriptions Actual'  AS cancelled_subscriptions_measure,
	cancelled_subscriptions_actual AS cancelled_subscriptions_value,
	'Cancelled Asv Actual' AS cancelled_asv_measure,
	cancelled_asv_actual AS cancelled_asv_value
FROM total_before_pivoting
WHERE cancelled_subscriptions_actual IS NOT NULL OR cancelled_asv_actual IS NOT NULL  
UNION ALL 
SELECT 
	reporting_date,
	country,
	store,
	subcategory,
	category_name,
	'Cancelled Subscriptions Target'  AS cancelled_subscriptions_measure,
	cancelled_subscriptions_target AS cancelled_subscriptions_value,
	'Cancelled Asv Target' AS cancelled_asv_measure,
	cancelled_asv_target AS cancelled_asv_value
FROM total_before_pivoting
WHERE cancelled_subscriptions_target IS NOT NULL OR cancelled_asv_target IS NOT NULL   
UNION ALL 
SELECT 
	reporting_date,
	country,
	store,
	subcategory,
	category_name,
	'Cancelled Subscriptions Forecast with Non Official Targets'  AS cancelled_subscriptions_measure,
	cancelled_subscriptions_forecast_not_official AS cancelled_subscriptions_value,
	'Cancelled Asv Forecast with Non Official Targets' AS cancelled_asv_measure,
	cancelled_asv_forecast_not_official AS cancelled_asv_value
FROM total_before_pivoting
WHERE cancelled_subscriptions_forecast_not_official IS NOT NULL OR cancelled_asv_forecast_not_official IS NOT NULL
;


BEGIN TRANSACTION;

DROP TABLE IF EXISTS dm_commercial.cancellation_prediction_results;
CREATE TABLE dm_commercial.cancellation_prediction_results AS
SELECT *
FROM tmp_cancellation_prediction_results;

END TRANSACTION;

GRANT SELECT ON dm_commercial.cancellation_prediction_results TO tableau;
