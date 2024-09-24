CREATE OR REPLACE VIEW dm_commercial.v_target_data_cancellation_rate_forecast AS 
WITH commercial_targets_daily_store_commercial_2022_2nd_part_table AS (
	SELECT  
		to_date AS datum,
		subcategories AS subcategory,
		CASE WHEN store_label = 'NL' THEN 'Netherlands'
			 WHEN store_label = 'US' THEN 'United States'
			 WHEN store_label = 'AT' THEN 'Austria'
			 WHEN store_label = 'ES' THEN 'Spain'
			 WHEN store_label = 'DE' THEN 'Germany'
			 WHEN store_label = 'Total' THEN 'No Info'
			 END AS country,
		channel_type AS customer_type,
		CASE WHEN measures = 'ASV' THEN amount END AS active_sub_value_daily_target ,
		CASE WHEN measures = 'Acquired ASV' THEN amount END AS acquired_subvalue_daily,
		CASE WHEN measures = 'Cancelled ASV' THEN amount END AS cancelled_subvalue_daily,
		CASE WHEN measures = 'Cancelled Subscriptions' THEN amount END AS cancelled_subscriptions,
		CASE WHEN measures = 'Acquired Subscriptions' THEN amount END AS acquired_subscriptions
	FROM dm_commercial.r_commercial_daily_targets_since_2022
	WHERE  date_trunc('month',to_date)>='2022-06-01'
)
, commercial_targets_daily_store_commercial_2022_2nd_part AS (
	SELECT  
		datum, 
		country,
		customer_type,
		subcategory,
		SUM(active_sub_value_daily_target) AS active_sub_value_daily_target,
		SUM(acquired_subvalue_daily) AS acquired_subvalue_daily,
		SUM(cancelled_subvalue_daily) AS cancelled_subvalue_daily,
		SUM(cancelled_subscriptions) AS cancelled_subscriptions,
 		SUM(acquired_subscriptions) AS acquired_subscriptions
	FROM commercial_targets_daily_store_commercial_2022_2nd_part_table t
	GROUP BY 1,2,3,4
)
, commercial_targets_daily_store_commercial_2022_1st_part AS (
	SELECT 
		datum,
		CASE WHEN country = 'NL' THEN 'Netherlands'
			 WHEN country = 'US' THEN 'United States'
			 WHEN country = 'AT' THEN 'Austria'
			 WHEN country = 'ES' THEN 'Spain'
			 WHEN country = 'DE' THEN 'Germany'
			 WHEN country = 'Total' THEN 'No Info'
			 END AS country,
		CASE WHEN store = 'B2C' THEN 'B2C'
			 WHEN store = 'Retail' THEN 'Retail'
			 WHEN store LIKE '%B2B%' THEN 'B2B'
			 END AS customer_type,	
		t.subcategory,
		SUM(asv) as active_sub_value_daily_target,
 		SUM(acquired_asv) as acquired_subvalue_daily,
 		SUM(cancelled_asv) aS cancelled_subvalue_daily,
 		SUM(cancelled_subscriptions) AS cancelled_subscriptions,
 		SUM(acquired_subscriptions) AS acquired_subscriptions
	FROM dw_targets.gs_commercial_targets_2022 t
	WHERE date_trunc('month',datum) BETWEEN '2022-01-01' AND '2022-05-31'
	GROUP BY 1,2,3,4
)
SELECT * FROM commercial_targets_daily_store_commercial_2022_2nd_part 
	UNION ALL
SELECT * FROM commercial_targets_daily_store_commercial_2022_1st_part
WITH NO SCHEMA BINDING;