CREATE OR REPLACE VIEW dm_sustainability.v_seasonality AS
WITH bf_week AS (
	SELECT datum
	FROM public.dim_dates
	WHERE datum BETWEEN '2023-11-20' AND '2023-11-27' --2023
	   OR datum BETWEEN '2022-11-21' AND '2022-11-28' --2022
	   OR datum BETWEEN '2021-11-22' AND '2021-11-29' --2021
)
, nov_numbers AS (
	SELECT 
		CONCAT(date_part('year', start_date)::varchar(50), date_part('month', start_date)::varchar(50)) AS acquisition_month,
		category_name,
		subcategory_name,
		rental_period,
		country_name,
		--CEILING(DATEDIFF('day', start_date::date, COALESCE(cancellation_date::date, current_date)) / 31.0) AS subscription_duration,
		count(DISTINCT subscription_id) AS num_subs
	FROM master.subscription a
	INNER JOIN ( --take the number OF the FIRST Mon TO Mon week IN November TO later REPLACE the BF week WITH them
			SELECT 
				year_number,
				min(datum) first_monday
			FROM public.dim_dates
			WHERE year_number IN (2021,2022,2023)
			  AND month_number = 11
			  AND week_day_number = 1
			GROUP BY 1
	) b ON a.start_date::date BETWEEN b.first_monday AND DATEADD('day', 7, b.first_monday)
	GROUP BY 1,2,3,4,5
)
, raw_ AS (
	SELECT 
		CONCAT(date_part('year', start_date)::varchar(50), 
			CASE WHEN date_part('month', start_date) < 10 THEN CONCAT('0', date_part('month', start_date)::varchar(50)) ELSE date_part('month', start_date)::varchar(50) END
		) AS acquisition_month,
		category_name,
		subcategory_name,
				rental_period,
				country_name,
		--CEILING(DATEDIFF('day', start_date::date, COALESCE(cancellation_date::date, current_date)) / 31.0) AS subscription_duration,
		count(DISTINCT subscription_id) AS num_subs
	FROM master.subscription a
	WHERE start_date::date >= '2021-01-01'
	  AND NOT EXISTS (SELECT NULL FROM bf_week b --EXCLUDE BF week here
	                  WHERE a.start_date::date = b.datum)
	GROUP BY 1,2,3,4,5
)
, seasonality_with_bf AS (
	SELECT 
		CONCAT(date_part('year', start_date)::varchar(50), 
			CASE WHEN date_part('month', start_date) < 10 THEN CONCAT('0', date_part('month', start_date)::varchar(50)) ELSE date_part('month', start_date)::varchar(50) END
		) AS acquisition_month,
		category_name,
		subcategory_name,
				rental_period,
				country_name,
		--CEILING(DATEDIFF('day', start_date::date, COALESCE(cancellation_date::date, current_date)) / 31.0) AS subscription_duration,
		count(DISTINCT subscription_id) AS num_subs
	FROM master.subscription a
	WHERE start_date::date >= '2021-01-01'
	GROUP BY 1,2,3,4,5
)
SELECT 
	r.acquisition_month,
	r.category_name,
	r.subcategory_name,
	r.rental_period,
	r.country_name,
	--r.subscription_duration,
	CASE WHEN r.acquisition_month IN ('202111', '202211', '202311')
		THEN n.num_subs + r.num_subs --bf excluded FROM raw_ so we have TO ADD the november numbers
		ELSE r.num_subs
	END AS num_subs,
	s.num_subs AS num_subs_with_bf
FROM raw_ r
LEFT JOIN nov_numbers n
  ON r.acquisition_month = n.acquisition_month
  AND r.category_name = n.category_name
  AND r.subcategory_name = n.subcategory_name
  AND r.rental_period = n.rental_period
  AND r.country_name = n.country_name
LEFT JOIN seasonality_with_bf s 
  ON r.acquisition_month = s.acquisition_month
  AND r.category_name = s.category_name
  AND r.subcategory_name = s.subcategory_name
  AND r.rental_period = s.rental_period
  AND r.country_name = s.country_name
WHERE r.rental_period IN (1,3,6,12,18,24)
  AND LEFT(r.acquisition_month, 4)::INT < 2024
ORDER BY 2,3,4,1 DESC
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_sustainability.v_seasonality TO tableau;
