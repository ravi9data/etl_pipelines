
CREATE OR REPLACE VIEW dm_catman.brand_performance AS 
WITH fact_days AS (
	SELECT
		DISTINCT datum AS fact_date
	FROM public.dim_dates
	WHERE datum BETWEEN DATEADD('week', -8, current_date) AND current_date
)	
,
asv AS (
	SELECT
		f.fact_date,
		s.category_name,
		s.subcategory_name,
		p.brand,
		CASE
			WHEN s.store_commercial LIKE ('%B2B%') THEN 'B2B'
			WHEN s.store_commercial LIKE ('%Partnerships%') THEN 'Retail'
			ELSE 'B2C'
		END AS channel_type,
		s.country_name,
		COALESCE(sum(s.subscription_value_lc), 0) AS active_subscription_value
	FROM fact_days f
	LEFT JOIN ods_production.subscription_phase_mapping s
	   ON f.fact_date::date >= s.fact_day::date
		AND f.fact_date::date <= COALESCE(s.end_date::date, f.fact_date::date + 1)
	LEFT JOIN ods_production.product p 	
		ON p.product_sku = s.product_sku
	WHERE EXISTS (SELECT NULL FROM fact_days d WHERE d.fact_date = f.fact_date)	
	GROUP BY 1,2,3,4,5,6
)
,
subs_a AS (
	SELECT
		s.start_date::date AS fact_date,
		s.category_name,
		s.subcategory_name,
		s.brand,
		CASE
			WHEN s.store_commercial LIKE ('%B2B%') THEN 'B2B'
			WHEN s.store_commercial LIKE ('%Partnerships%') THEN 'Retail'
			ELSE 'B2C'
		END AS channel_type,
		s.country_name,
		COALESCE(sum(subscription_value),0) AS acquired_subscription_value,
		COALESCE(count(subscription_id),0) AS acquired_subscriptions
	FROM ods_production.subscription s
	WHERE s.start_date IS NOT NULL
		AND EXISTS (SELECT NULL FROM fact_days d WHERE d.fact_date = fact_date)
	GROUP BY 1,2,3,4,5,6
) 
,
subs_c AS (
	SELECT
		s.cancellation_date::date AS fact_date,
		s.category_name,
		s.subcategory_name,
		s.brand,
		CASE
			WHEN s.store_commercial LIKE ('%B2B%') THEN 'B2B'
			WHEN s.store_commercial LIKE ('%Partnerships%') THEN 'Retail'
			ELSE 'B2C'
		END AS channel_type,
		s.country_name,
		COALESCE(sum(subscription_value),0) AS cancelled_subscription_value
	FROM ods_production.subscription s
	WHERE s.cancellation_date IS NOT NULL
		AND EXISTS (SELECT NULL FROM fact_days d WHERE d.fact_date = fact_date)
	GROUP BY 1,2,3,4,5,6
)
SELECT 
	a.fact_date,
	a.category_name,
	a.subcategory_name,
	a.brand,
	a.channel_type,
	a.country_name,
	a.active_subscription_value,
	sa.acquired_subscriptions,
	sa.acquired_subscription_value,
	sc.cancelled_subscription_value
FROM asv a 
LEFT JOIN subs_a sa 
	ON sa.fact_date = a.fact_date
	 AND sa.category_name = a.category_name
	 AND sa.subcategory_name = a.subcategory_name
	 AND sa.brand = a.brand
	 AND sa.channel_type = a.channel_type
	 AND sa.country_name = a.country_name
LEFT JOIN subs_c sc 
	ON sc.fact_date = a.fact_date
	 AND sc.category_name = a.category_name
	 AND sc.subcategory_name = a.subcategory_name
	 AND sc.brand = a.brand
	 AND sc.channel_type = a.channel_type
	 AND sc.country_name = a.country_name	 
WITH NO SCHEMA BINDING;