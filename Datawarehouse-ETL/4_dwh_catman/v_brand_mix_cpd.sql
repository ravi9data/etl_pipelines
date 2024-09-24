-- dm_catman.v_brand_mix_cpd source

CREATE OR REPLACE VIEW dm_catman.v_brand_mix_cpd AS 
WITH FACT_DAYS AS (
	SELECT
		DISTINCT DATUM::date AS fact_day
	FROM public.dim_dates
	WHERE DATUM <= CURRENT_DATE and DATUM::date >= dateadd('month',-13,date_trunc('month',current_date)) 
)	
, brand_vector AS (
SELECT 
	f.fact_day::date AS fact_date,
	a.brand AS brand,
	a.category_name AS category_name
FROM FACT_DAYS f
CROSS JOIN (SELECT DISTINCT brand,category_name FROM master.asset) a 
GROUP BY 1,2,3
)
, asset AS (
	SELECT 
		a.created_at::date AS fact_date,
		a.category_name,
		a.brand,
		count(DISTINCT asset_id) AS number_of_asset
	FROM master.asset a
	GROUP BY 1,2,3
)
, subs AS (
	SELECT 
		s.start_date::date AS fact_date,
		s.category_name,
		s.brand,
		count(DISTINCT s.subscription_id) AS acquired_subs,
		sum(s.subscription_value) AS acquired_subs_value
	FROM master.subscription s
	GROUP BY 1,2,3
)	
SELECT 
	bv.fact_date,
	bv.category_name,
	bv.brand,
	COALESCE(sum(number_of_asset),0) AS assets,
	COALESCE(sum(acquired_subs),0) AS acquired_subscriptions,
	round(COALESCE(sum(s.acquired_subs_value),0),2) AS acquired_subscription_value
FROM brand_vector bv 
LEFT JOIN asset a 
	ON a.fact_date = bv.fact_date
	 AND a.category_name = bv.category_name
	 AND a.brand = bv.brand
LEFT JOIN subs s 
	ON s.fact_date = bv.fact_date
	 AND s.category_name = bv.category_name
	 AND s.brand = bv.brand
GROUP BY 1,2,3
WITH NO SCHEMA binding
;