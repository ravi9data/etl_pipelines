CREATE OR REPLACE VIEW dm_marketing.v_conversion_funnel_daily_report_total_subs_per_sku_and_is_discount AS
WITH dates AS (
	SELECT DISTINCT
		datum AS fact_date
	FROM public.dim_dates
	WHERE DATE_TRUNC('week',datum) >= DATE_TRUNC('week',DATEADD('week',-7,current_date))
	AND datum <= current_date
)
, products_discount AS (
	SELECT
		d.fact_date::date,
		CASE p.store_parent 
			WHEN 'de' THEN 'Germany'
			WHEN 'es' THEN 'Spain'
			WHEN 'us' THEN 'United States'
			WHEN 'at' THEN 'Austria'
			WHEN 'nl' THEN 'Netherlands'
			END AS country_name,
		p.product_sku,
		MAX(p.is_discount) AS is_discount
	FROM dates d
	LEFT JOIN pricing.prod_pricing_historical p
		ON d.fact_date::date >= p.start_date::date
		AND d.fact_date::date <= p.end_date::date 
	WHERE country_name IS NOT NULL
	GROUP BY 1,2,3
)
SELECT 
	s.order_created_date::date AS fact_date,
	s.country_name,
	s.product_sku,
	s.product_name,
	s.category_name,
	s.subcategory_name,
	COALESCE(MAX(p.is_discount),0) AS is_discount,
	count(DISTINCT s.subscription_id) AS total_subscriptions
FROM master.subscription s 
LEFT JOIN products_discount p
	ON s.order_created_date::date = p.fact_date::date
	AND s.product_sku = p.product_sku
	AND s.country_name = p.country_name
WHERE DATE_TRUNC('week', s.order_created_date) >= DATE_TRUNC('week',DATEADD('week', -7, current_date))
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA BINDING;
