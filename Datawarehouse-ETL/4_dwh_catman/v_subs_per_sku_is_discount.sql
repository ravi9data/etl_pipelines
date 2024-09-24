CREATE OR REPLACE VIEW dm_commercial.v_subs_per_sku_is_discount AS
WITH dates AS (
	SELECT DISTINCT
		datum AS fact_date
	FROM public.dim_dates
	WHERE datum >= DATE_TRUNC('year',DATEADD('month',-12,current_date))
	AND datum <= current_date
)
, products_discount AS (
	SELECT
		d.fact_date::date,
		CASE 
			WHEN p.store_parent IN ('business_de','de') THEN 'Germany'
			WHEN p.store_parent IN ('business_at','at') THEN 'Austria'
			WHEN p.store_parent IN ('business_es','es') THEN 'Spain'
			WHEN p.store_parent IN ('business_nl','nl') THEN 'Netherlands'
		END AS country_name,
		CASE 
			WHEN p.store_parent LIKE '%business%' THEN 'business_customer'
			ELSE 'normal_customer'
		END AS customer_type,
		p.product_sku,
		min(p.m12_price) AS m12_price,
		min(p.m12_high_price) AS m12_high_price,
		max(p.is_discount) AS is_discount
	FROM dates d
	LEFT JOIN pricing.prod_pricing_historical p
		ON d.fact_date::date >= p.start_date::date
		AND d.fact_date::date <= p.end_date::date 
	WHERE country_name IS NOT NULL
		AND p.end_date >= DATE_TRUNC('year',DATEADD('month', -13, current_date))
	GROUP BY 1,2,3,4
)
SELECT 
	date_trunc('week',p.fact_date)::date AS fact_date,
	p.country_name,
	p.customer_type, 
	p.product_sku, 
	pr.product_name,
	pr.category_name,
	pr.subcategory_name,
	pr.brand,
	COALESCE(max(m12_price),0) AS max_m12_price,
	COALESCE(min(m12_price),0) AS min_m12_price,
	COALESCE(max(m12_high_price),0) AS m12_high_price,
	COALESCE(avg(m12_price),0) AS avg_m12_price,
	COALESCE(avg(m12_high_price),0) AS avg_m12_high_price,
	COALESCE(max(p.is_discount),0) AS is_discount,
	count(DISTINCT s.subscription_id) AS total_subscriptions,
	count(DISTINCT CASE WHEN is_discount = 1 THEN p.fact_date END) AS days_of_discount
FROM products_discount p
LEFT JOIN ods_production.product pr
	ON pr.product_sku = p.product_sku 
LEFT JOIN master.subscription s
	ON s.order_created_date::date = p.fact_date::date
	AND s.product_sku = p.product_sku
	AND s.country_name = p.country_name
	AND s.customer_type = p.customer_type	
WHERE pr.product_name NOT LIKE '%(Inactive)%'
GROUP BY 1,2,3,4,5,6,7,8
WITH NO SCHEMA BINDING;
