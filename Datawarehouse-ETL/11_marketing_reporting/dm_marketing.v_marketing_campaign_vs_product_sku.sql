CREATE OR REPLACE VIEW dm_marketing.v_marketing_campaign_vs_product_sku AS 
-- Orders per day
with orders as (
	select
		o.order_id,
		oi.product_sku,
		o.store_country AS country,
		o.customer_type,
		o.submitted_date::date AS reporting_date,
		m.marketing_channel,
		m.marketing_content,
		m.marketing_source,
		o.marketing_campaign,
		o.paid_orders AS is_order_paid,
		o.completed_orders as is_order_submitted,
		oi.quantity 
	FROM master.ORDER o 
	LEFT JOIN ods_production.order_item oi 
		ON o.order_id = oi.order_id
	LEFT JOIN ods_production.order_marketing_channel m 
		ON m.order_id = o.order_id
	WHERE o.submitted_date::date >= DATE_TRUNC('week',DATEADD('week',-6,current_date))
)
,-- Price history
price_history as (
	select
		product_sku,
		CASE WHEN store_parent ILIKE 'business%' THEN 'business_customer' ELSE 'normal_customer' END AS customer_type,
		CASE WHEN store_parent ILIKE '%at' THEN 'Austria'
			 WHEN store_parent ILIKE '%nl' THEN 'Netherlands'
			 WHEN store_parent ILIKE '%us' THEN 'United States'
			 WHEN store_parent ILIKE '%de' THEN 'Germany'
			 WHEN store_parent ILIKE '%es' THEN 'Spain'
			 ELSE 'Germany'
			 END AS country,
		snapshot_date :: date as reporting_date,
		CASE WHEN rental_plan_price_12_months like '%,%' then split_part(rental_plan_price_12_months, ',', 1) :: float
			else rental_plan_price_12_months:: float end as active_price_12m,
		CASE when rental_plan_price_12_months like '%,%' then split_part(rental_plan_price_12_months, ',', 2) :: float
			else rental_plan_price_12_months:: float END AS high_price_12m
	WHERE snapshot_date::date >= DATE_TRUNC('week',DATEADD('week',-6,current_date))
)
,-- Stock and utilisation per day
stockdata as (
	select
		product_sku,
		reporting_date::date,
		CASE WHEN warehouse = 'office_us' THEN 'United States' ELSE 'EU' END AS continent,
		sum(number_of_assets_in_stock) AS number_of_assets_in_stock
	FROM dwh.portfolio_overview
	WHERE reporting_date::date >= DATE_TRUNC('week',DATEADD('week',-6,current_date))
	GROUP BY 1,2,3
)
, mkt_prices as (
	select
		product_sku,
		snapshot_date AS reporting_date,
		final_price,
		'EU' AS continent
	FROM pricing.prisync_de_snapshots
	WHERE reporting_date >= DATE_TRUNC('week',DATEADD('week',-6,current_date))
	union
	select
		product_sku,
		snapshot_date AS reporting_date,
		final_price,
		'United States' AS continent
	FROM pricing.prisync_us_snapshots
	WHERE reporting_date >= DATE_TRUNC('week',DATEADD('week',-6,current_date))
),
dimensions_combined AS (
SELECT DISTINCT 
	dd.datum AS reporting_date,
	m.country,
	m.marketing_channel,
	m.marketing_source,
	m.marketing_content,
	m.marketing_campaign,
	m.customer_type,
	m.product_sku
FROM public.dim_dates dd 
CROSS JOIN orders m
WHERE dd.datum >= DATE_TRUNC('week',DATEADD('week',-6,current_date))
	AND dd.datum < current_date
)
select
	dc.reporting_date,
	datepart(week, dc.reporting_date) AS Week_number,
	date_trunc('month',dc.reporting_date)::date AS start_of_month,
	m.order_id,
	dc.marketing_channel,
	dc.marketing_source,
	dc.marketing_campaign,
	dc.marketing_content,
	dc.customer_type,
	dc.country,
	d.category_name,
	d.subcategory_name,
	d.brand,
	dc.product_sku,
	d.product_name,
	p.active_price_12m,
	p.high_price_12m,
	(p.active_price_12m - p.high_price_12m) / nullif(p.high_price_12m,0) AS discount,
	mkt.final_price AS mkt_price,
	p.active_price_12m / nullif(mkt.final_price,0) AS rrp_perc,
	coalesce(m.is_order_submitted, 0) AS is_order_submitted,
	coalesce(m.is_order_paid, 0) AS is_order_paid,
	coalesce(m.quantity,0) AS quantity_products,
	CASE WHEN coalesce(s.number_of_assets_in_stock,0) > 0 THEN 1 ELSE 0 END AS is_there_stock_on_hand
FROM dimensions_combined dc
LEFT JOIN price_history p
	ON dc.reporting_date = p.reporting_date
	AND dc.product_sku = p.product_sku
	AND dc.country = p.country
	AND dc.customer_type = p.customer_type
LEFT JOIN orders m 
	ON dc.product_sku = m.product_sku
	AND dc.reporting_date = m.reporting_date
	AND dc.country = m.country
	AND dc.customer_type = m.customer_type
	AND dc.marketing_channel = m.marketing_channel
	AND dc.marketing_source = m.marketing_source
	AND dc.marketing_campaign = m.marketing_campaign
	AND dc.marketing_content = m.marketing_content
LEFT JOIN stockdata s 
	ON dc.product_sku = s.product_sku
	AND dc.reporting_date = s.reporting_date
	AND CASE WHEN dc.country = 'United States' THEN 'United States'
			ELSE 'EU' END = s.continent
LEFT JOIN mkt_prices mkt 
	ON mkt.product_sku = dc.product_sku
	AND mkt.reporting_date = dc.reporting_date
	AND CASE WHEN dc.country = 'United States' THEN 'United States'
			ELSE 'EU' END = mkt.continent
LEFT JOIN ods_production.product d 
	ON dc.product_sku = d.product_sku
WITH NO SCHEMA BINDING;
