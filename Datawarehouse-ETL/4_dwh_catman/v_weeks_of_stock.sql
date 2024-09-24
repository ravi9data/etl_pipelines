CREATE OR REPLACE VIEW dm_catman.v_weeks_of_stock AS
WITH fact_days AS (
	SELECT
		DISTINCT datum::date AS fact_date
	FROM public.dim_dates
	WHERE datum::date >= dateadd('week',-6,date_trunc('week',current_date))
)
, subscriptions AS (
	SELECT 
		start_date::date AS fact_date,
		product_sku,
		count(DISTINCT subscription_id) AS acquired_subscriptions,
		sum(subscription_value_euro) AS acquired_subscription_value
	FROM master.subscription s 
	GROUP BY 1,2
)
, assets AS (
	SELECT 
		a.product_sku, 
		count (DISTINCT CASE
							WHEN date = dateadd('week',-1,date_trunc('week',current_date)) AND asset_status_original = 'IN STOCK'
								THEN asset_id
						END
			  )	AS stock_beginning_of_last_week,
		count (DISTINCT CASE
							WHEN date = dateadd('week',-2,date_trunc('week',current_date)) AND asset_status_original = 'IN STOCK'
								THEN asset_id
						END
			  )	AS stock_beginning_of_last_2_weeks,
		count (DISTINCT CASE
							WHEN date = dateadd('week',-3,date_trunc('week',current_date)) AND asset_status_original = 'IN STOCK'
								THEN asset_id
						END
			  )	AS stock_beginning_of_last_3_weeks ,
		count (DISTINCT CASE
							WHEN date = dateadd('week',-4,date_trunc('week',current_date)) AND asset_status_original = 'IN STOCK'
								THEN asset_id
						END
			  )	AS stock_beginning_of_last_4_weeks 
	FROM master.asset_historical a
	WHERE date >= dateadd('week',-6,date_trunc('week',current_date))
	GROUP BY 1
)
SELECT 
	d.fact_date,
	p.category_name,
	p.subcategory_name, 
	s.product_sku,
	p.product_name, 
	s.acquired_subscriptions,
	s.acquired_subscription_value,
	stock_beginning_of_last_week,
	stock_beginning_of_last_2_weeks,
	stock_beginning_of_last_3_weeks,
	stock_beginning_of_last_4_weeks
FROM fact_days d 
LEFT JOIN subscriptions s 
	ON s.fact_date = d.fact_date
LEFT JOIN assets a 
	ON a.product_sku = s.product_sku
LEFT JOIN ods_production.product p 
	ON p.product_sku = a.product_sku
WITH NO SCHEMA BINDING	
;