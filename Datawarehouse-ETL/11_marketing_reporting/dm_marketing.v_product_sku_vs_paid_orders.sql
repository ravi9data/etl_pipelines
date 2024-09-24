CREATE OR REPLACE VIEW dm_marketing.v_product_sku_vs_paid_orders AS
WITH paid_orders AS (
	SELECT DISTINCT 
		date_trunc('week',a.paid_date)::date AS paid_date_week,
		datepart('week', a.paid_date) AS week_number,
		a.store_country AS country,
		st.store_code,
		CASE WHEN a.store_country = 'United States' THEN 'US' ELSE 'EU' END AS continent,
		COALESCE(m.marketing_channel, 'n/a') AS marketing_channel_,
	    COALESCE(m.marketing_source, 'n/a') AS marketing_source_,
	    COALESCE(LOWER(
	    	CASE WHEN a.created_date::DATE <= '2023-03-22' 
	    				AND a.marketing_channel IN ('Paid Social Branding', 'Paid Social Performance')
				    THEN m.marketing_content
				 ELSE  m.marketing_campaign
				 END),'n/a') AS marketing_campaign_,
		lower(COALESCE(m.marketing_medium, 'n/a')) AS marketing_medium_,
	    lower(COALESCE(m.marketing_content,'n/a')) AS marketing_content_,
		b.category_name,
		b.subcategory_name,
		b.brand,
	    b.product_name,
	    b.product_sku,
	    count(DISTINCT a.order_id) AS paid_orders_,
	    sum(b.quantity) AS quantity_,
	    ROW_NUMBER() OVER (PARTITION BY country, marketing_channel_, marketing_source_, 
	    								marketing_campaign_, marketing_medium_, marketing_content_
	    	ORDER BY paid_orders_ DESC, quantity_ DESC, b.product_sku) AS product_rank
	FROM master.order a
	LEFT JOIN ods_production.order_item b 
		on a.order_id = b.order_id
	LEFT JOIN ods_production.order_marketing_channel m 
		on m.order_id = a.order_id
	LEFT JOIN ods_production.store st 
		ON st.id = a.store_id
	WHERE  a.paid_date::date >= DATE_ADD('week', -4, current_date)  
		AND a.paid_orders > 0
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
, stock AS (
	SELECT 
		'EU' AS continent,
		split_part(sku, 'V', 1) AS product_sku,
		sum(availablecount::float) AS products_in_stock
	FROM staging.store_1 
	GROUP BY 1,2
	UNION 
		SELECT 
		'US' AS continent,
		split_part(sku, 'V', 1) AS product_sku,
		sum(availablecount::float) AS products_in_stock
	FROM staging.store_621
	GROUP BY 1,2
)
, created_subs_last_week AS (
	SELECT
		product_sku,
		CASE WHEN country_name = 'United States' THEN 'US' ELSE 'EU' END AS continent,
		count(DISTINCT subscription_id) AS created_subs_last_week
	FROM master.subscription
	WHERE datediff('day', start_date, current_date) <= 7
	GROUP BY 1,2
)
, stock_rate AS (
	SELECT 
		s.continent,
		s.product_sku,
		CASE WHEN c.created_subs_last_week > 0 THEN s.products_in_stock::float / c.created_subs_last_week::float 
				ELSE s.products_in_stock::float END AS stock_vs_subs,
		s.products_in_stock,
		c.created_subs_last_week
	FROM stock s 
	LEFT JOIN created_subs_last_week c 
		ON s.continent = c.continent
		AND s.product_sku = c.product_sku
)
, last_friday AS (
	SELECT
		datum
	FROM public.dim_dates
	WHERE datum BETWEEN DATEADD(day, -7, CURRENT_DATE)
					AND DATEADD(day, -1, CURRENT_DATE)
		AND day_name = 'Friday'
)
, last_friday_12m_prices AS (
    SELECT
      product_sku,
      store_parent,
      m12_price
    FROM pricing.prod_pricing_historical ph
    INNER JOIN last_friday lf 
    	ON lf.datum:: date >= ph.start_date:: date
      	AND lf.datum:: date < ph.end_date:: date
)
, products_with_discount AS (
	SELECT 
		a.product_sku,
		a.store_parent,
		CASE WHEN a.m12_price < a.m12_high_price THEN 1 ELSE 0 END is_there_discount_12_months_plan,
		a.m12_price,
		a.m12_high_price,
		b.m12_price AS m12_price_last_friday,
		CASE WHEN b.m12_price IS NOT NULL THEN 100.0 * (a.m12_price - b.m12_price) / b.m12_price END 
			AS change_in_price_last_friday
	FROM pricing.prod_pricing_historical a
	LEFT JOIN last_friday_12m_prices b
		ON a.product_sku = b.product_sku
		AND a.store_parent = b.store_parent
	WHERE a.is_active
)
SELECT 
	a.paid_date_week,
	a.week_number,
	a.country,
	a.continent,
	a.marketing_channel_,
    a.marketing_source_,
    a.marketing_campaign_,
	a.marketing_medium_,
    a.marketing_content_,
	a.category_name,
	a.subcategory_name,
	a.brand,
    a.product_name,
    a.product_sku,
    a.paid_orders_,
    a.quantity_,
    a.product_rank,
    sr.stock_vs_subs,
	sr.products_in_stock,
	sr.created_subs_last_week,
	pwd.is_there_discount_12_months_plan,
	pwd.m12_price,
	pwd.m12_high_price,
	pwd.m12_price_last_friday,
	pwd.change_in_price_last_friday
FROM paid_orders a 
LEFT JOIN stock_rate sr
	ON a.continent = sr.continent
	AND a.product_sku = sr.product_sku
LEFT JOIN products_with_discount pwd
	ON pwd.product_sku = a.product_sku
	AND pwd.store_parent = a.store_code
WITH NO SCHEMA BINDING;