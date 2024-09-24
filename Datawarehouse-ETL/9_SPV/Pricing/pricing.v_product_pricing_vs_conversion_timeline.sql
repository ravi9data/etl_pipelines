CREATE OR REPLACE VIEW pricing.v_product_pricing_vs_conversion_timeline AS
WITH  dates AS (
	SELECT DISTINCT
		datum AS fact_date
	FROM public.dim_dates
	WHERE (datum BETWEEN DATEADD(MONTH, -3, current_date) AND current_date -1)
)
, prices_before AS (
	SELECT
		CASE store_parent 
				WHEN 'de' THEN 'Grover - Germany online'
				WHEN 'es' THEN 'Grover - Spain online'
				WHEN 'nl' THEN 'Grover - Netherlands online'
				WHEN 'us' THEN 'Grover - United States online'
				WHEN 'at' THEN 'Grover - Austria online'
		END AS store_label,
		product_sku,
		start_date::date,
		m1_price::float,
		lag(m1_price) over (partition by store_label,product_sku order by start_date) AS m1_price_before,
		m3_price::float,
		lag(m3_price) over (partition by store_label,product_sku order by start_date) AS m3_price_before,
		m6_price::float,
		lag(m6_price) over (partition by store_label,product_sku order by start_date) AS m6_price_before,
		m12_price::float,
		lag(m12_price) over (partition by store_label,product_sku order by start_date) AS m12_price_before,
		m18_price::float,
		lag(m18_price) over (partition by store_label,product_sku order by start_date) AS m18_price_before,
		m24_price::float,
		lag(m24_price) over (partition by store_label,product_sku order by start_date) AS m24_price_before,
		is_discount,
		ROW_NUMBER() OVER (PARTITION BY store_label, product_sku ORDER BY start_date) AS row_num
	FROM pricing.prod_pricing_historical
	WHERE store_label IS NOT null
	AND start_date::date <= DATEADD(DAY, -1, current_date)
) 
, pricing AS (
	SELECT
		store_label,
		product_sku,
		start_date,
		COALESCE(DATEADD('sec',-1,(lag(start_date) over (partition by store_label,product_sku order by start_date DESC))),'2099-12-01') AS end_date_new,
		m1_price::float,
		m3_price::float,
		m6_price::float,
		m12_price::float,
		m18_price::float,
		m24_price::float,
		is_discount
	FROM prices_before
	WHERE m1_price <> m1_price_before 
		OR m3_price <> m3_price_before 
		OR m6_price <> m6_price_before 
		OR m12_price <> m12_price_before 
		OR m18_price <> m18_price_before 
		OR m24_price <> m24_price_before
		OR row_num = 1
)
, acquired_metrics AS (
	  SELECT
		  o.store_label,
		  oi.product_sku,
		  o.created_date::date AS fact_date,
		  COALESCE(COUNT(DISTINCT oi.order_item_id),0) AS acquired_subscriptions
	FROM master.ORDER o
	INNER JOIN ods_production.order_item oi
		ON o.order_id = oi.order_id
	WHERE o.store_label LIKE 'Grover - %'
	  AND fact_date::date >= DATEADD(MONTH, -3, current_date::date) 
	GROUP BY 1,2,3
)
, pdp AS (
	SELECT 
		page_view_start::date AS fact_date,
		CASE 
	  		WHEN geo_country = 'DE' OR store_name = 'Germany' THEN 'Grover - Germany online'
	  		WHEN geo_country = 'ES' OR store_name =  'Spain' THEN 'Grover - Spain online'
	  		WHEN geo_country = 'AT' OR store_name = 'Austria' THEN 'Grover - Austria online'
	  		WHEN geo_country = 'NL' OR store_name = 'Netherlands' THEN 'Grover - Netherlands online'
	  		WHEN geo_country = 'US' OR store_name = 'United States' THEN 'Grover - United States online' END
		      	AS store_label,
		page_type_detail AS product_sku,
		COUNT(DISTINCT page_view_id) AS product_page_views
	FROM traffic.page_views
	WHERE page_type = 'pdp' 
	  AND page_view_date::date >= DATEADD(MONTH, -3, current_date::date) 
	GROUP BY 1,2,3
)
, products_with_non_null_traffic_pdp AS (
	SELECT DISTINCT
		store_label,
		product_sku
	FROM pdp 
)
, products_with_non_null_traffic_subs AS (
	SELECT DISTINCT
		store_label,
		product_sku
	FROM acquired_metrics 
)
SELECT 
	d.fact_date::date,
	p.store_label,
	p.product_sku,
	pp.product_name ,
	pp.category_name ,
	pp.subcategory_name ,
	pp.brand ,
	MIN(p.m1_price::float) AS m1_price,
	MIN(p.m3_price::float) AS m3_price,
	MIN(p.m6_price::float) AS m6_price,
	MIN(p.m12_price::float) AS m12_price,
	MIN(p.m18_price::float) AS m18_price,
	MIN(p.m24_price::float) AS m24_price,
	MAX(p.is_discount) AS is_discount,
	COALESCE(SUM(pdp.product_page_views),0) AS product_page_views,
	COALESCE(SUM(a.acquired_subscriptions),0) AS acquired_subscriptions
FROM dates d
LEFT JOIN pricing p
	ON d.fact_date::date > p.start_date::date
		AND d.fact_date::date <= p.end_date_new::date
LEFT JOIN pdp 
	ON d.fact_date::date = pdp.fact_date::date
	AND p.store_label = pdp.store_label
	AND p.product_sku = pdp.product_sku
LEFT JOIN acquired_metrics a 
	ON d.fact_date::date = a.fact_date::date
	AND p.store_label = a.store_label
	AND p.product_sku = a.product_sku
LEFT JOIN ods_production.product pp 
	ON p.product_sku = pp.product_sku	
INNER JOIN products_with_non_null_traffic_pdp t 
	ON p.store_label = t.store_label
	AND p.product_sku = t.product_sku
INNER JOIN products_with_non_null_traffic_subs s
	ON p.store_label = s.store_label
	AND p.product_sku = s.product_sku
GROUP BY 1,2,3,4,5,6,7
WITH NO SCHEMA BINDING;