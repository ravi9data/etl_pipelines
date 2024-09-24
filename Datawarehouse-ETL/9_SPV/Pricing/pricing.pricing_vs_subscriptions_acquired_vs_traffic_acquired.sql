DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_pricing;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_pricing
SORTKEY(
	product_sku,
	store_label,
	start_date
)
DISTKEY(
	product_sku
)
AS 
WITH prices_before AS (
	SELECT
		CASE store_parent 
				WHEN 'de' THEN 'Grover - Germany online'
				WHEN 'es' THEN 'Grover - Spain online'
				WHEN 'nl' THEN 'Grover - Netherlands online'
				WHEN 'us' THEN 'Grover - United States online'
				WHEN 'at' THEN 'Grover - Austria online'
		END AS store_label,
		product_sku,
		start_date,
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
		end_date,
		ROW_NUMBER() OVER (PARTITION BY store_label, product_sku ORDER BY start_date) AS row_num
	FROM pricing.prod_pricing_historical
	WHERE store_label IS NOT null
	AND start_date::date <= DATEADD(DAY, -1, current_date)::date
) 
, cleaning_price AS (
	SELECT
		store_label,
		product_sku,
		start_date,
		m1_price::float,
		COALESCE(DATEADD('sec',-1,(lag(start_date) over (partition by store_label,product_sku order by start_date DESC))),current_date::timestamp) AS end_date_new,
		m3_price::float,
		m6_price::float,
		m12_price::float,
		m18_price::float,
		m24_price::float,
		is_discount,
		end_date,
		DATEDIFF('hour',start_date::timestamp, end_date_new::timestamp ) AS hours_price_on,
		COALESCE(lag(m1_price) over (partition by store_label,product_sku order by start_date),m1_price) AS m1_price_before,
		COALESCE(lag(m3_price) over (partition by store_label,product_sku order by start_date),m3_price) AS m3_price_before,
		COALESCE(lag(m6_price) over (partition by store_label,product_sku order by start_date),m6_price) AS m6_price_before,
		COALESCE(lag(m12_price) over (partition by store_label,product_sku order by start_date),m12_price) AS m12_price_before,
		COALESCE(lag(m18_price) over (partition by store_label,product_sku order by start_date),m18_price) AS m18_price_before,
		COALESCE(lag(m24_price) over (partition by store_label,product_sku order by start_date),m24_price) AS m24_price_before,
		ROW_NUMBER() OVER (PARTITION BY store_label, product_sku ORDER BY start_date) AS row_num
	FROM prices_before
	WHERE m1_price <> m1_price_before 
		OR m3_price <> m3_price_before 
		OR m6_price <> m6_price_before 
		OR m12_price <> m12_price_before 
		OR m18_price <> m18_price_before 
		OR m24_price <> m24_price_before
		OR row_num = 1
)
SELECT 
	store_label,
	product_sku,
	start_date,
	end_date_new,
	m1_price::float,
	m1_price_before::float,
	CASE WHEN (m1_price_before <> m1_price OR row_num = 1) AND m1_price IS NOT NULL THEN 1 ELSE 0 END AS is_m1_price_changed,
	m3_price::float,
	m3_price_before::float,
	CASE WHEN (m3_price_before <> m3_price OR row_num = 1) AND m3_price IS NOT NULL THEN 1 ELSE 0 END AS is_m3_price_changed,
	m6_price::float,
	m6_price_before::float,
	CASE WHEN (m6_price_before <> m6_price OR row_num = 1) AND m6_price IS NOT NULL THEN 1 ELSE 0 END AS is_m6_price_changed,
	m12_price::float,
	m12_price_before::float,
	CASE WHEN (m12_price_before <> m12_price OR row_num = 1) AND m12_price IS NOT NULL THEN 1 ELSE 0 END AS is_m12_price_changed,
	m18_price::float,
	m18_price_before::float,
	CASE WHEN (m18_price_before <> m18_price OR row_num = 1) AND m18_price IS NOT NULL THEN 1 ELSE 0 END AS is_m18_price_changed,
	m24_price::float,
	m24_price_before::float,
	CASE WHEN (m24_price_before <> m24_price OR row_num = 1) AND m24_price IS NOT NULL THEN 1 ELSE 0 END AS is_m24_price_changed,
	is_discount,
	end_date,
	hours_price_on
FROM cleaning_price
;


DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs
SORTKEY(
	store_label,
	product_sku,
	fact_date
)
DISTKEY(
	product_sku
)
AS
	  SELECT
		  o.store_label,
		  oi.product_sku,
		  o.created_date AS fact_date,
		  COALESCE(COUNT(DISTINCT oi.order_item_id),0) AS acquired_subscriptions
	FROM master.ORDER o
	INNER JOIN ods_production.order_item oi
		ON o.order_id = oi.order_id
	WHERE o.store_label LIKE 'Grover - %'
	  AND fact_date >= '2022-02-01'
	GROUP BY 1,2,3
;	

DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_pdp;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_pdp
SORTKEY(
	store_label,
	product_sku,
	fact_date
)
DISTKEY(
	product_sku
)
AS
	SELECT 
		page_view_start AS fact_date,
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
	AND page_view_date >= '2022-02-01' 
	GROUP BY 1,2,3
;

DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_1st_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_1st_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	CASE WHEN p.hours_price_on > 252 THEN SUM(am_b14.acquired_subscriptions)::decimal/14 -- MORE 10,5 than 
		 END AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs am_b14
  ON p.store_label = am_b14.store_label
  AND p.product_sku = am_b14.product_sku
  AND TO_CHAR(am_b14.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(DATEADD(DAY, -14, p.start_date), 'YYYY-MM-DD HH24:MI:SS') 
  AND TO_CHAR(am_b14.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
;

DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_2nd_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_2nd_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	CASE WHEN p.hours_price_on > 252 THEN SUM(am_a14.acquired_subscriptions)::decimal/14 -- MORE 10,5 than 
		 END AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p
  LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs am_a14
  ON p.store_label = am_a14.store_label
  AND p.product_sku = am_a14.product_sku
  AND TO_CHAR(am_a14.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(DATEADD(DAY, 14, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(am_a14.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
;


DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_3rd_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_3rd_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	CASE WHEN p.hours_price_on > 252 THEN SUM(pdp_b14.product_page_views)::decimal/14 -- MORE 10,5 than 
		 END AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p
  LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_pdp pdp_b14
  ON p.store_label = pdp_b14.store_label
  AND p.product_sku = pdp_b14.product_sku
  AND TO_CHAR(pdp_b14.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(DATEADD(DAY, -14, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(pdp_b14.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 
 
 DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_4th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_4th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	CASE WHEN p.hours_price_on > 252 THEN SUM(pdp_a14.product_page_views)::decimal/14 -- MORE 10,5 than 
		 END AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p  
  LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_pdp pdp_a14
  ON p.store_label = pdp_a14.store_label
  AND p.product_sku = pdp_a14.product_sku
  AND TO_CHAR(pdp_a14.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(DATEADD(DAY, 14, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(pdp_a14.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 
  DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_5th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_5th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	CASE WHEN p.hours_price_on <= 252 THEN SUM(am_b7.acquired_subscriptions)::decimal/7-- 5 TO 10,5 days
		 END AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
 LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs am_b7
  ON p.store_label = am_b7.store_label
  AND p.product_sku = am_b7.product_sku
  AND TO_CHAR(am_b7.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(DATEADD(DAY, -7, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(am_b7.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 
 
   DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_6th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_6th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	CASE WHEN p.hours_price_on <= 252 THEN SUM(am_a7.acquired_subscriptions)::decimal/7-- 5 TO 10,5 days
		 END AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs am_a7
  ON p.store_label = am_a7.store_label
  AND p.product_sku = am_a7.product_sku
  AND TO_CHAR(am_a7.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(DATEADD(DAY, 7, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(am_a7.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 
 
DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_7th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_7th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	CASE WHEN p.hours_price_on <= 252 THEN SUM(pdp_b7.product_page_views)::decimal/7-- 5 TO 10,5 days
		 END AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_pdp pdp_b7
  ON p.store_label = pdp_b7.store_label
  AND p.product_sku = pdp_b7.product_sku
  AND TO_CHAR(pdp_b7.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(DATEADD(DAY, -7, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(pdp_b7.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 
DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_8th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_8th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	CASE WHEN p.hours_price_on <= 252 THEN SUM(pdp_a7.product_page_views)::decimal/7-- 5 TO 10,5 days
		 END AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_pdp pdp_a7
  ON p.store_label = pdp_a7.store_label
  AND p.product_sku = pdp_a7.product_sku
  AND TO_CHAR(pdp_a7.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(DATEADD(DAY, 7, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(pdp_a7.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 

DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_9th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_9th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	CASE WHEN p.hours_price_on <= 120 THEN SUM(am_b3.acquired_subscriptions)::decimal/3-- 1,5 TO 5 days 
		 END AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
 LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs am_b3
  ON p.store_label = am_b3.store_label
  AND p.product_sku = am_b3.product_sku
  AND TO_CHAR(am_b3.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(DATEADD(DAY, -3, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(am_b3.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;

DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_10th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_10th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	CASE WHEN p.hours_price_on <= 120 THEN SUM(am_a3.acquired_subscriptions)::decimal/3-- 1,5 TO 5 days
		 END AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs am_a3
  ON p.store_label = am_a3.store_label
  AND p.product_sku = am_a3.product_sku
  AND TO_CHAR(am_a3.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(DATEADD(DAY, 3, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(am_a3.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
  
DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_11th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_11th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	CASE WHEN p.hours_price_on <= 120 THEN SUM(pdp_b3.product_page_views)::decimal/3-- 1,5 TO 5 days
		 END AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_pdp pdp_b3
  ON p.store_label = pdp_b3.store_label
  AND p.product_sku = pdp_b3.product_sku
  AND TO_CHAR(pdp_b3.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(DATEADD(DAY, -3, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(pdp_b3.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ; 
 
 
DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_12th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_12th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	CASE WHEN p.hours_price_on <= 120 THEN SUM(pdp_a3.product_page_views)::decimal/3-- 1,5 TO 5 days
		 END AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_pdp pdp_a3
  ON p.store_label = pdp_a3.store_label
  AND p.product_sku = pdp_a3.product_sku
  AND TO_CHAR(pdp_a3.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(DATEADD(DAY, 3, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(pdp_a3.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ; 

DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_13th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_13th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	CASE WHEN p.hours_price_on <= 36 THEN SUM(am_b1.acquired_subscriptions) -- until 1,5 days
		 END AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs am_b1
  ON p.store_label = am_b1.store_label
  AND p.product_sku = am_b1.product_sku
  AND TO_CHAR(am_b1.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(DATEADD(DAY, -1, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(am_b1.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 

DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_14th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_14th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	CASE WHEN p.hours_price_on <= 36 THEN SUM(am_a1.acquired_subscriptions) -- until 1,5 days
		 END AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_acquired_subs am_a1
  ON p.store_label = am_a1.store_label
  AND p.product_sku = am_a1.product_sku
  AND TO_CHAR(am_a1.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(DATEADD(DAY, 1, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(am_a1.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 
DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_15th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_15th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	CASE WHEN p.hours_price_on <= 36 THEN SUM(pdp_b1.product_page_views) -- until 1,5 days
		 END AS avg_pdp_before,
	0 AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_pdp pdp_b1
  ON p.store_label = pdp_b1.store_label
  AND p.product_sku = pdp_b1.product_sku
  AND TO_CHAR(pdp_b1.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(DATEADD(DAY, -1, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(pdp_b1.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
  ;
 
DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_16th_left_join;
CREATE TEMP TABLE pricing_vs_subscriptions_acquired_vs_traffic_acquired_16th_left_join
SORTKEY(
	store_label,
	product_sku,
	start_date
)
DISTKEY(
	product_sku
)
AS
SELECT 
	p.store_label,
	p.product_sku,
	p.start_date,
	p.end_date_new,
	p.m1_price,
	p.m1_price_before,
	p.is_m1_price_changed,
	p.m3_price,
	p.m3_price_before,
	p.is_m3_price_changed,
	p.m6_price,
	p.m6_price_before,
	p.is_m6_price_changed,
	p.m12_price,
	p.m12_price_before,
	p.is_m12_price_changed,
	p.m18_price,
	p.m18_price_before,
	p.is_m18_price_changed,
	p.m24_price,
	p.m24_price_before,
	p.is_m24_price_changed,
	p.is_discount,
	0 AS avg_acquired_subs_before,
	0 AS avg_acquired_subs_after,
	0 AS avg_pdp_before,
	CASE WHEN p.hours_price_on <= 36 THEN SUM(pdp_a1.product_page_views) -- until 1,5 days
		 END AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_pricing p    
LEFT JOIN pricing_vs_subscriptions_acquired_vs_traffic_pdp pdp_a1
  ON p.store_label = pdp_a1.store_label
  AND p.product_sku = pdp_a1.product_sku
  AND TO_CHAR(pdp_a1.fact_date, 'YYYY-MM-DD HH24:MI:SS') <= TO_CHAR(DATEADD(DAY, 1, p.start_date), 'YYYY-MM-DD HH24:MI:SS')
  AND TO_CHAR(pdp_a1.fact_date, 'YYYY-MM-DD HH24:MI:SS') >= TO_CHAR(start_date, 'YYYY-MM-DD HH24:MI:SS')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,p.hours_price_on
;

  
DROP TABLE IF EXISTS pricing_vs_subscriptions_acquired_vs_traffic_acquired_union;
CREATE TEMP TABLE  pricing_vs_subscriptions_acquired_vs_traffic_acquired_union
AS
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_1st_left_join
UNION ALL 
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_2nd_left_join
UNION ALL 
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_3rd_left_join
UNION ALL 
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_4th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_5th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_6th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_7th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_8th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_9th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_10th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_11th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_12th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_13th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_14th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_15th_left_join
UNION ALL
SELECT * FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_16th_left_join
;

DROP TABLE IF EXISTS pricing.pricing_vs_subscriptions_acquired_vs_traffic_acquired;
CREATE TABLE  pricing.pricing_vs_subscriptions_acquired_vs_traffic_acquired
AS
SELECT
	u.store_label,
	u.product_sku,
	u.start_date,
	u.end_date_new AS end_date,
	u.m1_price,
	u.m1_price_before,
	u.is_m1_price_changed,
	u.m3_price,
	u.m3_price_before,
	u.is_m3_price_changed,
	u.m6_price,
	u.m6_price_before,
	u.is_m6_price_changed,
	u.m12_price,
	u.m12_price_before,
	u.is_m12_price_changed,
	u.m18_price,
	u.m18_price_before,
	u.is_m18_price_changed,
	u.m24_price,
	u.m24_price_before,
	u.is_m24_price_changed,
	u.is_discount,
	pp.product_name ,
	pp.category_name ,
	pp.subcategory_name ,
	pp.brand,
	COALESCE(MAX(u.avg_acquired_subs_before),0) AS avg_acquired_subs_before,
	COALESCE(MAX(u.avg_acquired_subs_after),0) AS avg_acquired_subs_after,
	COALESCE(MAX(u.avg_pdp_before),0) AS avg_pdp_before,
	COALESCE(MAX(u.avg_pdp_after),0) AS avg_pdp_after
FROM pricing_vs_subscriptions_acquired_vs_traffic_acquired_union u
LEFT JOIN ods_production.product pp 
	ON u.product_sku = pp.product_sku
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
;

GRANT SELECT ON pricing.pricing_vs_subscriptions_acquired_vs_traffic_acquired TO tableau;
