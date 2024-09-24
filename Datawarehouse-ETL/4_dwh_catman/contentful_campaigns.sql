--Contentful campaigns 

SET enable_case_sensitive_identifier TO TRUE;

DROP TABLE IF EXISTS contentful_campaigns;
CREATE TEMP TABLE contentful_campaigns 
SORTKEY(
	product_sku,
	fact_date,
	country_name
)
DISTKEY(
	product_sku
)
AS
WITH cte AS (
	SELECT
		*,
		json_parse(fields) AS f
	FROM
		pricing.contentful_snapshots cs
)
, landing_pages AS (
	SELECT 
		f."entryTitle".en::text,
		first_published_at::timestamp,
		f."metaKeyWords".en::date AS campaign_end_date,
		replace(f."slug".en::varchar,'"','') AS campaign_slug,
		published_at::timestamp,
		published_counter::int,
		published_version::int,
		f."storeCode".en::text AS store_code,
		items.sys.id::text as link_entry_id,
		ROW_NUMBER() OVER (PARTITION BY link_entry_id ORDER BY published_at DESC) AS row_no
		--listagg(items.sys.id::text,' | ') AS linked_entries
	FROM cte t
	LEFT JOIN t.f."pageContent".en AS items ON TRUE
	WHERE t.content_type = 'landingPage'
		AND store_code IN ('de','at','es','nl','us')
	--GROUP BY 1,2,3,4,5,6,7
)
, product_widgets AS (
	SELECT 
		t.entry_id,
		f."title".en::text,
		first_published_at::timestamp,
		published_at::timestamp,
		published_counter::int,
		published_version::int,
		json_serialize(f."productIDs".en)::text AS product_ids
	FROM cte t
	LEFT JOIN t.f."pageContent".en AS items ON TRUE
	WHERE t.content_type = 'productListWidget'
)
, hand_picked_product_widget AS (
	SELECT 
		f."title".en::text,
		first_published_at::timestamp,
		published_at::timestamp,
		published_counter::int,
		published_version::int,
		json_serialize(f."productId".en)::text AS product_id,
		ROW_NUMBER() OVER (PARTITION BY en ORDER BY published_at DESC) AS row_no 
	FROM
		cte t
	LEFT JOIN t.f."pageContent".en AS items ON TRUE
	WHERE t.content_type = 'handPickedProductWidget' 
)
, contentful_landing_pages AS (
	SELECT DISTINCT
		lp.en AS campaign_title,
		lp.campaign_end_date,
		MAX(lp.campaign_end_date) OVER (PARTITION BY lp.en, lp.store_code) AS final_campaign_end_date,
		lp.campaign_slug,
		lp.store_code AS country_name,
		lp.link_entry_id
	FROM landing_pages lp
	WHERE row_no = 1
)
, contentful_product_widgets AS (
	SELECT
		pw.*, 
		MAX(published_at) OVER (PARTITION BY entry_id)::date AS max_publish_date,
		ROW_NUMBER() OVER (PARTITION BY entry_id ORDER BY published_at DESC) AS rowno  
	FROM product_widgets pw
)
,
contentful_handpicked_product_widgets AS (
	SELECT *
	FROM hand_picked_product_widget
	WHERE row_no = 1
)
, campaign_timeframe AS (
	SELECT 
		d.datum AS fact_date,
		lp.campaign_title,
		REPLACE(lp.campaign_slug,'-',' ') AS campaign_slug,
		CASE 
			WHEN lp.country_name = 'de' THEN 'Germany'
			WHEN lp.country_name = 'at' THEN 'Austria'
			WHEN lp.country_name = 'es' THEN 'Spain'
			WHEN lp.country_name = 'nl' THEN 'Netherlands'
			WHEN lp.country_name = 'us' THEN 'United States'
		END AS country_name,
		p.product_sku, 
		c.en AS banner_title,
		TRUE AS is_campaign_timeframe
	FROM contentful_landing_pages lp 
	INNER JOIN contentful_product_widgets c
		ON c.entry_id = lp.link_entry_id
	LEFT JOIN ods_production.product p
		ON POSITION(p.product_id IN c.product_ids) > 0 
	LEFT JOIN public.dim_dates d
		ON d.datum BETWEEN c.first_published_at::date AND COALESCE(lp.final_campaign_end_date::date, CASE WHEN DATEADD('day', 3, c.max_publish_date) > current_date THEN current_date ELSE DATEADD('day', 3, c.max_publish_date) END)
	WHERE c.rowno = 1
	  AND c.first_published_at::date >= DATEADD('month', -12, CURRENT_DATE)
	  AND LEFT(SUBSTRING(c.product_ids FROM POSITION(p.product_id IN c.product_ids) - 1), 1) = '"' 
	  AND LEFT(SUBSTRING(c.product_ids FROM POSITION(p.product_id IN c.product_ids) + length(p.product_id)),1) = '"'
)
, comparison_timeframe_raw AS (
	SELECT 
		campaign_title,
		--REPLACE(campaign_slug,'-',' ') AS campaign_slug,
		--product_sku,
		--country_name,
		--banner_title,
		min(fact_date) AS min_date,
		max(fact_date) AS max_date,
		DATEDIFF('day', min(fact_date), max(fact_date)) AS diff
	FROM campaign_timeframe 
	GROUP BY 1--,2,3,4,5
)
,comparison_timeframe AS (
	SELECT DISTINCT
		d.datum AS fact_date,
		c.campaign_title,
		c.campaign_slug,
		c.country_name,
		c.product_sku,
		c.banner_title,
		FALSE AS is_campaign_timeframe
	FROM public.dim_dates d 
	INNER JOIN comparison_timeframe_raw ct
	  ON d.datum BETWEEN DATEADD('day', -abs(ct.diff+1), ct.min_date) AND DATEADD('day', -1, ct.min_date)
	INNER JOIN (SELECT DISTINCT campaign_title, campaign_slug, product_sku, country_name, banner_title FROM campaign_timeframe) c
	  ON c.campaign_title = ct.campaign_title
	 -- AND c.product_sku = ct.product_sku
	 -- AND c.country_name = ct.country_name	  
	 -- AND c.banner_title = ct.banner_title
)
SELECT *
FROM campaign_timeframe
WHERE DATE_DIFF('day', fact_date, current_date) <= 7 --have TO bring incremental setup TO the END here, AS we would interfere WITH the comparison timeframe IF we SET the cut before
UNION 
SELECT *
FROM comparison_timeframe 
WHERE DATE_DIFF('day', fact_date, current_date) <= 7
;

--SELECT * FROM contentful_campaigns

--ASV metrics

DROP TABLE IF EXISTS contentful_campaigns_subs_metrics;
CREATE TEMP TABLE contentful_campaigns_subs_metrics
SORTKEY(
	product_sku,
	fact_date,
	country_name
)
DISTKEY(
	product_sku
)
AS 
WITH dates AS (
	SELECT DISTINCT 
		datum AS fact_date, 
		date_trunc('month',datum)::DATE AS month_bom,
		LEAST(DATE_TRUNC('MONTH',DATEADD('MONTH',1,DATUM))::DATE-1,CURRENT_DATE) AS MONTH_EOM
	FROM public.dim_dates d
	WHERE EXISTS (SELECT NULL FROM contentful_campaigns cc WHERE cc.fact_date = d.datum)
)
, product_countries AS (
	SELECT DISTINCT p.product_sku, s.country_name 
	FROM ods_production.product p
	CROSS JOIN (SELECT DISTINCT country_name FROM ods_production.store WHERE country_name NOT IN ('Andorra', 'United Kingdom')) s
	WHERE EXISTS (SELECT NULL FROM contentful_campaigns cc 
	              WHERE cc.product_sku = p.product_sku
	                AND cc.country_name = s.country_name
	             )
)
,  vector_unique_combinations AS (
	SELECT DISTINCT 
		d.fact_date,
		p.product_sku,
		p.country_name
	FROM dates d
	CROSS JOIN product_countries p
)
, s AS (
	SELECT 
		s.subscription_id,
		v.country_name,
		v.product_sku,
		s.fact_day,
		s.end_date,
		v.fact_date,
		s.subscription_value_eur AS subscription_value_euro,
		s.subscription_value_lc AS actual_subscription_value
 	FROM vector_unique_combinations v 
 	LEFT JOIN ods_production.subscription_phase_mapping s 
 		ON s.product_sku = v.product_sku
 		 AND s.country_name = v.country_name
 		 AND v.fact_date::date BETWEEN s.fact_day::date AND COALESCE(s.end_date::date, v.fact_date::date+1)
 	WHERE s.subscription_id IS NOT NULL
)
, active_subs_country AS (
	SELECT 
		s.fact_date,
		s.country_name,
		s.product_sku,
		COUNT(DISTINCT subscription_id) AS active_subscriptions,
		SUM(subscription_value_euro) AS active_subscription_value,
		SUM(actual_subscription_value) AS actual_subscription_value
	FROM s 
	GROUP BY 1,2,3
)
, active_subs_continent AS (
	SELECT 
		fact_date,
		'EU-All Markets' AS country_name,
		product_sku,
		sum(active_subscriptions) AS active_subscriptions,
		sum(active_subscription_value) AS active_subscription_value,
		sum(actual_subscription_value) AS actual_subscription_value
	FROM active_subs_country
	WHERE country_name IN ('Germany','Austria','Spain','Netherlands')
	GROUP BY 1,2,3
)
, active_subs_union AS (
	SELECT * FROM active_subs_country
	UNION ALL
	SELECT * FROM active_subs_continent
)
, acquisition_country AS (
	SELECT 
		v.fact_date AS start_date, 
		v.country_name AS country_name,
		v.product_sku,
		COUNT(DISTINCT s.subscription_id) AS acquired_subscriptions
	FROM vector_unique_combinations v
	LEFT JOIN ods_production.subscription s
		ON v.product_sku = s.product_sku
		 AND v.country_name = s.country_name
		 AND v.fact_date::date = s.start_date::date
	WHERE s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3
)
, acquisition_continent AS (
	SELECT 
		start_date, 
		'EU-All Markets' AS country_name,
		product_sku,
		sum(acquired_subscriptions) AS acquired_subscriptions
	FROM acquisition_country
	WHERE country_name IN ('Germany','Austria','Spain','Netherlands')
	GROUP BY 1,2,3
)
, acquisition_union AS (
	SELECT * FROM acquisition_country
	UNION ALL 
	SELECT * FROM acquisition_continent
)
SELECT DISTINCT
	v.fact_date,
	v.product_sku,
	v.country_name, 
	COALESCE(s.active_subscriptions, 0) AS active_subscriptions,
	COALESCE(s.active_subscription_value, 0) AS active_subscription_value,
	COALESCE(s.actual_subscription_value, 0) AS actual_subscription_value,
	COALESCE(a.acquired_subscriptions,0) AS acquired_subscriptions
FROM vector_unique_combinations v
LEFT JOIN active_subs_union s
	ON v.fact_date::date = s.fact_date::date
	AND v.product_sku = s.product_sku
	AND v.country_name = s.country_name
LEFT JOIN acquisition_union a 
	ON v.fact_date::date = a.start_date::date
	AND v.product_sku = a.product_sku
	AND v.country_name = a.country_name	
;

--SELECT * FROM contentful_campaigns_subs_metrics LIMIT 100;

--Conversion Rate metrics

DROP TABLE IF EXISTS contentful_campaigns_conversionrate_metrics;
CREATE TEMP TABLE contentful_campaigns_conversionrate_metrics
SORTKEY(
	product_sku,
	fact_date,
	country_name
)
DISTKEY(
	product_sku
)
AS 
WITH product_orders_country AS (
	SELECT
		o.created_date::date AS fact_date,
		oi.product_sku, 
	 	coalesce(o.store_country,'n/a') AS country_name,	  			
	  	count(CASE WHEN omc.is_paid IS TRUE THEN oi.order_id END) AS submitted_orders_paid_traffic
	FROM master.order o
	LEFT JOIN ods_production.order_marketing_channel omc
		ON o.order_id = omc.order_id
	LEFT JOIN ods_production.order_item oi
		ON oi.order_id = o.order_id 
	WHERE EXISTS (SELECT NULL FROM contentful_campaigns c WHERE o.created_date::date = c.fact_date)
	GROUP BY 1,2,3
) 
, product_orders_continent AS (
	SELECT
		fact_date,
		product_sku, 
	 	'EU-All Markets' AS country_name,  			
		sum(submitted_orders_paid_traffic) AS submitted_orders_paid_traffic
	FROM product_orders_country
	WHERE country_name IN ('Germany','Austria','Spain','Netherlands')
	GROUP BY 1,2,3
)
, product_orders_union AS (
	SELECT * FROM product_orders_country
	UNION all
	SELECT * FROM product_orders_continent
)
, traffic_country AS (
	SELECT 
		pv.page_view_date AS fact_date,
		p.product_sku,
		CASE 
			WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
			WHEN s.geo_country= 'DE' THEN 'Germany'		
			WHEN s.geo_country= 'ES' THEN 'Spain'
			WHEN s.geo_country= 'AT' THEN 'Austria'
		 	WHEN s.geo_country= 'NL' THEN 'Netherlands'
			WHEN s.geo_country= 'US' THEN 'United States'
	    END AS country_name,
		count(DISTINCT CASE WHEN s.is_paid THEN COALESCE(pv.customer_id_mapped, pv.anonymous_id) END) AS paid_traffic
	FROM traffic.page_views pv
	LEFT JOIN traffic.sessions s
	  ON pv.session_id = s.session_id
	LEFT JOIN ods_production.product p
	ON p.product_sku = pv.page_type_detail
	WHERE EXISTS (SELECT NULL FROM contentful_campaigns c WHERE c.fact_date = pv.page_view_start::date)
	AND page_type = 'pdp'
	GROUP BY 1,2,3	
)
, traffic_continent AS (
	SELECT 
		fact_date,
		product_sku,
		'EU-All Markets' AS country_name,
		sum(paid_traffic) AS paid_traffic
	FROM traffic_country
	WHERE country_name IN ('Germany','Austria','Spain','Netherlands')
	GROUP BY 1,2,3	
)
, traffic_union AS (
	SELECT * FROM traffic_country
	UNION ALL 
	SELECT * FROM traffic_continent
)
SELECT
	t.fact_date,
	t.product_sku,
	coalesce(t.country_name,po.country_name) AS country_name, 
	coalesce(t.paid_traffic, 0) AS paid_traffic,
	coalesce(po.submitted_orders_paid_traffic, 0) AS submitted_orders_paid_traffic,
	coalesce( 
	CASE
		WHEN coalesce(t.paid_traffic, 0) != 0 
			THEN (po.submitted_orders_paid_traffic::DOUBLE PRECISION / t.paid_traffic::DOUBLE PRECISION)
		ELSE 0
	END,0) AS paid_conversion_per_product
FROM traffic_union t 
LEFT JOIN product_orders_union po 
 ON po.product_sku = t.product_sku
  AND po.fact_date = t.fact_date 
  AND po.country_name = t.country_name
;

--SELECT * FROM contentful_campaigns_conversionrate_metrics LIMIT 100;


--Pricing metrics

DROP TABLE IF EXISTS contentful_campaigns_pricing_metrics;
CREATE TEMP TABLE contentful_campaigns_pricing_metrics
SORTKEY(
	product_sku,
	start_date_pricing,
	country_name
)
DISTKEY(
	product_sku
)
AS 
WITH pricing_country_raw AS (
	SELECT 
		start_date::date AS start_date_pricing,
		product_sku, 
		CASE
			WHEN store_parent = 'de'
			THEN 'Germany'
			WHEN store_parent = 'at'
			THEN 'Austria'
			WHEN store_parent = 'es'
			THEN 'Spain'
			WHEN store_parent = 'nl'
			THEN 'Netherlands' 
			WHEN store_parent = 'us'
			THEN 'United States' 
		END AS country_name,
		min(m1_price) AS m1_price,
		min(m3_price) AS m3_price,
		min(m6_price) AS m6_price,
		min(m12_price) AS m12_price,
		min(m18_price) AS m18_price,
		min(m24_price) AS m24_price,
		max(is_discount) AS is_discount
	FROM pricing.prod_pricing_historical
	WHERE store_parent IN ('de','at','es','nl','us') 	
	GROUP BY 1,2,3
)
, pricing_country AS (
	SELECT 
		start_date_pricing,
		COALESCE(dateadd('day',-1,LAG(start_date_pricing) OVER (PARTITION BY product_sku, country_name ORDER BY start_date_pricing DESC)),'2099-12-31')::date AS end_date_pricing,
		product_sku,
		country_name,
		m1_price,
		m3_price,
		m6_price,
		m12_price,
		m18_price,
		m24_price,
		is_discount
	FROM pricing_country_raw	
)
, pricing_continent AS (
	SELECT 
		start_date_pricing,
		end_date_pricing,
		product_sku,
		'EU-All Markets' AS country_name,
		avg(m1_price) AS m1_price,
		avg(m3_price) AS m3_price,
		avg(m6_price) AS m6_price,
		avg(m12_price) AS m12_price,
		avg(m18_price) AS m18_price,
		avg(m24_price) AS m24_price,
		max(is_discount) AS is_discount 
	FROM pricing_country
	WHERE country_name IN ('Germany','Austria','Spain','Netherlands')
	GROUP BY 1,2,3,4
)
SELECT * FROM pricing_country
UNION ALL 
SELECT * FROM pricing_continent
;

--SELECT * FROM contentful_campaigns_pricing_metrics LIMIT 100;

--Traffic metrics 

DROP TABLE IF EXISTS contentful_campaigns_traffic_metrics;
CREATE TEMP TABLE contentful_campaigns_traffic_metrics AS
WITH page_views_filter AS (
	SELECT 
		p.page_view_date,
		p.session_id,
		p.page_view_id ,
		p.page_type_detail,
		s.relative_vmax 
	FROM traffic.page_views p
	LEFT JOIN scratch.web_events_scroll_depth s
		ON s.page_view_id = p.page_view_id
	WHERE store_label IN (
			'Grover - Austria online',
			'Grover - Germany online',
			'Grover - Netherlands online',
			'Grover - Spain online',
			'Grover - United States online') 
	    AND EXISTS (SELECT NULL FROM contentful_campaigns c WHERE c.fact_date = p.page_view_date) 
	    AND page_type = 'pdp'
)
, page_views_join_country AS (    
	SELECT DISTINCT
	      pv.page_view_date,
	      CASE WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
	      		WHEN s.geo_country = 'DE' THEN 'Germany'
	      		WHEN s.geo_country = 'ES' THEN 'Spain'
	      		WHEN s.geo_country = 'AT' THEN 'Austria'
	      		WHEN s.geo_country = 'NL' THEN 'Netherlands'
	      		WHEN s.geo_country = 'US' THEN 'United States'
	      END AS country_name,
	      pv.page_type_detail,
	      count(DISTINCT pv.session_id) AS sessions_with_product_page_views,
	      count(DISTINCT pv.page_view_id) AS product_page_views,
	      avg(relative_vmax) AS avg_scroll_depth
	FROM page_views_filter pv 
	LEFT JOIN traffic.sessions s
		ON pv.session_id = s.session_id 
	WHERE country_name IS NOT NULL
	GROUP BY 1,2,3
)
, page_views_join_continent AS (
	SELECT 
		page_view_date,
	    'EU-All Markets' country_name,
		page_type_detail,
		sum(sessions_with_product_page_views) AS sessions_with_product_page_views,
	    sum(product_page_views) AS product_page_views,
	    avg(avg_scroll_depth) AS avg_scroll_depth
	FROM page_views_join_country
	WHERE country_name IN ('Germany','Austria','Spain','Netherlands')
	GROUP BY 1,2,3
)
SELECT DISTINCT
	p.page_view_date AS fact_date,
	p.country_name AS country_name,
	p.page_type_detail AS product_sku,
	product_page_views,
	sessions_with_product_page_views,
	avg_scroll_depth
FROM page_views_join_country p
UNION ALL 
SELECT DISTINCT 
	page_view_date AS fact_date,
	country_name,
	page_type_detail AS product_sku,
	product_page_views,
	sessions_with_product_page_views,
	avg_scroll_depth
FROM page_views_join_continent
;

--SELECT * FROM contentful_campaigns_traffic_metrics LIMIT 100;


--Traffic metrics on Landing Pages

DROP TABLE IF EXISTS contentful_campaigns_traffic_landingpages_metrics;
CREATE TEMP TABLE contentful_campaigns_traffic_landingpages_metrics AS 
WITH page_views_filter AS (
	SELECT 
		p.page_view_date,
		p.session_id,
		p.page_view_id ,
		p.page_type_detail, 
		s.relative_vmax 
	FROM traffic.page_views p
	LEFT JOIN scratch.web_events_scroll_depth s
		ON s.page_view_id = p.page_view_id 
	WHERE p.store_label IN (
			'Grover - Austria online',
			'Grover - Germany online',
			'Grover - Netherlands online',
			'Grover - Spain online',
			'Grover - United States online') 
	    AND EXISTS (SELECT NULL FROM contentful_campaigns c WHERE c.fact_date = p.page_view_date AND c.campaign_slug = p.page_type_detail AND c.is_campaign_timeframe = TRUE)	
	    AND p.page_type = 'g explore'
)
, page_views_join_country AS (    
	SELECT DISTINCT
	      pv.page_view_date,
	      CASE WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
	      		WHEN s.geo_country = 'DE' THEN 'Germany'
	      		WHEN s.geo_country = 'ES' THEN 'Spain'
	      		WHEN s.geo_country = 'AT' THEN 'Austria'
	      		WHEN s.geo_country = 'NL' THEN 'Netherlands'
	      		WHEN s.geo_country = 'US' THEN 'United States'
	      END AS country_name,
	      pv.page_type_detail,
	      count(DISTINCT pv.session_id) AS sessions_landingpages,
	      count(DISTINCT pv.page_view_id) AS landingpages_page_views,
	      avg(relative_vmax) AS avg_scroll_depth
	FROM page_views_filter pv 
	LEFT JOIN traffic.sessions s
		ON pv.session_id = s.session_id 
	WHERE country_name IS NOT NULL
	GROUP BY 1,2,3
)
, page_views_join_continent AS (
	SELECT 
		page_view_date,
	    'EU-All Markets' country_name,
		page_type_detail,
		sum(sessions_landingpages) AS sessions_landingpages,
	    sum(landingpages_page_views) AS landingpages_page_views,
	    avg(avg_scroll_depth) AS avg_scroll_depth
	FROM page_views_join_country
	WHERE country_name IN ('Germany','Austria','Spain','Netherlands')
	GROUP BY 1,2,3
)
SELECT DISTINCT
	p.page_view_date AS fact_date,
	p.country_name AS country_name,
	p.page_type_detail AS landing_page_slug,
	landingpages_page_views,
	sessions_landingpages,
	avg_scroll_depth AS avg_scroll_depth_landingpage
FROM page_views_join_country p
UNION ALL 
SELECT DISTINCT 
	page_view_date AS fact_date,
	country_name,
	page_type_detail AS landing_page_slug,
	landingpages_page_views,
	sessions_landingpages,
	avg_scroll_depth AS avg_scroll_depth_landingpage
FROM page_views_join_continent
;

--SELECT * FROM contentful_campaigns_traffic_landingpages_metrics LIMIT 100;


--Inventory Metrics
--Store dimension is not at market level 

DROP TABLE IF EXISTS contentful_campaigns_inventory_metrics;
CREATE TEMP TABLE contentful_campaigns_inventory_metrics
SORTKEY(
	product_sku,
	fact_date
)
DISTKEY(
	product_sku
)
AS 
WITH assets AS (
	SELECT
		a."date" AS fact_date,
		a.product_sku,
		count(
			CASE 
			  	WHEN a.asset_status_original = 'IN STOCK' 
			  		THEN a.asset_id 
			  END) AS stock_on_hand,
		count(
			CASE 
			  	WHEN a.asset_status_original = 'RESERVED' 
			  		THEN a.asset_id 
			  END) AS reserved_count,
		stock_on_hand + reserved_count AS available_count	  
--		coalesce(avg(
--			CASE 
--				WHEN a.asset_status_original = 'IN STOCK' 
--					THEN coalesce(a.days_in_stock, 0)
--			END),0) AS avg_days_in_stock
	FROM master.asset_historical a 
	WHERE EXISTS (SELECT NULL FROM contentful_campaigns cc WHERE cc.fact_date = a.date)
	GROUP BY 1,2
)
, inbound AS (
	SELECT
		purchased_date::date AS fact_date,
		product_sku,
		sum(net_quantity) AS incoming_qty
	FROM ods_production.purchase_request_item
	WHERE request_status 
	NOT IN ('CANCELLED', 'NEW')
	GROUP BY 1,2
)
SELECT 
	a.fact_date,
	a.product_sku,
	COALESCE(a.stock_on_hand,0) AS stock_on_hand,
	COALESCE(a.reserved_count,0) AS reserved_count,
	COALESCE(a.available_count,0) AS available_count,
	--COALESCE(a.avg_days_in_stock,0) AS avg_days_in_stock,
	CASE 
		WHEN COALESCE(i.incoming_qty,0)>0
			THEN incoming_qty
		ELSE 0	
	END AS incoming_qty
FROM assets a 
LEFT JOIN inbound i
	ON i.product_sku = a.product_sku 
	 AND i.fact_date = a.fact_date
;


-- Orders Metrics
DROP TABLE IF EXISTS contentful_campaigns_order_metrics;
CREATE TEMP TABLE contentful_campaigns_order_metrics
SORTKEY(
	product_sku,
	fact_date,
	country_name
)
DISTKEY(
	product_sku
)
AS 
SELECT 
	o.created_date::date AS fact_date,
	o.store_country AS country_name, 
	oi.product_sku,
	count(DISTINCT oi.order_id) AS carts,
	count(DISTINCT o.order_id) AS created_orders,
	count(DISTINCT CASE WHEN o.completed_orders >= 1  THEN o.order_id END) AS submitted_orders,
	count(DISTINCT CASE WHEN o.paid_orders >= 1 THEN o.order_id END) AS paid_orders, 
	count(DISTINCT s.subscription_id) AS subscriptions_number
FROM master."order" o  
LEFT JOIN ods_production.order_item oi
	ON o.order_id = oi.order_id
LEFT JOIN master.subscription s 
		ON oi.product_sku = s.product_sku
		AND o.order_id = s.order_id	
WHERE EXISTS (SELECT NULL FROM contentful_campaigns cc WHERE cc.fact_date = o.created_date::date AND cc.product_sku = oi.product_sku)
GROUP BY 1,2,3
;


--Final_output_table
DELETE FROM dm_commercial.contentful_campaigns_overall_metrics WHERE DATE_DIFF('day', fact_date, current_date) <= 7;
INSERT INTO dm_commercial.contentful_campaigns_overall_metrics 
(
	fact_date,
	product_sku, 
	category_name,
	subcategory_name,
	brand,
	product_name,
	campaign_title,
	banner_title,
	country_name,
	is_campaign_timeframe,
	campaign_slug,
	active_subscriptions,
	active_subscription_value,
	actual_subscription_value,
	acquired_subscriptions,
	paid_conversion_per_product,
	m1_price,
	m3_price,
	m6_price,
	m12_price,
	m18_price,
	m24_price,
	is_discount,
	product_page_views,
	sessions_with_product_page_views,
	stock_on_hand,
	reserved_count,
	available_count,
	landingpages_page_views,
	sessions_landingpages,
	carts,
	created_orders,
	submitted_orders,
	paid_orders,
	subscription_number,
	avg_scroll_depth,
	avg_scroll_depth_landingpage
) 
SELECT 
	cc.fact_date,
	cc.product_sku, 
	pr.category_name,
	pr.subcategory_name,
	pr.brand,
	pr.product_name,
	cc.campaign_title,
	cc.banner_title,
	cc.country_name,
	cc.is_campaign_timeframe,
	cc.campaign_slug,
	COALESCE(s.active_subscriptions,0) AS active_subscriptions,
	COALESCE(s.active_subscription_value,0) AS active_subscription_value,
	COALESCE(s.actual_subscription_value,0) AS actual_subscription_value,
	COALESCE(s.acquired_subscriptions,0) AS acquired_subscriptions,
	cr.paid_conversion_per_product,
	p.m1_price,
	p.m3_price,
	p.m6_price,
	p.m12_price,
	p.m18_price,
	p.m24_price,
	p.is_discount,
	COALESCE(t.product_page_views,0) AS product_page_views,
	COALESCE(t.sessions_with_product_page_views,0) AS sessions_with_product_page_views,
	COALESCE(i.stock_on_hand,0) AS stock_on_hand,
	COALESCE(i.reserved_count,0) AS reserved_count,
	COALESCE(i.available_count,0) AS available_count,
	COALESCE(lp.landingpages_page_views,0) AS landingpages_page_views,
	COALESCE(lp.sessions_landingpages,0) AS sessions_landingpages,
	COALESCE(o.carts,0) AS carts,
	COALESCE(o.created_orders,0) AS created_orders,
	COALESCE(o.submitted_orders,0) AS submitted_orders,
	COALESCE(o.paid_orders,0) AS paid_orders,
	COALESCE(o.subscriptions_number,0) AS subscription_number,
	COALESCE(t.avg_scroll_depth,0) AS avg_scroll_depth,
	COALESCE(lp.avg_scroll_depth_landingpage,0) AS avg_scroll_depth_landingpage
FROM contentful_campaigns cc
LEFT JOIN ods_production.product pr
	ON pr.product_sku = cc.product_sku
LEFT JOIN contentful_campaigns_subs_metrics s
	ON s.fact_date = cc.fact_date
	 AND s.product_sku = cc.product_sku
	 AND s.country_name = cc.country_name
LEFT JOIN contentful_campaigns_conversionrate_metrics cr
	ON cr.fact_date = cc.fact_date
	 AND cr.product_sku = cc.product_sku
	 AND cr.country_name = cc.country_name
LEFT JOIN contentful_campaigns_pricing_metrics p	
	ON p.product_sku = cc.product_sku
	 AND p.country_name = cc.country_name
	 AND cc.fact_date BETWEEN start_date_pricing AND end_date_pricing
LEFT JOIN contentful_campaigns_traffic_metrics t
	ON t.fact_date = cc.fact_date
	 AND t.product_sku = cc.product_sku
	 AND t.country_name = cc.country_name
LEFT JOIN contentful_campaigns_traffic_landingpages_metrics lp
	ON lp.fact_date = cc.fact_date
	 AND lp.country_name = cc.country_name
	 AND lp.landing_page_slug = cc.campaign_slug
LEFT JOIN contentful_campaigns_order_metrics o
	ON o.fact_date = cc.fact_date
	 AND o.country_name = cc.country_name
	 AND o.product_sku = cc.product_sku
LEFT JOIN contentful_campaigns_inventory_metrics i 
	ON i.fact_date = cc.fact_date
	 AND i.product_sku = cc.product_sku
	 --AND i.country_name = cc.country_name
;