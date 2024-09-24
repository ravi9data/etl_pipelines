CREATE OR REPLACE VIEW dm_marketing.v_conversion_funnel_daily_report_products_out_of_stock_pdp AS
WITH page_views_filter AS (
	SELECT 
		page_view_date,
		os_family,
		device_type,
		session_id,
		anonymous_id,
		page_view_id ,
		page_type_detail 
	FROM traffic.page_views
	WHERE  
	      store_label IN (
			'Grover - Austria online',
			'Grover - Germany online',
			'Grover - Netherlands online',
			'Grover - Spain online',
			'Grover - UK online',
			'Grover - USA old online',
			'Grover - United States online') 
	    AND page_view_date BETWEEN DATEADD('month', -3, current_date) AND DATEADD('day', -1, current_date) -- changed FOR 1 MONTH
	    AND page_type = 'pdp'
)
, products_with_stock AS (
	SELECT 
		date AS datum,
		product_sku,
		CASE warehouse 
			WHEN 'office_us' THEN 'US'
			WHEN 'ups_softeon_us_kylse' then 'US'
			WHEN 'synerlogis_de' THEN 'Europe'
			WHEN 'ups_softeon_eu_nlrng' THEN 'Europe'
			WHEN 'ingram_micro_eu_flensburg' THEN 'Europe'
			WHEN 'office_de' THEN 'Europe'
			END AS continent,
		count(DISTINCT a.asset_id ) AS number_assets_in_stock
	FROM master.asset_historical a
	WHERE a.asset_status_original IN ('IN STOCK')
	GROUP BY 1,2,3
)
, page_views_join AS (    
	SELECT DISTINCT
	      pv.page_view_date,
	      CASE WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
	      		WHEN s.geo_country = 'DE' THEN 'Germany'
	      		WHEN s.geo_country = 'ES' THEN 'Spain'
	      		WHEN s.geo_country = 'AT' THEN 'Austria'
	      		WHEN s.geo_country = 'NL' THEN 'Netherlands'
	      		WHEN s.geo_country = 'US' THEN 'United States' END
	      	AS country,
	      CASE	WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands') THEN 'Europe'
		      	WHEN s.store_name IN ('United States') THEN 'US'
		      	WHEN s.geo_country IN ('DE','ES','AT','NL')  THEN 'Europe'
		      	WHEN s.geo_country IN ('US') THEN 'US'
	      	END AS continent,
	      pv.os_family AS os,
	      CASE WHEN os_family = 'Linux' AND pv.device_type = 'Other' THEN 'Computer' ELSE pv.device_type END AS device,
	      s.marketing_channel,
	      pv.session_id,
	      pv.anonymous_id,
	      pv.page_view_id ,
	      pv.page_type_detail,
	      CASE WHEN ss.number_assets_in_stock = 0 OR ss.number_assets_in_stock IS NULL THEN 1 ELSE 0 END AS is_product_out_of_stock
	   FROM page_views_filter pv 
	   LEFT JOIN traffic.sessions s
	   		ON pv.session_id = s.session_id 
	   LEFT JOIN products_with_stock ss
			ON pv.page_view_date::date = ss.datum::date
			AND continent = ss.continent
			AND pv.page_type_detail = ss.product_sku
	   WHERE country IS NOT NULL
  )
  SELECT DISTINCT
      p.page_view_date,
      p.country,
      p.continent,
      p.os,
      p.device,
      p.marketing_channel,
      p.page_type_detail AS product_sku,
      is_product_out_of_stock,
      pp.product_name ,
	  pp.category_name ,
	  pp.subcategory_name ,
	  pp.brand,
      COUNT(DISTINCT p.page_view_id) AS product_page_views ,
      COUNT(DISTINCT  p.session_id) AS sessions_with_product_page_views,
      COUNT(DISTINCT CASE WHEN is_product_out_of_stock = 1 THEN p.page_view_id END) AS product_page_views_with_no_stock,
      COUNT(DISTINCT CASE WHEN is_product_out_of_stock = 1 THEN p.session_id END) AS sessions_with_product_page_views_with_no_stock 
   FROM page_views_join p
   LEFT JOIN products_with_stock s
   	ON p.page_view_date::date = s.datum::date
   	AND p.continent = s.continent
   	AND p.page_type_detail = s.product_sku
   LEFT JOIN ods_production.product pp 
	ON p.page_type_detail = pp.product_sku
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12  
  WITH NO SCHEMA BINDING;
