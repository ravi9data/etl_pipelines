DELETE
FROM
WHERE
	event_time::DATE >=(current_Date-3);


WITH a AS (
SELECT
	DISTINCT session_id,
	event_time,
	ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC) AS rn
FROM
	segment.identify_events
WHERE
	event_time::DATE  >=(current_Date-3)   
)
SELECT 
	session_id::VARCHAR AS session_id,
	event_time,
	CASE 
FROM 
	a
WHERE 
	rn = 1
;
	

DELETE
FROM
	trans_dev.first_page_in_session
WHERE
	page_view_start::DATE >=(current_Date-3);


INSERT INTO trans_dev.first_page_in_session
SELECT
	a.session_id,
	a.anonymous_id,
	a.page_view_date,
	a.os_family,
	a.device_type,
	a.page_type,
	a.store_label,
	a.page_view_start,
	ROW_NUMBER() OVER (PARTITION BY a.session_id ORDER BY a.page_view_start) AS page_view_in_session_index
FROM
	traffic.page_views a -------------
WHERE
	a.page_view_start::DATE >=(current_Date-3)  
;



DELETE
FROM
	trans_dev.grove_care_pdp_1
WHERE
	page_view_date::DATE >=(current_Date-3);


INSERT INTO trans_dev.grove_care_pdp_1
SELECT
    pv.page_view_date,
    CASE
        WHEN s.first_page_url ILIKE '%/de-%' THEN 'Germany'
        WHEN s.first_page_url ILIKE '%/us-%' THEN 'United States'
        WHEN s.first_page_url ILIKE '%/es-%' THEN 'Spain'
        WHEN s.first_page_url ILIKE '%/nl-%' THEN 'Netherlands'
        WHEN s.first_page_url ILIKE '%/at-%' THEN 'Austria'
        WHEN s.first_page_url ILIKE '%/business_es-%' THEN 'Spain'
        WHEN s.first_page_url ILIKE '%/business-%' THEN 'Germany'
        WHEN s.first_page_url ILIKE '%/business_at-%' THEN 'Austria'
        WHEN s.first_page_url ILIKE '%/business_nl-%' THEN 'Netherlands'
        WHEN s.first_page_url ILIKE '%/business_us-%' THEN 'United States'
        WHEN s.store_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
        WHEN s.store_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
        WHEN s.store_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
        WHEN s.store_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
        WHEN s.store_name IS NULL AND s.geo_country = 'US' THEN 'United States'
        WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
        ELSE 'Germany' 
    END AS country,
    CASE 
	   WHEN cu.is_new_visitor IS TRUE 
	    THEN 'NEW' 
	   ELSE 'RECURRING' 
	  END AS new_recurring,
    CASE
	   WHEN pv.os_family ilike 'web' 
	     THEN 'web'
	   WHEN pv.os_family in ('android','Android','ios','iOS') 
	     THEN 'app'
	   ELSE NULL 
    END AS app_web ,
    COUNT(distinct pv.session_id) AS traffic_daily_unique_sessions,
    COUNT(DISTINCT CASE WHEN pv.page_type = 'pdp' THEN pv.session_id END) AS sessions_with_product_page_views
FROM 
	trans_dev.first_page_in_session pv 
LEFT JOIN traffic.sessions s
    ON pv.session_id = s.session_id
LEFT JOIN traffic.snowplow_user_mapping cu
    ON pv.anonymous_id = cu.anonymous_id
	AND pv.session_id = cu.session_id
WHERE
	pv.page_view_date::date   >=(current_Date-3)
GROUP BY
	1,2,3,4,5
;


---- Add to Cart

SELECT
	DISTINCT
    	order_id
FROM
	segment.track_events
WHERE
	event_time::DATE >= '2023-09-15'
	AND event_name = 'Product Added to Cart'
	AND properties ILIKE '%care_coverage%'
UNION
SELECT
	DISTINCT order_id
FROM
WHERE


DELETE
FROM
	trans_dev.tmp_conversion_funnel_daily_report_orders_cart
WHERE
	created_date::DATE >=(current_Date-3);


INSERT INTO trans_dev.tmp_conversion_funnel_daily_report_orders_cart
SELECT
	DISTINCT
    	o.created_date::date AS created_date,
	o.store_country AS country,
	o.new_recurring,
	CASE
		WHEN cc.order_id IS NULL THEN 'disabled'
		ELSE 'enabled'
	CASE
		WHEN oc.os ilike 'web' THEN 'web'
		WHEN oc.os in ('android','ios') THEN 'app'
		WHEN o.device = 'n/a' THEN 'app'
		ELSE NULL
	END AS app_web,
	count(DISTINCT o.order_id) AS cart_orders
FROM
	master."order" o
LEFT JOIN traffic.order_conversions oc
    ON o.order_id = oc.order_id
	ON o.order_id = cc.order_id
WHERE
	o.created_date::date >=(current_Date-3)
GROUP BY
	1,2,3,4,5
;


DROP TABLE IF EXISTS dm_finance.grove_care_pdp;
CREATE TABLE dm_finance.grove_care_pdp AS
SELECT
    COALESCE(o.page_view_date::date,p.created_date::date) AS created_date,
    COALESCE(o.country,p.country) AS country,
    COALESCE(o.new_recurring,p.new_recurring) AS new_recurring,
    COALESCE(o.app_web,p.app_web) AS app_web,
    COALESCE(traffic_daily_unique_sessions,0) AS traffic_daily_unique_sessions,
    COALESCE(sessions_with_product_page_views,0) AS viewed,
    COALESCE(p.cart_orders,0) AS cart_orders
FROM
	trans_dev.grove_care_pdp_1 o
FULL JOIN trans_dev.tmp_conversion_funnel_daily_report_orders_cart p
	ON o.page_view_date::date = p.created_date::date
	AND o.country = p.country
	AND o.new_recurring = p.new_recurring
	AND o.app_web = p.app_web
;


