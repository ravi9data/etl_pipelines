DROP TABLE IF EXISTS tmp_orders_prep_data;
CREATE TEMP TABLE tmp_orders_prep_data
SORTKEY(created_date, os, device, country, new_recurring, marketing_channel)
DISTKEY(created_date)
AS
WITH address_or_payment_event_orders AS (
	SELECT DISTINCT order_id, event_name
	FROM segment.track_events 
	WHERE 
		(event_name ILIKE '%address%'
	 		AND event_name NOT ILIKE '%email%'
	 		AND event_name NOT ILIKE '%billing%'
	 		AND event_name NOT IN ('Retrieve Loqate Address','Employee address upserted'))
	 	OR 
	 	 (event_name ILIKE '%payment method%'
	 		AND event_name NOT ILIKE '%delete%'
	 		AND event_name NOT ILIKE '%cancel%'
	 		AND event_name NOT ILIKE '%error%')
	 	OR event_name IN ('Cart Continue Clicked', 'Checkout Started', 'Order Review Viewed')
)
,orders_not_submitted_insinde_app AS (	 	
	SELECT DISTINCT 
		a.order_id,
		'Address'::varchar AS event_name
	FROM react_native.address_added a
	INNER JOIN master.ORDER o 
		ON a.order_id = o.order_id 
		AND o.completed_orders = 0
	UNION 
	SELECT DISTINCT
		a.order_id,
		'Address'::varchar AS event_name
	FROM react_native.address_selected a
	INNER JOIN master.ORDER o 
		ON a.order_id = o.order_id 
		AND o.completed_orders = 0
	UNION 
	SELECT DISTINCT
		a.order_id,
		'Address'::varchar AS event_name
	FROM react_native.delivery_address_edited a
	INNER JOIN master.ORDER o 
		ON a.order_id = o.order_id 
		AND o.completed_orders = 0
	UNION 
	SELECT DISTINCT
		a.order_id,
		'Payment'::varchar AS event_name
	FROM react_native.payment_method_added a
	INNER JOIN master.ORDER o 
		ON a.order_id = o.order_id 
		AND o.completed_orders = 0
	UNION 
	SELECT DISTINCT
		a.order_id,
		'Payment'::varchar AS event_name
	FROM react_native.payment_method_edited a
	INNER JOIN master.ORDER o 
		ON a.order_id = o.order_id 
		AND o.completed_orders = 0
	UNION 
	SELECT DISTINCT
		a.order_id,
		'Payment'::varchar AS event_name
	FROM react_native.payment_method_selected a
	INNER JOIN master.ORDER o 
		ON a.order_id = o.order_id 
		AND o.completed_orders = 0
	UNION 
	SELECT DISTINCT
		a.order_id,
		'Checkout'::varchar AS event_name
	FROM react_native.checkout_started a
	INNER JOIN master.ORDER o 
		ON a.order_id = o.order_id 
		AND o.completed_orders = 0
	UNION 
	SELECT DISTINCT
		a.order_id,
		'Checkout'::varchar AS event_name
	FROM react_native.order_review_viewed a
	INNER JOIN master.ORDER o 
		ON a.order_id = o.order_id 
		AND o.completed_orders = 0
)

   SELECT
       o.order_id,
   	   o.created_date::date AS created_date,
   	   o.submitted_date::DATE AS submitted_date,
       COALESCE(o.store_country, 'n/a') AS country,
       LOWER(CASE WHEN oc.os = 'n/a' or oc.os is null THEN 'Others' ELSE COALESCE(oc.os, 'Others') END) AS os,      
       LOWER(CASE WHEN os in ('Linux','Mac OS X','Windows') AND o.device in ('n/a', 'Other') THEN 'Computer'
           WHEN device = 'n/a' THEN 'Other' ELSE COALESCE(o.device, 'Other') END) AS device,   
       COALESCE(o.new_recurring, 'n/a') AS new_recurring,
       COALESCE(o.marketing_channel, 'n/a') AS marketing_channel,
       CASE WHEN o.customer_type IS NOT NULL THEN o.customer_type
			 WHEN COALESCE(o.store_commercial, 'n/a') ILIKE '%b2b%' THEN 'business_customer'
			 ELSE 'normal_customer' END AS customer_type,
       MAX(CASE WHEN o.customer_id IS NOT NULL THEN 1 
		 	WHEN ad.order_id IS NOT NULL THEN 1
		 	WHEN aaa.order_id IS NOT NULL THEN 1
		ELSE 0 END) AS is_customer_logged_in, -- IF the customer IS IN the NEXT steps, the customer should be logged
       count(DISTINCT o.order_id) AS cart_orders,
       COUNT(DISTINCT CASE WHEN o.completed_orders >= 1 THEN o.order_id END) AS submitted_orders,
       COUNT(DISTINCT CASE WHEN o.approved_date IS NOT NULL THEN o.order_id END) AS approved_orders,
       COUNT(DISTINCT CASE WHEN o.paid_orders >= 1 THEN o.order_id END) AS paid_orders   ,
       --conversions metrics
       count(DISTINCT 
			CASE WHEN o.completed_orders > 0 THEN o.order_id --FOR the customer submit an ORDER, they should have the cart page
			 	 WHEN ad.order_id IS NOT NULL THEN o.order_id 
			 	 WHEN aaa.order_id IS NOT NULL THEN o.order_id --address app tables
			  END) AS cart_page_conversion,
		count(DISTINCT 
			CASE WHEN o.completed_orders > 0 THEN o.order_id  --FOR the customer submit an ORDER, they should have the address
				 WHEN ad.event_name ILIKE '%address%' THEN o.order_id 
				 WHEN aaa.event_name = 'Address' THEN o.order_id
			END) AS address_conversion,
		count(DISTINCT 
			CASE WHEN o.completed_orders > 0 THEN o.order_id --FOR the customer submit an ORDER, they should have the payment
				 WHEN ad.event_name ILIKE '%payment method%' THEN o.order_id 
				 WHEN aaa.event_name = 'Payment' THEN o.order_id
			 END) AS payment_conversion,
		count(DISTINCT 
			CASE WHEN o.completed_orders > 0 THEN o.order_id --FOR the customer submit an ORDER, they should have the payment
				 WHEN ad.event_name ILIKE '%payment method%' THEN o.order_id 
				 WHEN ad.event_name ILIKE '%address%' THEN o.order_id 
				 WHEN aaa.event_name IN ('Address', 'Payment') THEN o.order_id
			 END) AS payment_or_address_conversion			
   FROM master."order" o
   LEFT JOIN traffic.order_conversions oc
     ON o.order_id = oc.order_id
   LEFT JOIN address_or_payment_event_orders ad 
	 ON o.order_id = ad.order_id
   LEFT JOIN orders_not_submitted_insinde_app aaa
   	 ON o.order_id = aaa.order_id
   WHERE coalesce(o.paid_date::DATE,o.submitted_date::DATE,o.created_date::date) >= DATEADD('month', -13, current_date)
   GROUP BY 1,2,3,4,5,6,7,8,9;

  
DROP TABLE IF EXISTS tmp_conversion_funnel_daily_report_cart_orders;
CREATE TEMP TABLE tmp_conversion_funnel_daily_report_cart_orders AS
SELECT 
	created_date AS reporting_date,
	country,
	os,      
	device,   
	new_recurring,
	marketing_channel,
	is_customer_logged_in,
	customer_type,
	sum(cart_orders) AS cart_orders,
	sum(cart_page_conversion) AS cart_page_conversion,
	sum(payment_conversion) AS payment_conversion,
	sum(address_conversion) AS address_conversion,
	sum(submitted_orders) AS submitted_orders_conversion,
	sum(payment_or_address_conversion) AS payment_or_address_conversion
FROM tmp_orders_prep_data
GROUP BY 1,2,3,4,5,6,7,8;

DROP TABLE IF EXISTS tmp_conversion_funnel_daily_report_submitted_orders;
CREATE TEMP TABLE tmp_conversion_funnel_daily_report_submitted_orders AS
SELECT
    submitted_date AS reporting_date,
    country,
    os,
    device,
    new_recurring,
    marketing_channel,
    is_customer_logged_in,
    customer_type,
    sum(submitted_orders) AS submitted_orders,
    sum(approved_orders) AS approved_orders,
    sum(paid_orders) AS paid_orders
FROM tmp_orders_prep_data
WHERE submitted_date IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8;

DROP TABLE IF EXISTS tmp_conversion_funnel_daily_report_page_view;
CREATE TEMP TABLE tmp_conversion_funnel_daily_report_page_view
SORTKEY(reporting_date, os, device, country, new_recurring, marketing_channel)
DISTKEY(reporting_date)
AS
WITH first_page_in_session AS (
SELECT 
	session_id,
    anonymous_id,
    page_view_date,
    page_view_id,
    os_family,
    device_type,
    page_type,
    user_bounced,
    store_label,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY page_view_start) AS page_view_in_session_index
FROM traffic.page_views
WHERE page_view_start::DATE BETWEEN DATEADD('day', -15, current_date) AND DATEADD('day', -1, current_date)
)

SELECT
    pv.page_view_date::date AS reporting_date,
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
        ELSE 'Germany' END                                                                         AS country,
    LOWER(coalesce(pv.os_family, 'Others')) AS os,
    lower(COALESCE(pv.device_type, 'Other')) AS device,
    CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS new_recurring,
    COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
    CASE WHEN s.customer_id  IS NOT NULL THEN 1 
		ELSE 0 END AS is_customer_logged_in, -- IF the customer IS IN the NEXT steps, the customer should be logged
	CASE WHEN s.first_page_url ILIKE '%/business%' THEN 'business_customer'
		 ELSE 'normal_customer' END AS customer_type,
 	COUNT(distinct pv.session_id) AS traffic_daily_unique_sessions,
    COUNT(DISTINCT COALESCE(cu.anonymous_id_new,pv.anonymous_id)) AS traffic_daily_unique_users,
    COUNT(distinct
          CASE
              WHEN pv.page_view_in_session_index = 1
                  AND pv.user_bounced
                  THEN anonymous_id_new END) AS num_first_page_bounces,
    COUNT(DISTINCT CASE WHEN pv.page_type = 'pdp' THEN pv.page_view_id END) AS product_page_views ,
    COUNT(DISTINCT CASE WHEN pv.page_type = 'pdp' THEN pv.session_id END) AS sessions_with_product_page_views
FROM first_page_in_session pv
         LEFT JOIN traffic.sessions s
                   ON pv.session_id = s.session_id
         LEFT JOIN traffic.snowplow_user_mapping cu
                   ON pv.anonymous_id = cu.anonymous_id
                       AND pv.session_id = cu.session_id
WHERE pv.page_view_date::date BETWEEN DATEADD('day', -15, current_date) AND DATEADD('day', -1, current_date)
GROUP BY 1,2,3,4,5,6,7,8
;



DROP TABLE IF EXISTS tmp_conversion_funnel_daily_report_app_installed;
CREATE TEMP TABLE tmp_conversion_funnel_daily_report_app_installed
SORTKEY(reporting_date, os, device, country, new_recurring, marketing_channel)
DISTKEY(reporting_date)
AS
SELECT 
	ai."timestamp"::date AS reporting_date,
	COALESCE(
		CASE WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name END,	
		CASE
	        WHEN ai.context_locale ILIKE '%-nl' THEN 'Netherlands'
	        WHEN ai.context_locale ILIKE '%-es' THEN 'Spain'
	        WHEN ai.context_locale ILIKE '%-at' THEN 'Austria'
	        WHEN ai.context_locale ILIKE '%-us' THEN 'United States'
	        WHEN ai.context_locale ILIKE '%-de' THEN 'Germany'
	        ELSE 'Germany' END)  AS country,
	COALESCE(NULLIF(lower(dm.device_platform), 'web'),lower(ai.context_device_type), 'others') AS os,
	CASE WHEN ai.context_device_model ILIKE '%ipad%'
        OR ai.context_device_model ILIKE '%TB-%'
        OR ai.context_device_model ILIKE '%pad%'
        OR ai.context_device_model ILIKE '%sm-x%'
        OR ai.context_device_model ILIKE '%sm-t%'
        OR ai.context_device_model ILIKE '%tablet%'
        OR ai.context_device_model ILIKE '%tab%'
             THEN 'tablet' ELSE 'mobile' END AS device,
	'NEW'::varchar AS new_recurring,
	'Direct'::varchar AS marketing_channel,
	CASE WHEN ai.user_id IS NOT NULL THEN 1 
		 WHEN s.customer_id IS NOT NULL THEN 1 
		ELSE 0 END AS is_customer_logged_in,
	CASE WHEN COALESCE(s.store_name,'n/a') ILIKE '%b2b%' THEN 'business_customer'
		 ELSE 'normal_customer' END AS customer_type,
	count(DISTINCT ai.context_actions_amplitude_session_id) AS app_installed
FROM react_native.application_installed ai
LEFT JOIN traffic.sessions s 
	ON ai.context_actions_amplitude_session_id = s.session_id
LEFT JOIN segment.device_mapping dm 
	ON dm.session_id = ai.context_actions_amplitude_session_id
WHERE ai."timestamp"::date BETWEEN DATEADD('day', -15, current_date) AND DATEADD('day', -1, current_date)
GROUP BY 1,2,3,4,5,6,7,8
;


DROP TABLE IF EXISTS tmp_conversion_funnel_daily_report_traffic;
CREATE TEMP TABLE tmp_conversion_funnel_daily_report_traffic
SORTKEY(reporting_date, os, device, country, new_recurring, marketing_channel)
DISTKEY(reporting_date)
AS
SELECT DISTINCT
   COALESCE(o.reporting_date::date,p.reporting_date::date) AS reporting_date,
   COALESCE(o.country,p.country) AS country,
   COALESCE(o.os,p.os) AS os,
   COALESCE(o.device,p.device) AS device,
   COALESCE(o.new_recurring,p.new_recurring) AS new_recurring,
   COALESCE(o.marketing_channel,p.marketing_channel) AS marketing_channel,
   COALESCE(o.is_customer_logged_in, p.is_customer_logged_in) AS is_customer_logged_in,
   COALESCE(o.customer_type, p.customer_type) AS customer_type,
   COALESCE(o.traffic_daily_unique_sessions,0) AS traffic_daily_unique_sessions,
   COALESCE(o.traffic_daily_unique_users,0) AS traffic_daily_unique_users,
   COALESCE(o.num_first_page_bounces,0) AS num_first_page_bounces, 
   COALESCE(o.product_page_views,0) AS product_page_views, 
   COALESCE(o.sessions_with_product_page_views,0) AS sessions_with_product_page_views,
   COALESCE(p.app_installed,0) AS app_installed
FROM tmp_conversion_funnel_daily_report_page_view o
  FULL JOIN tmp_conversion_funnel_daily_report_app_installed p
    ON o.reporting_date = p.reporting_date
    AND o.country = p.country
    AND o.os = p.os
    AND o.device = p.device
    AND o.new_recurring = p.new_recurring
    AND o.marketing_channel = p.marketing_channel  
    AND o.is_customer_logged_in = p.is_customer_logged_in
    AND o.customer_type = p.customer_type
;


BEGIN TRANSACTION;

DELETE FROM dm_marketing.conversion_funnel_daily_report
USING tmp_conversion_funnel_daily_report_traffic tmp
WHERE conversion_funnel_daily_report.reporting_date::date = tmp.reporting_date::date;

INSERT INTO dm_marketing.conversion_funnel_daily_report
SELECT * 
FROM tmp_conversion_funnel_daily_report_traffic;

END TRANSACTION;


DROP TABLE IF EXISTS tmp_conversion_funnel_daily_report;
CREATE TEMP TABLE tmp_conversion_funnel_daily_report
SORTKEY(reporting_date, os, device, country, new_recurring, marketing_channel)
DISTKEY(reporting_date)
AS
SELECT 
    COALESCE(o.reporting_date::date,p.reporting_date::date,so.reporting_date::date) AS reporting_date,
    COALESCE(o.country,p.country,so.country) AS country,
    COALESCE(o.os,p.os,so.os) AS os,
    COALESCE(o.device,p.device,so.device) AS device,
    COALESCE(o.new_recurring,p.new_recurring,so.new_recurring) AS new_recurring,
    COALESCE(o.marketing_channel,p.marketing_channel,so.marketing_channel) AS marketing_channel,
    COALESCE(o.is_customer_logged_in, p.is_customer_logged_in,so.is_customer_logged_in) AS is_customer_logged_in,
    COALESCE(o.customer_type, p.customer_type, so.customer_type) AS customer_type,

    COALESCE(o.traffic_daily_unique_sessions,0) AS traffic_daily_unique_sessions,
    COALESCE(o.traffic_daily_unique_users,0) AS traffic_daily_unique_users,
    COALESCE(o.num_first_page_bounces,0) AS num_first_page_bounces,
    COALESCE(o.product_page_views,0) AS product_page_views,
    COALESCE(o.sessions_with_product_page_views,0) AS sessions_with_product_page_views,
    COALESCE(o.app_installed ,0) AS app_installed,
    --
    COALESCE(p.cart_orders,0) AS cart_orders,
    COALESCE(so.submitted_orders,0) AS submitted_orders,
    COALESCE(so.approved_orders,0) AS approved_orders,
    COALESCE(so.paid_orders,0) AS paid_orders,
    --
    COALESCE(p.cart_page_conversion ,0) AS cart_page_conversion,
    COALESCE(p.address_conversion , 0) AS address_conversion,
    COALESCE(p.payment_conversion , 0) AS payment_conversion,
    COALESCE(p.submitted_orders_conversion,0) AS submitted_orders_conversion,
    COALESCE(p.payment_or_address_conversion , 0) AS payment_or_address_conversion
FROM dm_marketing.conversion_funnel_daily_report o
         FULL JOIN tmp_conversion_funnel_daily_report_cart_orders p
                   ON o.reporting_date::date = p.reporting_date::date
                       AND o.country = p.country
                       AND o.os = p.os
                       AND o.device = p.device
                       AND o.new_recurring = p.new_recurring
                       AND o.marketing_channel = p.marketing_channel
                       AND o.is_customer_logged_in = p.is_customer_logged_in
                       AND o.customer_type = p.customer_type
         FULL JOIN tmp_conversion_funnel_daily_report_submitted_orders so
                   ON COALESCE(p.reporting_date::date, o.reporting_date::date)  = so.reporting_date::date
                       AND COALESCE(p.country, o.country) = so.country
                       AND COALESCE(p.os, o.os) = so.os
                       AND COALESCE(p.device, o.device) = so.device
                       AND COALESCE(p.new_recurring, o.new_recurring) = so.new_recurring
                       AND COALESCE(p.marketing_channel, o.marketing_channel) = so.marketing_channel
                       AND COALESCE(p.is_customer_logged_in, o.is_customer_logged_in) = so.is_customer_logged_in
                       AND COALESCE(p.customer_type, o.customer_type) = so.customer_type
WHERE COALESCE(o.reporting_date::date,p.reporting_date::date, so.reporting_date::date)
BETWEEN DATEADD('month', -13, current_date) AND DATEADD('day', -1, current_date);

BEGIN TRANSACTION;

DROP TABLE IF EXISTS dm_marketing.conversion_funnel_daily_report;
CREATE TABLE dm_marketing.conversion_funnel_daily_report AS
	SELECT
	    *
	FROM tmp_conversion_funnel_daily_report;
 
END TRANSACTION;

GRANT ALL ON ALL TABLES IN SCHEMA MARKETING TO GROUP BI;
GRANT select ON ALL TABLES IN SCHEMA dm_marketing TO redash_growth;

GRANT SELECT ON dm_marketing.conversion_funnel_daily_report TO tableau;
