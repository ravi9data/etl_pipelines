CREATE TEMP TABLE order_conversions_temp AS
WITH subcategories AS (
SELECT DISTINCT 
  order_id
 ,subcategory_name 
FROM ods_production.order_item
), 

orders AS (
SELECT DISTINCT order_id
FROM web.session_order_mapping_snowplow som 
WHERE session_start::DATE >= DATEADD(WEEK, -2, CURRENT_DATE)  
),
last_mkt_data_non_direct AS ( 
  SELECT 
    order_id,
    MAX(session_rank_order_excl_direct) max_session_rank_order_excl_direct
  FROM web.session_order_mapping_snowplow
  GROUP BY 1
)
SELECT 
  m.order_id 
 ,LISTAGG(DISTINCT m.geo_city, ' -> ') 
   WITHIN GROUP (ORDER BY m.session_rank_order) AS geo_cities
 ,COALESCE(CASE 
   WHEN REGEXP_COUNT(geo_cities,' -> ') IS NULL 
    THEN NULL 
   ELSE REGEXP_COUNT(geo_cities,' -> ') + 1 
  END ,0) AS no_of_geo_cities
 ,COALESCE(SUM(ef.mietkauf_tooltip_events) ,0) AS mietkauf_tooltip_events
 ,COALESCE(SUM(ef.failed_delivery_tracking_events) ,0) AS failed_delivery_tracking_events
 ,MIN(m.session_start) AS min_session_start
 ,COALESCE(SUM(CASE 
   WHEN POSITION(oi.subcategory_name IN COALESCE(l.search_click_subcategories,'')) > 0
    THEN ef.search_enter 
  END) ,0) AS search_enter_events
 ,MIN(CASE 
   WHEN POSITION(oi.subcategory_name IN COALESCE(l.search_click_subcategories,'')) > 0
    THEN ef.min_search_enter_timestamp 
  END) AS min_search_enter_timestamp
 ,COALESCE(SUM(CASE 
   WHEN POSITION(oi.subcategory_name IN COALESCE(l.search_click_subcategories,'')) > 0
    THEN ef.search_product_click + ef.search_cta_click 
  END) ,0) AS search_confirm_events
 ,COALESCE(SUM(CASE 
   WHEN POSITION(oi.subcategory_name IN COALESCE(l.search_click_subcategories,'')) > 0
     THEN ef.search_exit 
  END) ,0) AS search_exit_events
 ,COALESCE(MAX(ef.availability_service) ,0) AS availability_service
 ,MAX(m.session_count_order) AS touchpoints
 ,MAX(CASE 
   WHEN m.first_touchpoint_30d
    THEN m.marketing_channel 
  END) AS first_touchpoint_30d
 ,MAX(CASE 
   WHEN m.first_touchpoint
    THEN m.marketing_channel 
  END) AS first_touchpoint
 ,MAX(CASE 
   WHEN m.session_rank_order = m.session_count_order 
    THEN m.marketing_channel 
  END) AS last_touchpoint
 ,COALESCE(MAX(CASE 
   WHEN m.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct 
    THEN m.marketing_channel 
  END), last_touchpoint) AS last_touchpoint_excl_direct
 ,MAX(CASE 
   WHEN m.session_rank_order = 1 
    THEN m.marketing_source 
  END) AS first_touchpoint_mkt_source
 ,MAX(CASE 
   WHEN m.first_touchpoint_30d
    THEN m.marketing_source 
  END) AS first_touchpoint_30d_mkt_source
 ,MAX(CASE 
   WHEN m.session_rank_order = m.session_count_order 
    THEN m.marketing_source 
  END) AS last_touchpoint_mkt_source
 ,COALESCE(MAX(CASE 
   WHEN m.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct 
    THEN m.marketing_source 
  END),'n/a') AS last_touchpoint_excl_direct_mkt_source
 ,MAX(CASE 
   WHEN m.session_rank_order = 1 
    THEN m.marketing_medium 
  END) AS first_touchpoint_mkt_medium
 ,MAX(CASE 
   WHEN m.first_touchpoint_30d
    THEN m.marketing_medium 
  END) AS first_touchpoint_30d_mkt_medium
 ,MAX(CASE 
   WHEN m.session_rank_order = m.session_count_order 
    THEN m.marketing_medium 
  END) AS last_touchpoint_mkt_medium
 ,COALESCE(MAX(CASE 
   WHEN m.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct 
    THEN m.marketing_medium 
  END),'n/a') AS last_touchpoint_excl_direct_mkt_medium
 ,COALESCE(MAX(CASE 
   WHEN m.last_touchpoint_before_submitted
    THEN m.os 
  END),'n/a') AS os
 ,COALESCE(MAX(CASE 
   WHEN m.last_touchpoint_before_submitted
    THEN m.browser
  END),'n/a') AS browser
 ,MAX(CASE 
   WHEN m.last_touchpoint_before_submitted
    THEN l.checkout_flow 
  END) AS last_touchpoint_checkout_flow  
 ,LISTAGG(DISTINCT m.marketing_channel, ' -> ') 
   WITHIN GROUP (ORDER BY m.session_rank_order) AS customer_journey
 ,LISTAGG(DISTINCT m.marketing_channel, ' -> ') 
   WITHIN GROUP (ORDER BY m.session_rank_order) AS unique_customer_journey
 ,LISTAGG(DISTINCT l.checkout_flow, ' -> ') 
   WITHIN GROUP (ORDER BY m.session_rank_order) AS customer_journey_checkout_flow
 ,COALESCE(MAX(CASE 
   WHEN m.last_touchpoint_before_submitted
    THEN m.device_type 
  END),'n/a') AS last_touchpoint_device_type
 ,COALESCE(MAX(CASE 
   WHEN m.last_touchpoint_before_submitted
    THEN s.referer_url  
  END),'n/a') AS last_touchpoint_referer_url 
 ,COALESCE(MAX(CASE 
   WHEN m.last_touchpoint_before_submitted
    THEN s.marketing_campaign  
  END),'n/a') AS last_touchpoint_marketing_campaign
 ,COALESCE(MAX(CASE 
   WHEN m.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct  
    THEN s.marketing_campaign  
  END),'n/a') AS last_touchpoint_excl_direct_marketing_campaign   
 ,COALESCE(MAX(CASE 
   WHEN m.session_rank_order_excl_direct = lm.max_session_rank_order_excl_direct  
    THEN s.marketing_content 
  END),'n/a') AS last_touchpoint_excl_direct_marketing_content    
FROM web.session_order_mapping_snowplow m 
  INNER JOIN orders o 
    ON m.order_id = o.order_id
  LEFT JOIN subcategories oi 
    ON m.order_id = oi.order_id
  LEFT JOIN web.session_events_features ef 
    ON m.session_id = ef.session_id
  LEFT JOIN web.session_events_lists l 
    ON m.session_id = l.session_id
  LEFT JOIN web.sessions_snowplow s 
    ON m.session_id = s.session_id
  LEFT JOIN last_mkt_data_non_direct lm 
    ON m.order_id = lm.order_id
GROUP BY 1
;

BEGIN TRANSACTION;

DELETE FROM web.order_conversions_snowplow 
USING order_conversions_temp b
WHERE order_conversions_snowplow.order_id = b.order_id;

INSERT INTO web.order_conversions_snowplow 
SELECT * 
FROM order_conversions_temp;

END TRANSACTION;

DROP TABLE order_conversions_temp;
