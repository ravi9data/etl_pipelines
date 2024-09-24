DROP TABLE IF EXISTS page_views_temp_table;
CREATE TEMP TABLE page_views_temp_table 
  DISTKEY(snowplow_user_id) 
  SORTKEY(page_view_start) AS 
WITH relevant_ids AS (
SELECT DISTINCT 
  domain_sessionid AS session_id
  ,domain_userid AS snowplow_user_id
FROM scratch.web_events AS a
  LEFT JOIN scratch.web_events_time AS b
    ON a.page_view_id = b.page_view_id
WHERE TRUE
  AND a.br_family != 'Robot/Spider'
  AND a.useragent NOT SIMILAR TO '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
  AND a.domain_userid IS NOT NULL -- rare edge CASE
  AND a.domain_sessionidx > 0 -- rare edge CASE
  AND b.min_tstamp::DATE >= DATEADD(WEEK, -1, CURRENT_DATE)
)
, last_page_view AS (
SELECT 
  pv.snowplow_user_id
  ,MAX(page_view_index) last_page_view_index
FROM web.page_views_snowplow pv 
  INNER JOIN relevant_ids ri
    ON ri.snowplow_user_id = pv.snowplow_user_id 
WHERE pv.page_view_date < DATEADD(WEEK, -1, CURRENT_DATE)
GROUP BY 1
)
, last_page_view_in_session AS (
SELECT 
  pv.session_id
  ,MAX(page_view_in_session_index) last_page_view_in_session_index
FROM web.page_views_snowplow pv 
  INNER JOIN relevant_ids ri
    ON ri.session_id = pv.session_id 
WHERE pv.page_view_date < DATEADD(WEEK, -1, CURRENT_DATE)
GROUP BY 1
)
SELECT
  a.root_id
 ,a.domain_userid AS snowplow_user_id
 ,a.user_id AS encoded_customer_id
 ,CASE 
   WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'UTC', b.min_tstamp), 'YYYY-MM-DD') >'2020-10-23' 
    THEN a.user_id
   WHEN TO_CHAR(CONVERT_TIMEZONE('UTC', 'UTC', b.min_tstamp), 'YYYY-MM-DD') >= '2020-10-22' 
     AND LEN(a.user_id)<10
    THEN a.user_id
   ELSE f.customer_id 
  END AS customer_id
 ,f.user_registration_date
 ,f.acquisition_date AS customer_acquisition_date
 ,f.customer_id_mapped
 ,a.domain_sessionid AS session_id
 ,a.domain_sessionidx AS session_index
 ,a.page_view_id
 ,COALESCE(li.last_page_view_index, 0) + 
   ROW_NUMBER() OVER (PARTITION BY a.domain_userid ORDER BY b.min_tstamp, a.page_view_id) AS page_view_index
 ,COALESCE(lis.last_page_view_in_session_index, 0) + 
   ROW_NUMBER() OVER (PARTITION BY a.domain_sessionid ORDER BY b.min_tstamp, a.page_view_id) AS page_view_in_session_index
 ,TO_CHAR(CONVERT_TIMEZONE('UTC', 'UTC', b.min_tstamp), 'YYYY-MM-DD') AS page_view_date
 ,CONVERT_TIMEZONE('UTC', 'UTC', b.min_tstamp) AS page_view_start
 ,CONVERT_TIMEZONE('UTC', 'UTC', b.max_tstamp) AS page_view_end
 ,CONVERT_TIMEZONE('UTC', a.os_timezone, b.min_tstamp) AS page_view_start_local
 ,CONVERT_TIMEZONE('UTC', a.os_timezone, b.max_tstamp) AS page_view_end_local
 ,CASE 
   WHEN customer_id_mapped IS NULL 
    THEN 'customer_not_registered'
   WHEN customer_id IS NOT NULL 
    THEN 'logged_in'
   WHEN customer_id IS NULL AND customer_id_mapped IS NOT NULL 
    THEN 'logged_out'
   ELSE NULL 
  END AS login_status
 ,b.time_engaged_in_s
 ,CASE
   WHEN b.time_engaged_in_s BETWEEN 0 AND 9 
    THEN '0s to 9s'
   WHEN b.time_engaged_in_s BETWEEN 10 AND 29 
    THEN '10s to 29s'
   WHEN b.time_engaged_in_s BETWEEN 30 AND 59 
    THEN '30s to 59s'
   WHEN b.time_engaged_in_s > 59 THEN '60s or more'
   ELSE NULL
  END AS time_engaged_in_s_tier
 ,c.vmax AS vertical_pixels_scrolled
 ,CASE
   WHEN b.time_engaged_in_s = 0 
    THEN '0% to 24%'
   WHEN c.relative_vmax BETWEEN 0 AND 24 
    THEN '0% to 24%'
   WHEN c.relative_vmax BETWEEN 25 AND 49 
    THEN '25% to 49%'
   WHEN c.relative_vmax BETWEEN 50 AND 74 
    THEN '50% to 74%'
   WHEN c.relative_vmax BETWEEN 75 AND 100 
    THEN '75% to 100%'
   ELSE NULL
  END AS vertical_percentage_scrolled_tier
 ,CASE 
   WHEN b.time_engaged_in_s = 0 
    THEN TRUE 
   ELSE FALSE 
  END AS user_bounced
 ,CASE 
   WHEN b.time_engaged_in_s >= 30 AND c.relative_vmax >= 25 
    THEN TRUE 
   ELSE FALSE 
  END AS user_engaged
 ,a.page_url
 ,a.page_urlpath
 ,a.page_title
 ,page.page_type
 ,page.page_type_detail
 ,c.doc_width AS page_width
 ,c.doc_height AS page_height
 ,store.store_id
 ,store.store_name AS store_name
 ,store.store_label AS store_label
 ,a.refr_urlhost || a.refr_urlpath AS referer_url
 ,a.refr_urlhost AS referer_url_host
 ,CASE
   WHEN a.refr_medium IS NULL 
    THEN 'direct'
   WHEN a.refr_medium = 'unknown' 
    THEN 'other'
   ELSE a.refr_medium
  END AS referer_medium
 ,a.refr_source AS referer_source
 ,a.refr_term AS referer_term
 ,a.mkt_medium AS marketing_medium
 ,a.mkt_source AS marketing_source
 ,a.mkt_term AS marketing_term
 ,a.mkt_content AS marketing_content
 ,a.mkt_campaign AS marketing_campaign
 ,a.mkt_clickid AS marketing_click_id
 ,a.mkt_network AS marketing_network
 ,a.geo_country
 ,a.geo_region_name
 ,a.geo_city
 ,a.geo_zipcode
 ,a.geo_latitude
 ,a.geo_longitude
 ,a.geo_timezone -- often NULL (use os_timezone instead)
 ,a.user_ipaddress AS ip_address
 ,a.ip_isp
 ,a.ip_organization
 ,a.ip_domain
 ,a.ip_netspeed AS ip_net_speed
 ,d.useragent_version AS browser
 ,CASE 
   WHEN a.br_family IN ('Chrome','Safari','Firefox','Microsoft Edge','
                         Opera','Apple WebKit','Internet Explorer') 
    THEN a.br_family 
   ELSE 'Others' 
  END AS browser_family
 ,a.br_lang AS browser_language
 ,a.platform
 ,d.os_version AS os
 ,CASE 
   WHEN a.os_family IN ('Windows','Android','iOS','Mac OS X','Linux') 
    THEN a.os_family 
   ELSE 'Others' 
  END AS os_family
 ,a.os_timezone
 ,d.device_family AS device
 ,CASE
   WHEN a.platform = 'app' 
    THEN 'App'
   WHEN a.dvce_type IN ('Computer','Mobile','Tablet') 
    THEN dvce_type 
   ELSE 'Other' 
  END AS device_type
 ,a.dvce_ismobile AS device_is_mobile
 ,e.redirect_time_in_ms
 ,e.unload_time_in_ms
 ,e.app_cache_time_in_ms
 ,e.dns_time_in_ms
 ,e.tcp_time_in_ms
 ,e.request_time_in_ms
 ,e.response_time_in_ms
 ,e.processing_time_in_ms
 ,e.dom_loading_to_interactive_time_in_ms
 ,e.dom_interactive_to_complete_time_in_ms
 ,e.onload_time_in_ms
 ,e.total_time_in_ms
FROM scratch.web_events AS a
  LEFT JOIN scratch.web_events_time AS b
    ON a.page_view_id = b.page_view_id
  LEFT JOIN scratch.web_events_scroll_depth AS c
    ON a.page_view_id = c.page_view_id
  LEFT JOIN scratch.web_ua_parser_context AS d
    ON a.page_view_id = d.page_view_id
  LEFT JOIN scratch.web_timing_context AS e
    ON a.page_view_id = e.page_view_id
  LEFT JOIN scratch.customer_mapping AS f
    ON a.page_view_id = f.page_view_id
  LEFT JOIN scratch.url_store_mapping store
    ON a.page_url = store.page_url
  LEFT JOIN scratch.url_page_type_mapping page
    ON a.page_url = page.page_url
  LEFT JOIN last_page_view  li
    ON a.domain_userid = li.snowplow_user_id
  LEFT JOIN last_page_view_in_session lis
    ON a.domain_sessionid  = lis.session_id
WHERE TRUE
  AND page_view_date >= DATEADD(WEEK, -1, CURRENT_DATE) 
  AND a.br_family != 'Robot/Spider'
  AND a.useragent NOT SIMILAR TO '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
  AND a.domain_userid IS NOT NULL -- rare edge CASE
  AND a.domain_sessionidx > 0 -- rare edge CASE
;

BEGIN TRANSACTION; 
DELETE FROM web.page_views_snowplow
USING page_views_temp_table tmp
WHERE page_views_snowplow.page_view_id = tmp.page_view_id
;

INSERT INTO web.page_views_snowplow 
SELECT *
FROM page_views_temp_table
;
END TRANSACTION;

DROP TABLE page_views_temp_table;