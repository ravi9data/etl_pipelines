
CREATE TEMP TABLE tmp_segment_page_views_web AS
WITH 

old_sessions AS (
    SELECT DISTINCT 
        session_id::VARCHAR AS session_id,
        anonymous_id,
        event_id AS page_view_id
    FROM segment.page_events
WHERE loaded_at < CURRENT_DATE - 3
),

last_page_view AS (
    SELECT 
        a.anonymous_id,
        MAX(a.page_view_index) AS last_page_view_index,
        MAX(a.session_index) AS last_session_index
    FROM segment.page_views_web a
        INNER JOIN old_sessions b ON a.anonymous_id = b.anonymous_id AND a.session_id = b.session_id
    GROUP BY 1
),
    
last_page_view_in_session AS (
    SELECT 
        a.session_id,
        MAX(a.page_view_in_session_index) AS last_page_view_in_session_index
    FROM segment.page_views_web a
        INNER JOIN old_sessions b ON a.session_id = b.session_id AND a.page_view_id = b.page_view_id
    GROUP BY 1
)

SELECT
    NULL AS root_id,
    a.anonymous_id,
    NULL AS encoded_customer_id,
    NULLIF(a.user_id,'9999999999')::INT AS customer_id,
    f.user_registration_date,
    f.customer_acquisition_date,
    f.customer_id AS customer_id_mapped,
    a.session_id::VARCHAR AS session_id,
    COALESCE(lp.last_session_index, 0) + DENSE_RANK() OVER (PARTITION BY a.anonymous_id order by a.session_id) AS session_index,
    a.event_id AS page_view_id,
    COALESCE(lp.last_page_view_index, 0) + ROW_NUMBER() OVER (PARTITION BY a.anonymous_id ORDER BY a.event_time) AS page_view_index,
    COALESCE(lps.last_page_view_in_session_index, 0) + ROW_NUMBER() OVER (PARTITION BY a.session_id ORDER BY a.event_time) AS page_view_in_session_index,
    TO_CHAR(CONVERT_TIMEZONE('UTC', 'UTC', a.event_time), 'YYYY-MM-DD') AS page_view_date,
    CONVERT_TIMEZONE('UTC', 'UTC', a.event_time) AS page_view_start,
    CONVERT_TIMEZONE('UTC', 'UTC', LEAD(a.event_time) OVER(partition by a.session_id order by a.event_time)) AS page_view_end,
    NULL AS page_view_start_local,
    NULL AS page_view_end_local,
    CASE
        WHEN customer_id_mapped IS NULL
            THEN 'customer_not_registered'
        WHEN a.user_id IS NOT NULL
            THEN 'logged_in'
        WHEN a.user_id IS NULL AND customer_id_mapped IS NOT NULL
            THEN 'logged_out'
        ELSE NULL
        END AS login_status,
    DATE_DIFF('second', a.event_time, 
		COALESCE( LEAD(a.event_time) OVER(PARTITION BY a.session_id order by a.event_time),a.event_time)) AS time_engaged_in_s,
    CASE
        WHEN time_engaged_in_s BETWEEN 0 AND 9
            THEN '0s to 9s'
        WHEN time_engaged_in_s BETWEEN 10 AND 29
            THEN '10s to 29s'
        WHEN time_engaged_in_s BETWEEN 30 AND 59
            THEN '30s to 59s'
        WHEN time_engaged_in_s > 59 THEN '60s or more'
        ELSE NULL
        END AS time_engaged_in_s_tier,
    NULL AS vertical_pixels_scrolled,
    NULL AS vertical_percentage_scrolled_tier,
    CASE
        WHEN time_engaged_in_s = 0
        	AND (count(*) over (partition by a.session_id)) = 1
        	AND page_view_in_session_index = 1
            THEN TRUE
        ELSE FALSE
        END AS user_bounced,
    CASE
        WHEN time_engaged_in_s >= 30
            THEN TRUE
        ELSE FALSE
        END AS user_engaged,
    a.page_url,
    a.page_path AS page_urlpath,
    a.page_title,
    a.page_type,
      CASE 
        WHEN page_type = 'landing-page' THEN split_part(a.page_path,'/',4) 
        WHEN page_type = 'category' THEN
                CASE WHEN REPLACE(JSON_EXTRACT_PATH_TEXT(a.properties,'slug'), '-', '') IN 
                            (SELECT DISTINCT lower(REPLACE(REPLACE(REPLACE(category_name, ' ', ''), '&', 'and'), '-', '')) FROM master.subscription WHERE category_name IS NOT NULL)
                        THEN INITCAP(REGEXP_REPLACE(JSON_EXTRACT_PATH_TEXT(a.properties,'slug'), '-', ' '))
                     WHEN REPLACE(SPLIT_PART(a.page_path,'/',3), '-', '') IN 
                            (SELECT DISTINCT lower(REPLACE(REPLACE(REPLACE(category_name, ' ', ''), '&', 'and'), '-', '')) FROM master.subscription WHERE category_name IS NOT NULL)
                        THEN INITCAP(REGEXP_REPLACE(SPLIT_PART(a.page_path,'/',3), '-', ' '))
                END
        WHEN page_type = 'sub-category' THEN
                CASE WHEN JSON_EXTRACT_PATH_TEXT(a.properties,'slug') = 'computer-accessories' OR SPLIT_PART(a.page_path,'/',4) = 'computer-accessories' THEN 'Computing Accessories'
                     WHEN JSON_EXTRACT_PATH_TEXT(a.properties,'slug') = 'vacuumsmops' OR SPLIT_PART(a.page_path,'/',4) = 'vacuumsmops' THEN 'Vacuum And Mops'
                     WHEN JSON_EXTRACT_PATH_TEXT(a.properties,'slug') = 'coffee-machine' OR SPLIT_PART(a.page_path,'/',4) = 'coffee-machine' THEN 'Coffee Machines'
                     WHEN REPLACE(JSON_EXTRACT_PATH_TEXT(a.properties,'slug'), '-', '') IN
                            (SELECT DISTINCT lower(REPLACE(REPLACE(REPLACE(subcategory_name, ' ', ''), '&', 'and'), '-', '')) FROM master.subscription WHERE subcategory_name IS NOT NULL)
                        THEN INITCAP(REGEXP_REPLACE(JSON_EXTRACT_PATH_TEXT(a.properties,'slug'), '-', ' '))
                     WHEN REPLACE(SPLIT_PART(a.page_path,'/',4), '-', '') IN 
                            (SELECT DISTINCT lower(REPLACE(REPLACE(REPLACE(subcategory_name, ' ', ''), '&', 'and'), '-', '')) FROM master.subscription WHERE subcategory_name IS NOT NULL)
                        THEN INITCAP(REGEXP_REPLACE(SPLIT_PART(a.page_path,'/',4), '-', ' '))
                END 
        WHEN page_type = 'pdp' THEN JSON_EXTRACT_PATH_TEXT(a.properties,'product_sku') 
    END AS page_type_detail,
    NULL AS page_width,
    NULL AS page_height,
    COALESCE(a.store_id, s.store_id) AS store_id,
    s.store_name AS store_name,
    s.store_label AS store_label,
    a.page_referrer AS referer_url,
    split_part(a.page_referrer,'/',1) AS referer_url_host,
    NULL AS referer_medium,
    NULL AS referer_source,
    NULL AS referer_term,
    CASE WHEN LEN(a.marketing_medium) >= 255 THEN NULL ELSE SPLIT_PART(a.marketing_medium,'%26',1) END AS marketing_medium,
    CASE WHEN LEN(a.marketing_source) >= 255 THEN NULL ELSE SPLIT_PART(a.marketing_source,'%26',1) END AS marketing_source,
    CASE WHEN LEN(a.marketing_term) >= 255 THEN NULL ELSE SPLIT_PART(a.marketing_term,'%26',1) END AS marketing_term,
    CASE WHEN LEN(a.marketing_content) >= 500 THEN NULL ELSE SPLIT_PART(a.marketing_content,'%26',1) END AS marketing_content,
    CASE WHEN LEN(a.marketing_campaign) >= 6000 THEN NULL ELSE SPLIT_PART(a.marketing_campaign,'%26',1) END AS marketing_campaign,
    a.click_id AS marketing_click_id,
    NULL AS marketing_network,
    NULL AS geo_country,
    NULL AS geo_region_name,
    NULL AS geo_city,
    NULL AS geo_zipcode,
    NULL AS geo_latitude,
    NULL AS geo_longitude,
    NULL AS geo_timezone,
    a.ip AS ip_address,
    NULL AS ip_isp,
    NULL AS ip_organization,
    NULL AS ip_domain,
    NULL AS ip_net_speed,
    NULL AS browser, --use user_agent,
    NULL AS browser_family, --use user_agent,
    NULL AS browser_language,
    CASE 
      WHEN dm.device_platform = 'web' 
        THEN 'web' 
      WHEN dm.device_platform IN ('ios', 'android') 
        THEN 'app' 
      ELSE NULL END AS platform,
    NULL AS os, --use user_agent,
    dm.device_platform AS os_family, --we don't have actuall os_family here, but this is what we receive from segment and it is enough to do mapping that we need
    NULL AS os_timezone,
    dm.device_brand AS device, --use user_agent,
    dm.device_type AS device_type, 
    CASE WHEN dm.device_type = 'mobile' THEN TRUE ELSE FALSE END AS device_is_mobile,
    NULL AS redirect_time_in_ms,
    NULL AS unload_time_in_ms,
    NULL AS app_cache_time_in_ms,
    NULL AS dns_time_in_ms,
    NULL AS tcp_time_in_ms,
    NULL AS request_time_in_ms,
    NULL AS response_time_in_ms,
    NULL AS processing_time_in_ms,
    NULL AS dom_loading_to_interactive_time_in_ms,
    NULL AS dom_interactive_to_complete_time_in_ms,
    NULL AS onload_time_in_ms,
    NULL AS total_time_in_ms
FROM segment.page_events a
    LEFT JOIN segment.customer_mapping_web f ON a.event_id = f.event_id
    LEFT JOIN segment.url_store_mapping s ON a.page_url = s.page_url
    LEFT JOIN segment.device_mapping dm ON dm.session_id = a.session_id
    LEFT JOIN last_page_view lp ON a.anonymous_id = lp.anonymous_id
    LEFT JOIN last_page_view_in_session lps ON a.session_id = lps.session_id
WHERE a.loaded_at >= CURRENT_DATE -3 
    AND a.user_agent NOT ILIKE '%datadog%';

BEGIN TRANSACTION; 

DELETE FROM segment.page_views_web 
USING tmp_segment_page_views_web tmp
WHERE page_views_web.page_view_id = tmp.page_view_id;

INSERT INTO segment.page_views_web 
SELECT *
FROM tmp_segment_page_views_web;

END TRANSACTION;

BEGIN TRANSACTION; 

DELETE FROM traffic.page_views 
USING tmp_segment_page_views_web tmp
WHERE page_views.page_view_id = tmp.page_view_id;

INSERT INTO traffic.page_views
SELECT 
    root_id,
    anonymous_id,
    encoded_customer_id,
    customer_id::VARCHAR,
    user_registration_date,
    customer_acquisition_date,
    customer_id_mapped::VARCHAR,
    session_id::varchar,
    -- session_index,
    page_view_id,
    -- page_view_index,
    -- page_view_in_session_index,
    page_view_date,
    page_view_start,
    page_view_end,
    page_view_start_local::timestamp,
    page_view_end_local::timestamp,
    login_status,
    time_engaged_in_s,
    time_engaged_in_s_tier,
    vertical_pixels_scrolled::int,
    vertical_percentage_scrolled_tier,
    user_bounced,
    user_engaged,
    page_url,
    page_urlpath,
    page_title,
    page_type,
    page_type_detail,
    page_width::int,
    page_height::int,
    store_id,
    store_name,
    store_label,
    referer_url,
    referer_url_host,
    referer_medium,
    referer_source,
    referer_term,
    marketing_medium,
    marketing_source,
    marketing_term,
    marketing_content,
    marketing_campaign,
    marketing_click_id,
    marketing_network,
    geo_country,
    geo_region_name,
    geo_city,
    geo_zipcode,
    geo_latitude::float,
    geo_longitude::float,
    geo_timezone,
    ip_address,
    platform,
    os_family,
    device,
    device_type,
    device_is_mobile::bool,
    'segment_web'::VARCHAR AS traffic_source
FROM tmp_segment_page_views_web
WHERE page_view_start::DATE >= '2023-05-01';

END TRANSACTION;

DROP TABLE IF EXISTS tmp_segment_page_views_web;
