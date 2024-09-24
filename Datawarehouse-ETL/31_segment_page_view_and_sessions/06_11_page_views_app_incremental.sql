
CREATE TEMP TABLE tmp_segment_page_views_app AS
WITH 

old_sessions AS (
    SELECT DISTINCT 
        context_actions_amplitude_session_id::VARCHAR AS session_id,
        anonymous_id,
        id AS page_view_id
    FROM react_native.screens
WHERE timestamp::DATE < CURRENT_DATE - 3
),

last_page_view AS (
    SELECT 
        a.anonymous_id,
        MAX(a.page_view_index) AS last_page_view_index,
        MAX(a.session_index) AS last_session_index
    FROM segment.page_views_app a
        INNER JOIN old_sessions b ON a.anonymous_id = b.anonymous_id AND a.session_id = b.session_id
    GROUP BY 1
),
    
last_page_view_in_session AS (
    SELECT 
        a.session_id,
        MAX(a.page_view_in_session_index) AS last_page_view_in_session_index
    FROM segment.page_views_app a
        INNER JOIN old_sessions b ON a.session_id = b.session_id AND a.page_view_id = b.page_view_id
    GROUP BY 1
),

remove_duplicates AS (
    SELECT id,
           context_actions_amplitude_session_id,
           "timestamp" AS page_view_time,
           "search",
           sku,
           COALESCE(category, category_name) category,
           COALESCE(sub_category, subcategory_name) subcategory ,
           slug,
           page_type,
           COALESCE(slug,'') || COALESCE(page_type,'') || COALESCE(sku,'') AS page_info,
           ROW_NUMBER() OVER (PARTITION BY context_actions_amplitude_session_id, "timestamp" ORDER BY id) AS rowno
    FROM react_native.screens
    WHERE page_info <> '' AND "timestamp" >= CURRENT_DATE - 3
), 

cleaning AS (
    SELECT  id,
            lag(page_info) OVER (PARTITION BY context_actions_amplitude_session_id ORDER BY page_view_time) AS prev_page_info,
            page_info,
            slug,
            page_type,
            page_view_time,
            sku,
            category,
            subcategory,
            "search"
    FROM remove_duplicates
    WHERE rowno = 1
),

events_to_be_included AS (
SELECT id
FROM cleaning
WHERE page_info <> COALESCE(prev_page_info, '') --COALESCE TO INCLUDE the FIRST screen VIEW (in this case home)
)

SELECT
    NULL AS root_id,
    a.anonymous_id,
    NULL AS encoded_customer_id,
    a.user_id::INT AS customer_id,
    f.user_registration_date,
    f.customer_acquisition_date,
    f.customer_id AS customer_id_mapped,
    a.context_actions_amplitude_session_id::VARCHAR AS session_id,
    COALESCE(lp.last_session_index, 0) + DENSE_RANK() OVER (PARTITION BY a.anonymous_id order by a.context_actions_amplitude_session_id) AS session_index,
    a.id AS page_view_id,
    COALESCE(lp.last_page_view_index, 0) + ROW_NUMBER() OVER (PARTITION BY a.anonymous_id ORDER BY a.timestamp) AS page_view_index,
    COALESCE(lps.last_page_view_in_session_index, 0) + ROW_NUMBER() OVER (PARTITION BY a.context_actions_amplitude_session_id ORDER BY a.timestamp) AS page_view_in_session_index,
    TO_CHAR(CONVERT_TIMEZONE('UTC', 'UTC', a.timestamp), 'YYYY-MM-DD') AS page_view_date,
    CONVERT_TIMEZONE('UTC', 'UTC', a.timestamp) AS page_view_start,
    CONVERT_TIMEZONE('UTC', 'UTC', LEAD(a.timestamp) OVER(partition by a.context_actions_amplitude_session_id ORDER BY a.timestamp)) AS page_view_end,
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
    DATE_DIFF('second', a.timestamp,
              COALESCE( LEAD(a.timestamp) OVER(PARTITION BY a.context_actions_amplitude_session_id order by a.timestamp),a.timestamp)) AS time_engaged_in_s,
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
            AND (count(*) over (partition by a.context_actions_amplitude_session_id)) = 1
            AND page_view_in_session_index = 1
            THEN TRUE
        ELSE FALSE
        END AS user_bounced,
    CASE
        WHEN time_engaged_in_s >= 30
            THEN TRUE
        ELSE FALSE
        END AS user_engaged,
    null as page_url,
    a.path AS page_urlpath,
    a.page_type || ' - ' ||a.slug AS page_title,
    a.page_type,
    CASE
        WHEN page_type = 'category'
            THEN COALESCE(category, category_name)
        WHEN page_type = 'sub-category'
            THEN COALESCE(sub_category, subcategory_name)
        WHEN page_type = 'pdp'
            THEN product_sku
        ELSE slug
        END AS page_type_detail,
    context_screen_width AS page_width,
    context_screen_height AS page_height,
    CASE WHEN a.store_id = '-1' THEN 1 ELSE a.store_id END AS store_id,
    s.store_name AS store_name,
    s.store_label AS store_label,
    null AS referer_url,
    null AS referer_url_host,
    NULL AS referer_medium,
    NULL AS referer_source,
    NULL AS referer_term,
    NULL AS marketing_medium,
    NULL AS marketing_source,
    NULL AS marketing_term,
    NULL AS marketing_content,
    NULL AS marketing_campaign,
    NULL AS marketing_click_id,
    NULL AS marketing_network,
    NULL AS geo_country,
    NULL AS geo_region_name,
    NULL AS geo_city,
    NULL AS geo_zipcode,
    NULL AS geo_latitude,
    NULL AS geo_longitude,
    context_timezone AS geo_timezone,
    context_ip AS ip_address,
    'app' AS platform,
    context_device_type AS os_family,
    context_device_manufacturer AS device, --use user_agent,
    case when context_device_model ILIKE '%ipad%'
        OR context_device_model ILIKE '%TB-%'
        OR context_device_model ILIKE '%pad%'
        OR context_device_model ILIKE '%sm-x%'
        OR context_device_model ILIKE '%sm-t%'
        OR context_device_model ILIKE '%tablet%'
        OR context_device_model ILIKE '%tab%'
             THEN 'tablet' else 'mobile' end AS device_type,
    CASE WHEN device_type = 'mobile' THEN TRUE ELSE FALSE END AS device_is_mobile
FROM react_native.screens a
    INNER JOIN events_to_be_included b ON a.id = b.id
    LEFT JOIN segment.customer_mapping_app f ON a.id = f.id
    LEFT JOIN ods_production.store s ON a.store_id = s.id
    LEFT JOIN last_page_view lp ON a.anonymous_id = lp.anonymous_id
    LEFT JOIN last_page_view_in_session lps ON a.context_actions_amplitude_session_id = lps.session_id
WHERE timestamp::DATE >= CURRENT_DATE - 3 AND a.context_actions_amplitude_session_id IS NOT NULL;

BEGIN TRANSACTION; 

DELETE FROM segment.page_views_app
USING tmp_segment_page_views_app tmp
WHERE page_views_app.session_id = tmp.session_id AND page_views_app.page_view_date = tmp.page_view_date;

INSERT INTO segment.page_views_app 
SELECT *
FROM tmp_segment_page_views_app;

END TRANSACTION;

BEGIN TRANSACTION; 

DELETE FROM traffic.page_views 
USING tmp_segment_page_views_app tmp
WHERE page_views.session_id = tmp.session_id 
    AND page_views.page_view_date = tmp.page_view_date
    AND traffic_source = 'segment_app';

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
    'segment_app'::VARCHAR AS traffic_source
FROM tmp_segment_page_views_app
WHERE page_view_start::DATE >= '2023-05-01';

END TRANSACTION;

DROP TABLE IF EXISTS tmp_segment_page_views_app;
