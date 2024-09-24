DROP TABLE IF EXISTS segment.page_views_app;
CREATE TABLE segment.page_views_app
    DISTKEY(session_id)
    SORTKEY(page_view_start) AS

WITH remove_duplicates AS (
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
    WHERE page_info <> '' AND timestamp >= '2023-07-14'
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
    DENSE_RANK() OVER (PARTITION BY a.anonymous_id order by a.context_actions_amplitude_session_id) AS session_index,
    a.id AS page_view_id,
    ROW_NUMBER() OVER (PARTITION BY a.anonymous_id ORDER BY a.timestamp) AS page_view_index,
    ROW_NUMBER() OVER (PARTITION BY a.context_actions_amplitude_session_id ORDER BY a.timestamp) AS page_view_in_session_index,
    TO_CHAR(CONVERT_TIMEZONE('UTC', 'UTC', a.timestamp), 'YYYY-MM-DD') AS page_view_date,
    CONVERT_TIMEZONE('UTC', 'UTC', a.timestamp) AS page_view_start,
    CONVERT_TIMEZONE('UTC', 'UTC', LEAD(a.timestamp) OVER(partition by a.context_actions_amplitude_session_id order by a.timestamp)) AS page_view_end,
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
    context_device_manufacturer AS device, 
    CASE WHEN context_device_model ILIKE '%ipad%'
        OR context_device_model ILIKE '%TB-%'
        OR context_device_model ILIKE '%pad%'
        OR context_device_model ILIKE '%sm-x%'
        OR context_device_model ILIKE '%sm-t%'
        OR context_device_model ILIKE '%tablet%'
        OR context_device_model ILIKE '%tab%'
             THEN 'tablet' ELSE 'mobile' END AS device_type,
    CASE WHEN device_type = 'mobile' THEN TRUE ELSE FALSE END AS device_is_mobile
FROM react_native.screens a
    LEFT JOIN events_to_be_included b ON a.id = b.id
    LEFT JOIN segment.customer_mapping_app f ON a.id = f.id
    LEFT JOIN ods_production.store s ON a.store_id = s.id
WHERE (a.timestamp::DATE BETWEEN '2023-05-01' AND '2023-07-13') OR (a.timestamp::DATE >='2023-07-14' AND b.id IS NOT NULL);

DELETE FROM segment.page_views_app WHERE session_id IS NULL;

GRANT SELECT ON segment.page_views_app TO tableau;
GRANT SELECT ON segment.page_views_app TO hams;
