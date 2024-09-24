
CREATE TEMP TABLE tmp_segment_all_events AS
SELECT
    anonymous_id,
    NULLIF(user_id,'9999999999')::INT AS user_id,
    session_id::VARCHAR AS session_id,
    event_id,
    event_name,
    event_type,
    event_time,
    CASE WHEN app_name = 'Grover' THEN 'app' ELSE 'web' END AS platform,
    device_id,
    device_manufacturer,
    device_model,
    device_name,
    device_type,
    ip,
    locale,
    network_cellular,
    network_wifi,
    network_bluetooth,
    network_carrier,
    os_name,
    os_version,
    screen_density,
    screen_height,
    screen_width,
    timezone,
    nullif(marketing_content,'_') AS marketing_content,
    nullif(marketing_medium,'_') AS marketing_medium,
    nullif(marketing_campaign,'_') AS marketing_campaign,
    nullif(marketing_source,'_') AS marketing_source,
    nullif(marketing_term,'_') AS marketing_term,
    nullif(traits_content,'_') AS traits_content,
    nullif(traits_medium,'_') AS traits_medium,
    nullif(traits_campaign,'_') AS traits_campaign,
    nullif(traits_source,'_') AS traits_source,
    nullif(traits_term,'_') AS traits_term,
    click_id,
    user_agent,
    page_path,
    page_referrer,
    page_search,
    page_title,
    page_url,
    NULL AS page_name,
    NULL AS page_type,
    store_id,
    loaded_at::DATE AS loaded_at
FROM segment.identify_events
WHERE (user_agent NOT ILIKE '%datadog%' OR user_agent IS NULL)
AND loaded_at >= current_date - 1

UNION ALL

SELECT
    anonymous_id,
    NULLIF(user_id,'9999999999')::INT AS user_id,
    session_id::VARCHAR AS session_id,
    event_id,
    event_name,
    event_type,
    event_time,
    CASE WHEN app_name = 'Grover' THEN 'app' ELSE 'web' END AS platform,
    device_id,
    device_manufacturer,
    device_model,
    device_name,
    device_type,
    ip,
    locale,
    network_cellular,
    network_wifi,
    network_bluetooth,
    network_carrier,
    os_name,
    os_version,
    screen_density,
    screen_height,
    screen_width,
    timezone,
    nullif(marketing_content,'_') AS marketing_content,
    nullif(marketing_medium,'_') AS marketing_medium,
    nullif(marketing_campaign,'_') AS marketing_campaign,
    nullif(marketing_source,'_') AS marketing_source,
    nullif(marketing_term,'_') AS marketing_term,
    nullif(traits_content,'_') AS traits_content,
    nullif(traits_medium,'_') AS traits_medium,
    nullif(traits_campaign,'_') AS traits_campaign,
    nullif(traits_source,'_') AS traits_source,
    nullif(traits_term,'_') AS traits_term,
    click_id,
    user_agent,
    page_path,
    page_referrer,
    page_search,
    page_title,
    page_url,
    NULL AS page_name,
    NULL AS page_type,
    store_id,
    loaded_at::DATE AS loaded_at
FROM segment.track_events
WHERE (user_agent NOT ILIKE '%datadog%' OR user_agent IS NULL)
AND loaded_at >= current_date - 1

UNION ALL

SELECT
    anonymous_id,
    NULLIF(user_id,'9999999999')::INT AS user_id,
    session_id::VARCHAR AS session_id,
    event_id,
    event_name,
    event_type,
    event_time,
    'web' AS platform,
    NULL AS device_id,
    NULL AS device_manufacturer,
    NULL AS device_model,
    NULL AS device_name,
    NULL AS device_type,
    ip,
    locale,
    NULL AS network_cellular,
    NULL AS network_wifi,
    NULL AS network_bluetooth,
    NULL AS network_carrier,
    NULL AS os_name,
    NULL AS os_version,
    NULL AS screen_density,
    NULL AS screen_height,
    NULL AS screen_width,
    NULL AS timezone,
    nullif(marketing_content,'_') AS marketing_content,
    nullif(marketing_medium,'_') AS marketing_medium,
    nullif(marketing_campaign,'_') AS marketing_campaign,
    nullif(marketing_source,'_') AS marketing_source,
    nullif(marketing_term,'_') AS marketing_term,
    nullif(traits_content,'_') AS traits_content,
    nullif(traits_medium,'_') AS traits_medium,
    nullif(traits_campaign,'_') AS traits_campaign,
    nullif(traits_source,'_') AS traits_source,
    nullif(traits_term,'_') AS traits_term,
    click_id,
    user_agent,
    page_path,
    page_referrer,
    page_search,
    page_title,
    page_url,
    page_name,
    page_type,
    store_id,
    loaded_at::DATE AS loaded_at
FROM segment.page_events
WHERE (user_agent NOT ILIKE '%datadog%' OR user_agent IS NULL)
AND loaded_at >= current_date - 1;

BEGIN TRANSACTION; 

DELETE FROM segment.all_events 
USING tmp_segment_all_events tmp
WHERE all_events.event_id = tmp.event_id;

INSERT INTO segment.all_events
SELECT *
FROM tmp_segment_all_events;

END TRANSACTION;
