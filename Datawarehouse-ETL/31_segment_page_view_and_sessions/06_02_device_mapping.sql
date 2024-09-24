DROP TABLE IF EXISTS segment.device_mapping;
CREATE TABLE segment.device_mapping AS
WITH get_data AS (
    SELECT session_id,
           event_time,
           user_agent,
           device_type,
           device_manufacturer,
           CASE WHEN IS_VALID_JSON(traits) then NULLIF(LOWER(JSON_EXTRACT_PATH_TEXT(traits,'device_brand')),'') END AS traits_device_brand,
           CASE WHEN IS_VALID_JSON(traits) then NULLIF(LOWER(JSON_EXTRACT_PATH_TEXT(traits,'device_platform')),'') END AS traits_device_platform,
           CASE WHEN IS_VALID_JSON(traits) then NULLIF(LOWER(JSON_EXTRACT_PATH_TEXT(traits,'device_type')),'') END AS traits_device_type
    FROM segment.identify_events
    WHERE loaded_at >= '2023-05-01'

    UNION ALL

    SELECT session_id,
           event_time,
           user_agent,
           device_type,
           device_manufacturer,
           CASE WHEN IS_VALID_JSON(traits) then NULLIF(LOWER(JSON_EXTRACT_PATH_TEXT(traits,'device_brand')),'') END AS traits_device_brand,
           CASE WHEN IS_VALID_JSON(traits) then NULLIF(LOWER(JSON_EXTRACT_PATH_TEXT(traits,'device_platform')),'') END AS traits_device_platform,
           CASE WHEN IS_VALID_JSON(traits) then NULLIF(LOWER(JSON_EXTRACT_PATH_TEXT(traits,'device_type')),'') END AS traits_device_type
    FROM segment.track_events
    WHERE loaded_at >= '2023-05-01'
),

    mapping AS (
    SELECT
        session_id,
        event_time,
        user_agent,
        CASE
            WHEN user_agent ILIKE '%wv%'
                THEN
                CASE
                    WHEN user_agent ILIKE '%ios%'
                        THEN 'ios'
                    WHEN user_agent ILIKE '%iphone%'
                        THEN 'ios'
                    ELSE 'android'
                    END
            ELSE 'web'
            END AS device_platform_case,
        COALESCE(device_type, device_platform_case) AS event_device_platform,
        device_manufacturer AS event_device_brand,
        CASE
            WHEN (user_agent ilike '%Windows NT%'
                OR user_agent ILIKE '%Macintosh%'
                OR user_agent ILIKE '%Linux%'
                OR user_agent ILIKE '%Ubuntu%'
                OR user_agent ILIKE '%CrOS%'
                OR user_agent ILIKE '%compatible%')
                AND user_agent NOT ILIKE '%Android%'
                AND user_agent NOT ILIKE '%Phone%'
                AND user_agent NOT ILIKE '%ipad%'
                AND user_agent NOT ILIKE '%mobile%'
                THEN 'computer'
            WHEN user_agent ILIKE '%ipad%'
                OR user_agent ILIKE '%TB-%'
                OR user_agent ILIKE '%pad%'
                OR user_agent ILIKE '%sm-x%'
                OR user_agent ILIKE '%sm-t%'
                OR user_agent ILIKE '%tablet%'
                OR user_agent ILIKE '%tab%'
                THEN 'tablet'
            WHEN user_agent ILIKE '%playstation%'
                OR user_agent ILIKE '%nintendo%'
                OR user_agent ILIKE '%tvbrowser%'
                THEN 'other'
            ELSE 'mobile'
            END AS device_type_case,
        traits_device_brand,
        traits_device_platform,
        traits_device_type,
        device_type_case AS device_type,
        COALESCE(traits_device_platform, event_device_platform) AS device_platform,
        COALESCE(traits_device_brand, event_device_brand) AS device_brand
    FROM get_data
    ),

     order_submitted_events AS (
         SELECT session_id,
                LOWER(device_type) as device_platform,
                CASE
                    WHEN (user_agent ilike '%Windows NT%'
                        OR user_agent ILIKE '%Macintosh%'
                        OR user_agent ILIKE '%Linux%'
                        OR user_agent ILIKE '%Ubuntu%'
                        OR user_agent ILIKE '%CrOS%'
                        OR user_agent ILIKE '%compatible%')
                        AND user_agent NOT ILIKE '%Android%'
                        AND user_agent NOT ILIKE '%Phone%'
                        AND user_agent NOT ILIKE '%ipad%'
                        AND user_agent NOT ILIKE '%mobile%'
                        THEN 'computer'
                    WHEN user_agent ILIKE '%ipad%'
                        OR user_agent ILIKE '%TB-%'
                        OR user_agent ILIKE '%pad%'
                        OR user_agent ILIKE '%sm-x%'
                        OR user_agent ILIKE '%sm-t%'
                        OR user_agent ILIKE '%tablet%'
                        OR user_agent ILIKE '%tab%'
                        THEN 'tablet'
                    WHEN user_agent ILIKE '%playstation%'
                        OR user_agent ILIKE '%nintendo%'
                        OR user_agent ILIKE '%tvbrowser%'
                        THEN 'other'
                    ELSE 'mobile'
                    END AS device_type,
                device_manufacturer AS device_brand,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC) AS rn
         FROM segment.track_events 
         WHERE event_name = 'Order Submitted' AND event_time >= '2023-05-01'
     ),

device_per_session AS (
SELECT
    session_id,
    traits_device_brand,
    traits_device_platform,
    traits_device_type,
    device_type,
    device_platform,
    device_brand,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time) AS rn
FROM mapping
WHERE device_type IS NOT NULL)

SELECT
    a.session_id,
    COALESCE(b.device_type,a.device_type) AS device_type,
    COALESCE(b.device_platform,a.device_platform) AS device_platform,
    COALESCE(b.device_brand,a.device_brand) AS device_brand
FROM device_per_session a
LEFT JOIN order_submitted_events b ON a.session_id = b.session_id AND b.rn = 1
WHERE a.rn = 1;

GRANT SELECT ON segment.device_mapping TO hams;