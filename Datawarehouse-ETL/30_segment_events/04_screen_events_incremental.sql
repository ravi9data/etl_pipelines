SET enable_case_sensitive_identifier TO TRUE;

CREATE TEMP TABLE tmp_segment_screen_events AS
WITH screen_data AS (
SELECT 
    anonymous_id,
    userid,
    message_id,
    name,
    event_name,
    event_type,
    "timestamp",
    integrations,
    context,
    properties,
    traits,
    CASE WHEN IS_VALID_JSON(integrations) THEN JSON_PARSE(NULLIF(integrations,'nan')) ELSE NULL END AS i,
    CASE WHEN IS_VALID_JSON(context) THEN JSON_PARSE(context) ELSE NULL END AS c,
    CASE WHEN IS_VALID_JSON(properties) THEN JSON_PARSE(NULLIF(properties,'nan')) ELSE NULL END AS p,
    CASE WHEN IS_VALID_JSON(traits) THEN JSON_PARSE(NULLIF(traits,'nan')) ELSE NULL END AS t,
    sent_at,
    received_at,
    (year||'-'||month||'-'||day)::DATE AS loaded_at,
    ROW_NUMBER() OVER(PARTITION BY message_id ORDER BY received_at DESC) AS rn
WHERE (year||'-'||month||'-'||day)::DATE >= CURRENT_DATE - 1 AND "timestamp"::DATE <= CURRENT_DATE
AND event_type = 'screen'
),

parsed_data AS (
SELECT DISTINCT
    "anonymous_id",
    split_part(nullif(nullif("userid",'nan'),'None'),'.',1) as user_id,
    CASE WHEN c."session_id" <> '' THEN c."session_id"::BIGINT  
        ELSE split_part(NULLIF(JSON_EXTRACT_PATH_TEXT(integrations,'Actions Amplitude','session_id'),''),'.',1)::BIGINT  
    END AS session_id,
    "message_id" as event_id,
    "name" AS event_name,
    "event_type",
    "timestamp"::timestamp AS event_time,
    "context",
    c."app"."build"::VARCHAR AS app_build,
    c."app"."name"::VARCHAR AS app_name,
    c."app"."namespace"::VARCHAR AS app_namespace,
    c."app"."version"::VARCHAR AS app_version,
    c."device"."id"::VARCHAR AS device_id,
    c."device"."manufacturer"::VARCHAR AS device_manufacturer,
    c."device"."model"::VARCHAR AS device_model,
    c."device"."name"::VARCHAR AS device_name,
    c."device"."type"::VARCHAR AS device_type,
    c."ip"::VARCHAR AS ip,
    c."library"."name"::VARCHAR AS library_name,
    c."library"."version"::VARCHAR AS library_version,
    c."locale"::VARCHAR AS locale,
    c."network"."cellular"::VARCHAR AS network_cellular,
    c."network"."wifi"::VARCHAR AS network_wifi,
    c."network"."bluetooth"::VARCHAR AS network_bluetooth,
    c."network"."carrier"::VARCHAR AS network_carrier,
    c."os"."name"::VARCHAR AS os_name,
    c."os"."version"::VARCHAR AS os_version,
    c."protocols"."sourceId"::VARCHAR AS protocols_source_id,
    c."screen"."density"::DOUBLE PRECISION AS screen_density,
    c."screen"."height"::INT AS screen_height,
    c."screen"."width"::INT AS screen_width,
    c."timezone"::VARCHAR AS timezone,
    "properties",
    p."path"::VARCHAR AS page_path,
    p."screenType"::VARCHAR AS screen_type,
    p."store_Id"::INT AS store_id,
    "traits",
    "sent_at",
    "received_at",
    "loaded_at"
FROM screen_data
WHERE rn = 1)

SELECT 
    "anonymous_id",
    CASE WHEN "user_id" ~ '^[0-9]+$' THEN "user_id" ELSE NULL END AS "user_id",
    "session_id",
    "event_id",
    "event_name",
    "event_type",
    "event_time",
    "context",
    "app_build",
    "app_name",
    "app_namespace",
    "app_version",
    "device_id",
    "device_manufacturer",
    "device_model",
    "device_name",
    "device_type",
    "ip",
    "library_name",
    "library_version",
    "locale",
    "network_cellular",
    "network_wifi",
    "network_bluetooth",
    "network_carrier",
    "os_name",
    "os_version",
    "protocols_source_id",
    "screen_density",
    "screen_height",
    "screen_width",
    "timezone",
    "properties",
    "page_path",
    "screen_type",
    "store_id",
    "traits",
    "sent_at",
    "received_at",
    "loaded_at"
FROM parsed_data;

BEGIN TRANSACTION; 

DELETE FROM segment.screen_events 
USING tmp_segment_screen_events tmp
WHERE screen_events.event_id = tmp.event_id;

INSERT INTO segment.screen_events
SELECT *
FROM tmp_segment_screen_events;

END TRANSACTION;

SET enable_case_sensitive_identifier TO FALSE;
