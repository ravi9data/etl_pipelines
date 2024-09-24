SET enable_case_sensitive_identifier TO TRUE;

DROP TABLE IF EXISTS segment.identify_events;
CREATE TABLE segment.identify_events
DISTKEY(session_id)
SORTKEY(event_time) AS

WITH identify_data AS (
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
WHERE (year||'-'||month||'-'||day)::DATE >= '2023-01-01' AND "timestamp"::DATE <= CURRENT_DATE
    AND event_type = 'identify'
),

parsed_data AS (
SELECT
    "anonymous_id",
    split_part(nullif(nullif("userid",'nan'),'None'),'.',1) AS user_id,
    CASE WHEN c."session_id" <> '' THEN c."session_id"::BIGINT  
        ELSE split_part(NULLIF(JSON_EXTRACT_PATH_TEXT(integrations,'Actions Amplitude','session_id'),''),'.',1)::BIGINT  
    END AS session_id,
    "message_id" AS event_id,
    'Identify'::VARCHAR AS event_name,
    "event_type",
    "timestamp"::timestamp AS "event_time",
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
    c."screen"."density"::DOUBLE PRECISION AS screen_density,
    c."screen"."height"::INT AS screen_height,
    c."screen"."width"::INT AS screen_width,
    c."timezone"::VARCHAR AS timezone,
    c."campaign"."content"::VARCHAR AS context_content,
    t."marketing_content"::VARCHAR AS traits_content,
    c."campaign"."medium"::VARCHAR AS context_medium,
    t."marketing_medium"::VARCHAR AS traits_medium,
    c."campaign"."name"::VARCHAR AS context_campaign,
    t."marketing_name"::VARCHAR AS traits_campaign,
    c."campaign"."source"::VARCHAR AS context_source,
    t."marketing_source"::VARCHAR AS traits_source,
    c."campaign"."term"::VARCHAR AS context_term,
    t."marketing_term"::VARCHAR AS traits_term,
    c."protocols"."sourceId"::VARCHAR AS protocols_source_id,
    c."userAgent"::VARCHAR AS user_agent,
    properties,
    c."page"."path"::VARCHAR AS page_path,
    c."page"."referrer"::VARCHAR AS page_referrer,
    c."page"."search"::VARCHAR AS page_search,
    c."page"."title"::VARCHAR AS page_title,
    c."page"."url"::VARCHAR AS page_url,
    CASE WHEN page_referrer LIKE '%utm_medium%'
              THEN SPLIT_PART(SPLIT_PART(page_referrer,'utm_medium=',2),'&',1)
         ELSE NULL END AS referrer_utm_medium,
    CASE WHEN page_referrer LIKE '%utm_source%'
              THEN SPLIT_PART(SPLIT_PART(page_referrer,'utm_source=',2),'&',1)
         ELSE NULL END AS referrer_utm_source,
    CASE WHEN page_referrer LIKE '%utm_campaign%'
              THEN SPLIT_PART(SPLIT_PART(page_referrer,'utm_campaign=',2),'&',1)
         ELSE NULL END AS referrer_utm_campaign,
    CASE WHEN page_referrer LIKE '%utm_content%'
              THEN SPLIT_PART(SPLIT_PART(page_referrer,'utm_content=',2),'&',1)
         ELSE NULL END AS referrer_utm_content,
    CASE WHEN page_referrer LIKE '%utm_term%'
              THEN SPLIT_PART(SPLIT_PART(page_referrer,'utm_term=',2),'&',1)
         ELSE NULL END AS referrer_utm_term,
    CASE WHEN page_referrer LIKE '%gclid%'
             THEN SPLIT_PART(SPLIT_PART(page_referrer,'gclid=',2),'&',1)
         WHEN page_referrer LIKE '%fbclid%'
             THEN SPLIT_PART(SPLIT_PART(page_referrer,'fbclid=',2),'&',1)
         WHEN page_referrer LIKE '%msclkid%'
             THEN SPLIT_PART(SPLIT_PART(page_referrer,'msclkid=',2),'&',1)
         WHEN page_referrer LIKE '%tduid%'
             THEN SPLIT_PART(SPLIT_PART(page_referrer,'tduid=',2),'&',1)
         ELSE NULL END AS referrer_click_id,
    CASE WHEN page_url LIKE '%utm_medium%'
              THEN SPLIT_PART(SPLIT_PART(page_url,'utm_medium=',2),'&',1)
         ELSE NULL END AS url_utm_medium,
    CASE WHEN page_url LIKE '%utm_source%'
              THEN SPLIT_PART(SPLIT_PART(page_url,'utm_source=',2),'&',1)
         ELSE NULL END AS url_utm_source,
    CASE WHEN page_url LIKE '%utm_campaign%'
              THEN SPLIT_PART(SPLIT_PART(page_url,'utm_campaign=',2),'&',1)
         ELSE NULL END AS url_utm_campaign,
    CASE WHEN page_url LIKE '%utm_content%'
              THEN SPLIT_PART(SPLIT_PART(page_url,'utm_content=',2),'&',1)
         ELSE NULL END AS url_utm_content,
    CASE WHEN page_url LIKE '%utm_term%'
              THEN SPLIT_PART(SPLIT_PART(page_url,'utm_term=',2),'&',1)
         ELSE NULL END AS url_utm_term,
    CASE WHEN page_url LIKE '%gclid%' 
             THEN SPLIT_PART(SPLIT_PART(page_url,'gclid=',2),'&',1)
         WHEN page_url LIKE '%fbclid%' 
             THEN SPLIT_PART(SPLIT_PART(page_url,'fbclid=',2),'&',1)
         WHEN page_url LIKE '%msclkid%' 
             THEN SPLIT_PART(SPLIT_PART(page_url,'msclkid=',2),'&',1)
         WHEN page_url LIKE '%tduid%' 
             THEN SPLIT_PART(SPLIT_PART(page_url,'tduid=',2),'&',1)
         ELSE NULL END AS url_click_id,
    NULL::INT AS store_id,
    "traits",
    "sent_at",
    "received_at",
    "loaded_at"
FROM identify_data
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
    "screen_density",
    "screen_height",
    "screen_width",
    "timezone",
    COALESCE("context_content", "url_utm_content", "referrer_utm_content") AS "marketing_content",
    COALESCE("context_medium", "url_utm_medium", "referrer_utm_medium") AS "marketing_medium",
    COALESCE("context_campaign", "url_utm_campaign", "referrer_utm_campaign") AS "marketing_campaign",
    COALESCE("context_source", "url_utm_source", "referrer_utm_source") AS "marketing_source",
    COALESCE("context_term", "url_utm_term", "referrer_utm_term") AS "marketing_term",
    "traits_content",
    "traits_medium",
    "traits_campaign",
    "traits_source",
    "traits_term",
    COALESCE("url_click_id", "referrer_click_id") AS "click_id",
    "protocols_source_id",
    "user_agent",
    "properties",
    "page_path",
    "page_referrer",
    "page_search",
    "page_title",
    "page_url",
    "store_id",
    "traits",
    "sent_at",
    "received_at",
    "loaded_at"
FROM parsed_data;

UPDATE segment.identify_events SET marketing_source = 'ebay_kleinanzeigen' WHERE marketing_source ILIKE '%ebay_kleinanzeigen%';
UPDATE segment.identify_events SET traits_source = 'ebay_kleinanzeigen' WHERE traits_source ILIKE '%ebay_kleinanzeigen%';

SET enable_case_sensitive_identifier TO FALSE;
GRANT SELECT ON segment.identify_events TO hams;