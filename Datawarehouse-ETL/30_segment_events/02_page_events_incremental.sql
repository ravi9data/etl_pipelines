SET enable_case_sensitive_identifier TO TRUE;

CREATE TEMP TABLE tmp_segment_page_events AS
WITH page_data AS (
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
WHERE (year||'-'||month||'-'||day)::DATE >= current_date - 1 AND "timestamp"::DATE <= CURRENT_DATE
  AND event_type = 'page'
),

parsed_data AS (
SELECT
    "anonymous_id",
    split_part(nullif(nullif("userid",'nan'),'None'),'.',1) as user_id,
    CASE WHEN c."session_id" <> '' THEN c."session_id"::BIGINT  
        ELSE split_part(NULLIF(JSON_EXTRACT_PATH_TEXT(integrations,'Actions Amplitude','session_id'),''),'.',1)::BIGINT  
    END AS session_id,
    "message_id" as event_id,
    "name" as event_name,
    "event_type",
    "timestamp"::timestamp AS "event_time",
    context,
    c."campaign"."name"::VARCHAR AS context_campaign,
    c."campaign"."medium"::VARCHAR AS context_medium,
    c."campaign"."source"::VARCHAR AS context_source,
    c."campaign"."term"::VARCHAR AS context_term,
    c."campaign"."content"::VARCHAR AS context_content,
    c."traits"."marketing_name"::VARCHAR AS traits_campaign,
    c."traits"."marketing_medium"::VARCHAR AS traits_medium,
    c."traits"."marketing_source"::VARCHAR AS traits_source,
    c."traits"."marketing_term"::VARCHAR AS traits_term,
    c."traits"."marketing_content"::VARCHAR AS traits_content,
    c."ip"::VARCHAR AS ip,
    c."library"."name"::VARCHAR AS library_name,
    c."library"."version"::VARCHAR AS library_version,
    c."locale"::VARCHAR AS locale,
    c."protocols"."sourceId"::VARCHAR AS protocols_source_id,
    c."userAgent"::VARCHAR AS user_agent,
    p."name"::VARCHAR AS page_name,
    p."page_type"::VARCHAR AS page_type,
    properties,
    COALESCE(c."page"."path", p."path")::VARCHAR AS page_path,
    COALESCE(c."page"."referrer", p."referrer")::VARCHAR AS page_referrer,
    COALESCE(c."page"."search", p."search")::VARCHAR AS page_search,
    COALESCE(c."page"."title", p."title")::VARCHAR AS page_title,
    COALESCE(c."page"."url", p."url")::VARCHAR AS page_url,
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
    "traits",
    "sent_at",
    "received_at",
    "loaded_at",
     NULLIF(JSON_EXTRACT_PATH_TEXT(properties,'store_id'),'')::INT AS store_id,
     NULLIF(JSON_EXTRACT_PATH_TEXT(properties,'store'),'')::VARCHAR AS store
FROM page_data
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
    COALESCE("context_content", "url_utm_content", "referrer_utm_content") AS "marketing_content",
    COALESCE("context_medium", "url_utm_medium","referrer_utm_medium") AS "marketing_medium",
    COALESCE("context_campaign", "url_utm_campaign", "referrer_utm_campaign") AS "marketing_campaign",
    COALESCE("context_source", "url_utm_source", "referrer_utm_source") AS "marketing_source",
    COALESCE("context_term","url_utm_term", "referrer_utm_term") AS "marketing_term",
    "traits_campaign",
    "traits_medium",
    "traits_source",
    "traits_term",
    "traits_content",
    COALESCE("url_click_id","referrer_click_id") AS "click_id",
    "ip",
    "library_name",
    "library_version",
    "locale",
    "protocols_source_id",
    "user_agent",
    "page_name",
    "page_type",
    "properties",
    "page_path",
    "page_referrer",
    "page_search",
    "page_title",
    "page_url",
    "traits",
    "sent_at",
    "received_at",
    "loaded_at",
    "store_id",
    "store"
FROM parsed_data;

BEGIN TRANSACTION; 

DELETE FROM segment.page_events 
USING tmp_segment_page_events tmp
WHERE page_events.event_id = tmp.event_id;

INSERT INTO segment.page_events
SELECT *
FROM tmp_segment_page_events;

UPDATE segment.page_events SET marketing_source = 'ebay_kleinanzeigen' WHERE marketing_source ILIKE '%ebay_kleinanzeigen%';
UPDATE segment.page_events SET traits_source = 'ebay_kleinanzeigen' WHERE traits_source ILIKE '%ebay_kleinanzeigen%';

END TRANSACTION;

SET enable_case_sensitive_identifier TO FALSE;
