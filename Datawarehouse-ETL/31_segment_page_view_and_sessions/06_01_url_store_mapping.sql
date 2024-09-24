DROP TABLE IF EXISTS segment.url_store_mapping;
CREATE TABLE segment.url_store_mapping AS
WITH parsed_url AS (
    SELECT DISTINCT 
        page_url,
        page_path,
        split_part(page_path,'/',2) AS url_split,
        REGEXP_REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(url_split,'-nl',''),'-es',''),'-en',''),'-de',''),'-','_'),'_.[0-9]{3}') AS parsed_store_name,
        CASE
            WHEN parsed_store_name IN ('at','at_','at_at','at_lp') THEN 'Austria'
            WHEN parsed_store_name IN ('uk_uk', 'uk') THEN 'UK'
            WHEN parsed_store_name IN ('business','business_en','business_de') THEN 'Grover_B2B_Germany'
            WHEN parsed_store_name IN ('business_nl') THEN 'Grover_B2B_Netherlands'
            WHEN parsed_store_name IN ('business_at') THEN 'Grover_B2B_Austria'
            WHEN parsed_store_name IN ('business_es') THEN 'Grover_B2B_Spain'
            WHEN parsed_store_name IN ('business_us') THEN 'Grover_B2B_Usited_States'
            WHEN parsed_store_name IN ('de', 'de_','de_lp') THEN 'Germany'
            WHEN parsed_store_name IN ('mediamarkt') THEN 'Media_Markt'
            WHEN parsed_store_name IN ('saturn') THEN 'saturn'
            WHEN parsed_store_name = 'conrad' THEN 'conrad'
            WHEN parsed_store_name IN ('otto_austria') THEN 'Otto_Austria'
            WHEN parsed_store_name IN ('C:') THEN ''
            WHEN parsed_store_name IN ('universal_online') THEN 'Universal_Austria'
            WHEN parsed_store_name IN ('nl','nl_','nl_lp') THEN 'Netherlands'
            WHEN parsed_store_name IN ('es','es_','es_lp') THEN 'Spain'
            WHEN parsed_store_name IN ('us','us_', 'us_us','us_lp') THEN 'United_States'
            WHEN parsed_store_name ilike ('%irobot%') THEN 'iRobot'
            WHEN parsed_store_name ilike ('%Baur%') THEN 'Baur'
            ELSE parsed_store_name 
        END AS store_name,
        COUNT(DISTINCT session_id) AS sessions
    FROM segment.page_events
    GROUP BY 1,2
    ),

    final_url AS (
    SELECT DISTINCT
        a.page_url,
        s.id AS store_id,
        a.store_name AS store_name_segment,
        s.store_name,
        CASE
            WHEN a.store_name LIKE 'saturn%' AND s.store_name IS NULL THEN 'Saturn offline'
            WHEN a.store_name LIKE 'mediamarkt%' AND s.store_name IS NULL THEN 'Media Markt offline'
            WHEN a.store_name LIKE 'gravis%' AND s.store_name IS NULL THEN 'Gravis offline'
            ELSE coalesce(store_label,'Others') 
        END AS store_label,
        a.sessions,
        ROW_NUMBER() OVER(PARTITION BY page_url ORDER BY a.sessions DESC) AS rn 
    FROM parsed_url a
        LEFT JOIN ods_production.store s
            ON LOWER(SPLIT_PART(a.store_name,'_',1)) = SPLIT_PART(s.store_name_normalized,' ',1)
            AND LOWER(SPLIT_PART(a.store_name,'_',2)) = SPLIT_PART(s.store_name_normalized,' ',2)
            AND LOWER(SPLIT_PART(a.store_name,'_',3)) = SPLIT_PART(s.store_name_normalized,' ',3)
    )

    SELECT 
        page_url,
        store_id,
        store_name_segment,
        store_name,
        store_label,
        sessions
    FROM final_url 
    WHERE rn = 1;

GRANT SELECT ON segment.url_store_mapping TO hams;