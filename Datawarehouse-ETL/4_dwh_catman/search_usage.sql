DROP TABLE IF EXISTS dm_catman.search_usage;
CREATE TABLE dm_catman.search_usage AS 
SELECT 
	s.fact_date,
	s.se_category,
	s.se_action,
	s.search_term,
	s.country_name,
	ROW_NUMBER() OVER() AS id_search_index
FROM (
			SELECT 
				te.event_time::date AS fact_date,
				'search' AS se_category,
				te.event_name AS se_action,
				CASE 
					WHEN te.event_name = 'Search Term Used' 
					 AND (is_valid_json(json_extract_path_text(te.properties,'search_term'))
					  OR is_valid_json(json_extract_path_text(te.properties,'eventProperty','search_term')))
						THEN COALESCE(NULLIF(json_extract_path_text(te.properties,'search_term'),''),NULLIF((json_extract_path_text(te.properties,'eventProperty','search_term')),''))
				END AS search_term,
				split_part(te.page_path,'/',2) AS url_split,
			REGEXP_REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(url_split,'-nl',''),'-es',''),'-en',''),'-de',''),'-','_'),'_.[0-9]{3}') AS parsed_store_name,
			CASE
	            WHEN parsed_store_name IN ('at','at_','at_at','at_lp','business_at') THEN 'Austria'
	            WHEN parsed_store_name IN ('de', 'de_','de_lp','business','business_en','business_de') THEN 'Germany'
	            WHEN parsed_store_name IN ('nl','nl_','nl_lp','business_nl') THEN 'Netherlands'
	            WHEN parsed_store_name IN ('es','es_','es_lp','business_es') THEN 'Spain'
	            WHEN parsed_store_name IN ('us','us_', 'us_us','us_lp','business_us') THEN 'United States' 
	        END AS country_name
			FROM segment.track_events te
			LEFT JOIN traffic.sessions s
				ON te.session_id = s.session_id
			WHERE event_name = 'Search Term Used'
				AND te.event_time::date > dateadd('week',-6,date_trunc('week',current_date))	
		) s
WHERE s.search_term <> '' OR s.search_term IS NOT NULL
;

GRANT SELECT ON dm_catman.search_usage TO tableau;
