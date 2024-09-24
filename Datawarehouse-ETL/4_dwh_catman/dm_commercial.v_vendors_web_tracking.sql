CREATE OR REPLACE VIEW dm_commercial.v_vendors_web_tracking AS 
WITH a AS (
	SELECT 
		te.session_id,
		te.anonymous_id,
		te.event_id,
		te.event_time AS event_time_start,		
		split_part(page_path,'/',2) AS url_split,
		REGEXP_REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(url_split,'-nl',''),'-es','')
			,'-en',''),'-de',''),'-','_'),'_.[0-9]{3}') AS parsed_store_name,
		CASE
            WHEN parsed_store_name IN ('at','at_','at_at','at_lp','business_at') THEN 'Austria'
            WHEN parsed_store_name IN ('de', 'de_','de_lp','business','business_en','business_de') THEN 'Germany'
            WHEN parsed_store_name IN ('nl','nl_','nl_lp','business_nl') THEN 'Netherlands'
            WHEN parsed_store_name IN ('es','es_','es_lp','business_es') THEN 'Spain'
            WHEN parsed_store_name IN ('us','us_', 'us_us','us_lp','business_us') THEN 'United States' 
        END AS store_country,
    	CASE 
    		WHEN parsed_store_name LIKE '%business%' THEN 'B2B' 
    		ELSE 'B2C'
    	END AS customer_type,
		JSON_EXTRACT_PATH_TEXT(te.properties,'widget_name') AS widget_label
	FROM segment.track_events te 
	WHERE event_name = 'Widget Clicked'
		AND te.event_time::date >= dateadd('month',-6,date_trunc('month',current_date)) 
		AND json_extract_path_text(te.properties, 'widget') NOT IN ('MC1','MC2','MC3')
		AND page_path LIKE '%brands%'
)
SELECT 
	store_country,
	customer_type,
	widget_label,
	event_time_start::date,
	count(DISTINCT session_id) AS sessions,
	count(DISTINCT anonymous_id) AS users,
	count(DISTINCT event_id) AS clicks
FROM a 
GROUP BY 1,2,3,4
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_commercial.v_vendors_web_tracking TO tableau;
