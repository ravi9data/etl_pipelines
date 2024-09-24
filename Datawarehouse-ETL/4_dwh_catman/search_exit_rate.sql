CREATE TEMP TABLE tmp_search_exit_rate AS 
WITH search_entered AS (
	SELECT 
		te.anonymous_id,
		te.session_id,
		te.event_id AS event_id_search_entered, 
		te.event_time AS event_time_search_entered,
		te.event_name, 
		split_part(te.page_path,'/',2) AS url_split,
		REGEXP_REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(url_split,'-nl',''),'-es',''),'-en',''),'-de',''),'-','_'),'_.[0-9]{3}') AS parsed_store_name,
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
        END AS customer_type
	FROM segment.track_events te 
	WHERE event_name = 'Search Entered'
		AND te.event_time >= current_date - 3
)
, search_exited AS (
	SELECT 
		te.session_id,
		te.event_id AS event_id_search_exited, 
		te.event_time AS event_time_search_exited
	FROM segment.track_events te 
	WHERE event_name = 'Search Exited'
		AND te.event_time >= current_date - 3
)
, first_exited_event AS (
	SELECT 
		se.anonymous_id,
		se.session_id,
		se.event_id_search_entered,
		se.event_time_search_entered,
		se.store_country,
		se.customer_type,
		sx.event_id_search_exited,
		sx.event_time_search_exited,
		ROW_NUMBER () OVER (PARTITION BY se.event_id_search_entered ORDER BY sx.event_time_search_exited) AS rowno_entered
	FROM search_entered se
	LEFT JOIN search_exited sx
		ON sx.session_id = se.session_id
			AND sx.event_time_search_exited > se.event_time_search_entered
)
SELECT 
	anonymous_id,
	session_id,
	event_id_search_entered,
	event_time_search_entered,
	store_country,
	customer_type,	
	ROW_NUMBER() OVER (PARTITION BY event_id_search_exited ORDER BY event_time_search_entered DESC) rowno_exited,
	CASE 
		WHEN rowno_exited = 1 
			THEN event_id_search_exited
	END AS event_id_search_exited, 
	CASE 
		WHEN rowno_exited = 1
			THEN event_time_search_exited
	END AS event_time_search_exited
FROM first_exited_event
WHERE rowno_entered = 1
;

BEGIN TRANSACTION;

DELETE FROM dm_commercial.search_exit_rate 
WHERE event_time_search_entered::date >= current_date-3;

INSERT INTO dm_commercial.search_exit_rate
SELECT *
FROM tmp_search_exit_rate;

END TRANSACTION;

DROP TABLE IF EXISTS tmp_search_exit_rate;

GRANT SELECT ON dm_commercial.search_exit_rate TO tableau;
