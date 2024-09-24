CREATE OR REPLACE VIEW dm_commercial.v_commercial_campaign_tracking_click_through_rate AS 
WITH homepage_traffic AS (
	SELECT DISTINCT 
		session_id
	FROM segment.page_events pe  
	WHERE page_type = 'home'
		AND event_time::date BETWEEN '2023-05-25' AND current_date
)
, widget_clicks AS (
	SELECT DISTINCT 
		session_id,
		CASE WHEN IS_VALID_JSON(te.properties) THEN json_extract_path_text(properties, 'widget') END AS marketing_card,
		CASE WHEN IS_VALID_JSON(te.properties) THEN json_extract_path_text(properties, 'destination_url') END AS destination_url,
		REPLACE(REPLACE(destination_url, '/g-explore/', ''),'/','') AS campaign_title,
		NULLIF(json_extract_path_text(properties, 'content_position'),'') AS marketing_card_position,
		min(event_time) AS event_time
	FROM segment.track_events te 
	WHERE event_name= 'Widget Clicked'
		AND json_extract_path_text(properties, 'widget') IN ('MC1','MC2','MC3')
		AND event_time::date BETWEEN '2023-05-25' AND current_date
	GROUP BY 1,2,3,4,5
)
, click_through_rate_measures AS (
	SELECT DISTINCT
		t.session_id AS homepage_session_id,
		s.session_start::date AS event_time,
		CASE
			WHEN s.first_page_url ILIKE '%/de-%' THEN 'Germany'
			WHEN s.first_page_url ILIKE '%/us-%' THEN 'United States'
			WHEN s.first_page_url ILIKE '%/es-%' THEN 'Spain'
			WHEN s.first_page_url ILIKE '%/nl-%' THEN 'Netherlands'
			WHEN s.first_page_url ILIKE '%/at-%' THEN 'Austria'
			WHEN s.first_page_url ILIKE '%/business_es-%' THEN 'Spain'
			WHEN s.first_page_url ILIKE '%/business-%' THEN 'Germany'
			WHEN s.first_page_url ILIKE '%/business_at-%' THEN 'Austria'
			WHEN s.first_page_url ILIKE '%/business_nl-%' THEN 'Netherlands'
			WHEN s.first_page_url ILIKE '%/business_us-%' THEN 'United States'			
			WHEN s.store_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
			WHEN s.store_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
			WHEN s.store_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
			WHEN s.store_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
			WHEN s.store_name IS NULL AND s.geo_country = 'US' THEN 'United States'
			WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
	   		ELSE 'Germany' 
	   	END AS store_name,
	   	CASE 
	   		WHEN s.first_page_url ILIKE '%business%' THEN 'B2B'
	   		ELSE 'B2C'
	   	END AS customer_type,
		s.anonymous_id,
		CASE WHEN wc.session_id IS NOT NULL
					OR wc1.session_id IS NOT NULL
					OR wc2.session_id IS NOT NULL
					OR wc3.session_id IS NOT NULL
					OR wc4.session_id IS NOT NULL 
				THEN 1 ELSE 0 END AS session_has_widget_click,
		first_value(wc.marketing_card) OVER (PARTITION BY t.session_id ORDER BY wc.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			 AS marketing_card_MC1_0,
		first_value(wc.campaign_title) OVER (PARTITION BY t.session_id ORDER BY wc.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS campaign_title_MC1_0,
		first_value(wc.marketing_card_position) OVER (PARTITION BY t.session_id ORDER BY wc.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS marketing_card_location_MC1_0,
		first_value(wc1.marketing_card) OVER (PARTITION BY t.session_id ORDER BY wc1.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS marketing_card_MC1_1,
		first_value(wc1.campaign_title) OVER (PARTITION BY t.session_id ORDER BY wc1.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS campaign_title_MC1_1,
		first_value(wc1.marketing_card_position) OVER (PARTITION BY t.session_id ORDER BY wc1.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS marketing_card_location_MC1_1,
		first_value(wc2.marketing_card) OVER (PARTITION BY t.session_id ORDER BY wc2.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			 AS marketing_card_MC1_2,
		first_value(wc2.campaign_title) OVER (PARTITION BY t.session_id ORDER BY wc2.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS campaign_title_MC1_2,
		first_value(wc2.marketing_card_position) OVER (PARTITION BY t.session_id ORDER BY wc2.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS marketing_card_location_MC1_2,
		first_value(wc3.marketing_card) OVER (PARTITION BY t.session_id ORDER BY wc3.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS marketing_card_MC1_3,
		first_value(wc3.campaign_title) OVER (PARTITION BY t.session_id ORDER BY wc3.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS campaign_title_MC1_3,
		first_value(wc3.marketing_card_position) OVER (PARTITION BY t.session_id ORDER BY wc3.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
			AS marketing_card_location_MC1_3,
		first_value(wc4.marketing_card) OVER (PARTITION BY t.session_id ORDER BY wc4.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
			AS marketing_card_MC1,
		first_value(wc4.campaign_title) OVER (PARTITION BY t.session_id ORDER BY wc4.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
			AS campaign_title_MC1,
		first_value(wc5.marketing_card) OVER (PARTITION BY t.session_id ORDER BY wc5.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
			AS marketing_card_MC2,
		first_value(wc5.campaign_title) OVER (PARTITION BY t.session_id ORDER BY wc5.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
			AS campaign_title_MC2,
		first_value(wc6.marketing_card) OVER (PARTITION BY t.session_id ORDER BY wc6.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
			AS marketing_card_MC3,
		first_value(wc6.campaign_title) OVER (PARTITION BY t.session_id ORDER BY wc6.event_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
			AS campaign_title_MC3
	FROM homepage_traffic t
	LEFT JOIN widget_clicks wc
		ON wc.session_id = t.session_id
		 AND wc.marketing_card_position = 0
		 AND wc.marketing_card = 'MC1'
	LEFT JOIN widget_clicks wc1
		ON wc1.session_id = t.session_id
		 AND wc1.marketing_card_position = 1
		 AND wc1.marketing_card = 'MC1'
	LEFT JOIN widget_clicks wc2
		ON wc2.session_id = t.session_id
		 AND wc2.marketing_card_position = 2
		 AND wc2.marketing_card = 'MC1'
	LEFT JOIN widget_clicks wc3
		ON wc3.session_id = t.session_id
		 AND wc3.marketing_card_position = 3
		 AND wc3.marketing_card = 'MC1'
	LEFT JOIN widget_clicks wc4 -- WHEN we do not have the card POSITION we take the first MC click (only for past campaigns)
		ON wc4.session_id = t.session_id
		 AND wc4.marketing_card = 'MC1'
	LEFT JOIN widget_clicks wc5 
		ON wc5.session_id = t.session_id
	 	 AND wc5.marketing_card = 'MC2'
	LEFT JOIN widget_clicks wc6 
		ON wc6.session_id = t.session_id
		 AND wc6.marketing_card = 'MC3'
	LEFT JOIN traffic.sessions s 
		ON t.session_id = s.session_id
)		
SELECT 
	event_time,
	store_name,
	customer_type,
	session_has_widget_click,
	marketing_card_MC1_0,
	campaign_title_MC1_0,
	marketing_card_location_MC1_0,
	marketing_card_MC1_1,
	campaign_title_MC1_1,
	marketing_card_location_MC1_1,
	marketing_card_MC1_2,
	campaign_title_MC1_2,
	marketing_card_location_MC1_2,
	marketing_card_MC1_3,
	campaign_title_MC1_3,
	marketing_card_location_MC1_3,
	marketing_card_MC1,
	campaign_title_MC1,
	marketing_card_MC2,
	campaign_title_MC2,
	marketing_card_MC3,
	campaign_title_MC3,
	count(DISTINCT homepage_session_id) AS unique_homepage_sessions,
	count(DISTINCT anonymous_id) AS unique_homepage_users
FROM click_through_rate_measures
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,21,22
WITH NO SCHEMA BINDING
;
