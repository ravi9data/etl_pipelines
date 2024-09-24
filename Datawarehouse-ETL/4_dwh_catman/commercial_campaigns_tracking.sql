--Commercial Campaign Performance Tracking
--End User: Commercial Campaign & Planning Team 

--dm_commercial.commercial_campaigns_tracking INCREMENTAL

CREATE TEMP TABLE tmp_commercial_campaigns_tracking AS
WITH widget_clicks AS (
	SELECT 
		te.session_id,
		te.anonymous_id AS user_id,
		te.event_id, 
		te.event_name, 
		te.event_time AS event_time_start,		
		json_extract_path_text(te.properties, 'widget') AS marketing_card,
		json_extract_path_text(te.properties, 'destination_url') AS destination_url,
		split_part(page_path,'/',2) AS url_split,
		REGEXP_REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(url_split,'-nl',''),'-es','')
			,'-en',''),'-de',''),'-','_'),'_.[0-9]{3}') AS parsed_store_name,
		REPLACE(REPLACE(destination_url, '/g-explore/', ''),'/','') AS marketing_card_title, 
		JSON_EXTRACT_PATH_TEXT(te.properties,'widget_name') AS widget_label,
		NULLIF(json_extract_path_text(te.properties, 'content_position'),'') AS marketing_card_position
	FROM segment.track_events te 
	WHERE event_name = 'Widget Clicked'
		AND te.event_time::date >= '2023-05-25'
		AND json_extract_path_text(te.properties, 'widget') IN ('MC1','MC2','MC3')
		AND te.event_time::date >= current_date - 3
		AND (app_name <> 'Grover' OR app_name IS NULL)
)
, landing_page_sessions AS ( 
	SELECT 
		te.session_id, 
		te.anonymous_id AS user_id,
		te.event_id, 
		te.event_name,
		te.event_time AS event_time_start,		
		NULL AS marketing_card,
		te.page_path,
		split_part(page_path,'/',2) AS url_split,
		REGEXP_REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(url_split,'-nl','')
			,'-es',''),'-en',''),'-de',''),'-','_'),'_.[0-9]{3}') AS parsed_store_name_,
		CASE
			WHEN page_path ILIKE '%/g-explore/%'
				THEN split_part(page_path, '/g-explore/',2)
			WHEN page_path LIKE '%referrals%' OR page_path LIKE '%deals%'
				THEN split_part(page_path, '/',3)
		END  AS marketing_card_title_, 
		NULL AS widget_label,
		NULL AS marketing_card_position
	FROM segment.track_events te 
	WHERE event_name = ('Session Started')
		AND te.event_time::date >= '2023-05-25'
		AND EXISTS (SELECT NULL FROM widget_clicks wc 
					WHERE wc.marketing_card_title = marketing_card_title_ 
						AND wc.parsed_store_name = parsed_store_name_) --with this condition we only look for campaigns url, excluding partnership or other traffic
		AND (page_path ILIKE '%/g-explore/%' OR page_path LIKE '%referrals%' OR page_path LIKE '%deals%')
		AND te.event_time::date >= current_date - 3
)
, traffic_union AS ( 
	SELECT 
		session_id,
		user_id,
		event_id, 
		event_name, 
		event_time_start,		
		marketing_card,
		parsed_store_name,
		marketing_card_title, 
		widget_label,
		marketing_card_position
	FROM widget_clicks
	UNION 
	SELECT 
		session_id,
		user_id,
		event_id, 
		event_name,
		event_time_start,		
		marketing_card,
		parsed_store_name_ AS parsed_store_name,
		marketing_card_title_ AS marketing_card_title, 
		widget_label,
		marketing_card_position
	FROM landing_page_sessions
)
, page_loaded AS (
	SELECT 
		pe.session_id,
		pe.user_id,
		pe.anonymous_id,
		pe.event_id, 
		pe.event_time AS event_time_start,
		pe.page_path,
		split_part(page_path,'/',2) AS url_split,
		REGEXP_REPLACE(replace(REPLACE(REPLACE(REPLACE(REPLACE(url_split,'-nl','')
			,'-es',''),'-en',''),'-de',''),'-','_'),'_.[0-9]{3}') AS parsed_store_name_,
		CASE 
			WHEN (pe.page_type = 'landing-page' AND page_path ILIKE '%/g-explore/%') 
				THEN split_part(page_path, '/g-explore/',2)
			WHEN pe.page_type = 'referrals' OR pe.page_type = 'deals'
				THEN split_part(page_path, '/',3)
		END AS marketing_card_title_, 
		COALESCE(lag(event_time) OVER (PARTITION BY session_id ORDER BY event_time ASC),'2015-01-01') AS previous_page_loaded,
		COALESCE(lag(event_time) OVER (PARTITION BY session_id ORDER BY event_time DESC),current_timestamp)  AS following_page_loaded
	FROM segment.page_events pe
	WHERE 
		(
			(pe.page_type = 'landing-page' AND page_path ILIKE '%/g-explore/%')
		OR
			pe.page_type = 'referrals'
		OR 	pe.page_type = 'deals'
		)
 		AND pe.event_time::date >= '2023-05-25'
		AND EXISTS (SELECT NULL FROM traffic_union tu 
						WHERE tu.marketing_card_title = marketing_card_title_ 
							AND tu.parsed_store_name = parsed_store_name_) --with this condition we only look for campaigns url, excluding partnership or other traffic	
		AND pe.event_time::date >= current_date - 3
)
, page_loaded_definition AS ( -- All sessions start with a page loaded, we then define the type of traffic with the left join  
	SELECT DISTINCT 
		pl.session_id,
		pl.user_id,
		pl.anonymous_id,
		pl.event_id AS event_id_page_loaded,
		pl.event_time_start AS event_time_page_loaded,
		pl.parsed_store_name_,
		pl.marketing_card_title_ AS campaign_title, 
		tu.event_id AS event_id_traffic,  
		tu.event_name AS event_name_traffic,
		tu.marketing_card AS marketing_card_traffic, --MC1, MC2, MC3
		tu.widget_label AS widget_label_traffic,
		tu.marketing_card_position AS marketing_card_position_traffic,
		ROW_NUMBER () OVER (PARTITION BY event_id_page_loaded ORDER BY tu.event_time_start) AS row_no --first traffic event after page loaded
	FROM page_loaded pl 
	LEFT JOIN traffic_union tu 
		ON tu.session_id = pl.session_id
		 AND pl.marketing_card_title_ = tu.marketing_card_title
		 AND tu.event_time_start > CASE
 								       WHEN tu.event_name = 'Session Started' THEN pl.event_time_start 
			 						   WHEN tu.event_name = 'Widget Clicked' THEN pl.previous_page_loaded
		 						   END
         AND tu.event_time_start < CASE
 								       WHEN tu.event_name = 'Session Started' THEN pl.following_page_loaded 
			 						   WHEN tu.event_name = 'Widget Clicked' THEN pl.event_time_start
		 						   END
)
, product_clicked AS (
	SELECT 
		te.session_id,
		te.user_id,
		te.event_id, 
		te.event_time AS event_time_start,
		COALESCE(NULLIF(json_extract_path_text(te.properties,'product_sku'),''),
		NULLIF(json_extract_path_text(te.properties,'productSKU'),'')) AS product_sku,
		COALESCE(NULLIF(split_part(split_part(te.page_referrer,'?',1) , '/g-explore/',2),'')
			,NULLIF(split_part(te.page_path, '/g-explore/',2),'')
			,NULLIF(split_part(te.page_path, '/',3),'')
			) AS campaign_title
	FROM segment.track_events te
	WHERE event_name = 'Product Clicked'
		AND te.event_time::date >= '2023-05-25'
		AND (te.page_referrer ILIKE '%/g-explore/%'	
			OR te.page_path ILIKE '%/g-explore/%'
			OR te.page_path ILIKE '%/deals%')
		AND te.event_time::date >= current_date - 3
)
, product_viewed_prep AS ( 
	SELECT 
		pe.session_id,
		pe.user_id, 
		pe.event_id,
		pe.page_type,
		pe.page_referrer,
		pe.event_time AS event_time_start,
		split_part(split_part(pe.page_referrer,'?',1) , '/g-explore/',2) AS campaign_title,
		json_extract_path_text(pe.properties,'product_sku') AS product_sku,
		LAG(page_path) OVER (PARTITION BY pe.session_id ORDER BY pe.event_time) AS previous_page_path
	FROM segment.page_events pe
	WHERE pe.event_time::date >= '2023-05-25'
		AND pe.event_time::date >= current_date -3
)
, product_viewed AS ( 
	SELECT 
		pe.session_id,
		pe.user_id, 
		pe.event_id,
		pe.event_time_start,
		COALESCE(NULLIF(campaign_title, ''), split_part(previous_page_path, '/',3) ) AS campaign_title,
		product_sku,
		previous_page_path
	FROM product_viewed_prep pe
	WHERE pe.page_type = 'pdp'
		AND pe.event_time_start::date >= '2023-05-25'
		AND (pe.page_referrer ILIKE '%/g-explore/%'	
			OR pe.previous_page_path ILIKE '%/g-explore/%'
			OR pe.previous_page_path ILIKE '%/deals%')
		AND pe.event_time_start::date >= current_date -3
)		
, product_click_to_viewed AS (
	SELECT DISTINCT 
		pc.session_id,
		COALESCE(pc.user_id, pv.user_id) as user_id,
		pc.event_id AS product_clicked_event_id, 
		pc.event_time_start AS product_clicked_time_start,
		pc.campaign_title, 
		pc.product_sku,
		ROW_NUMBER () OVER (PARTITION BY pv.event_id ORDER BY pc.event_time_start) AS row_no_product_viewed, -- EACH product VIEW has 1 product click
		CASE WHEN row_no_product_viewed = 1 THEN pv.event_id END AS product_viewed_event_id,
		CASE WHEN row_no_product_viewed = 1 THEN pv.event_time_start END AS product_viewed_time_start,
		ROW_NUMBER () OVER (PARTITION BY pc.event_id ORDER BY pv.event_time_start) AS row_no --EACH product click has 1 product view
	FROM product_clicked pc
	LEFT JOIN product_viewed pv 
		ON pv.session_id = pc.session_id
		 AND pv.product_sku = pc.product_sku
		 AND pv.campaign_title = pc.campaign_title
		 AND pv.event_time_start > pc.event_time_start		 
)
, product_added AS ( 
	SELECT 
		te.session_id,
		COALESCE(te.user_id::varchar, o.customer_id::varchar) AS user_id,
		te.event_id, 
		te.event_time AS event_time_start,
		CASE 
			WHEN json_extract_path_text(te.properties,'product_sku') LIKE 'GRB%' THEN json_extract_path_text(te.properties,'product_sku')
			ELSE p.product_sku 
		END AS product_sku,
		COALESCE(o.order_id,te.order_id) AS order_id,
		o.created_date,
		o.submitted_date,
		o.paid_date 
	FROM segment.track_events te 
	LEFT JOIN master."order" o 
		ON o.order_id = te.order_id 
	LEFT JOIN ods_production.variant v 
		ON NULLIF(json_extract_path_text(te.properties,'variant'),'') = v.variant_id 
	LEFT JOIN ods_production.product p 
		ON p.product_id = v.product_id	
	WHERE te.event_name IN ('Product Added','Product Added to Cart') 
		AND te.event_time::date >= '2023-05-25'
		AND te.event_time::date >= current_date - 3
)
, page_loaded_with_product_click AS ( 
	SELECT DISTINCT 
		pld.session_id,
		coalesce(pld.user_id,pv.user_id) AS user_id,
		pld.anonymous_id,
		pld.event_id_page_loaded,
		pld.event_time_page_loaded,
		pld.campaign_title,
		CASE
            WHEN pld.parsed_store_name_ IN ('at','at_','at_at','at_lp','business_at') THEN 'Austria'
            WHEN pld.parsed_store_name_ IN ('de', 'de_','de_lp','business','business_en','business_de') THEN 'Germany'
            WHEN pld.parsed_store_name_ IN ('nl','nl_','nl_lp','business_nl') THEN 'Netherlands'
            WHEN pld.parsed_store_name_ IN ('es','es_','es_lp','business_es') THEN 'Spain'
            WHEN pld.parsed_store_name_ IN ('us','us_', 'us_us','us_lp','business_us') THEN 'United_States' 
        END AS store_country,
        CASE 
        	WHEN pld.parsed_store_name_ LIKE '%business%' THEN 'B2B' 
        	ELSE 'B2C'
        END AS customer_type,
        pld.event_id_traffic,
        pld.marketing_card_traffic,
		COALESCE(pld.event_name_traffic, 'Click on the Campaign Directly') AS event_name_traffic,
		pld.widget_label_traffic, 
		pld.marketing_card_position_traffic,
		ROW_NUMBER () OVER (PARTITION BY pv.product_viewed_event_id ORDER BY pld.event_time_page_loaded DESC) AS rowno, 
			-- 1 product view can only have 1 traffic (widget click, session start or the other event)
		CASE 
			WHEN rowno = 1 THEN pv.product_viewed_event_id
		END AS product_viewed_event_id,
		CASE 
			WHEN rowno = 1 THEN pv.product_viewed_time_start
		END AS product_viewed_time_start,
		CASE 
			WHEN rowno = 1 THEN pv.product_sku
		END AS product_sku
	FROM page_loaded_definition pld
	LEFT JOIN product_click_to_viewed pv
		ON pv.session_id = pld.session_id
		 AND pv.product_clicked_time_start > pld.event_time_page_loaded
		 AND pld.campaign_title = pv.campaign_title
		 AND pv.row_no = 1
	WHERE pld.row_no = 1
)
, web_final AS (
	SELECT DISTINCT 
		f.session_id,
		COALESCE(f.user_id::varchar,pa.user_id::varchar) AS user_id,
		f.anonymous_id,
		f.event_id_page_loaded,
		f.event_time_page_loaded,
		f.campaign_title,
		f.store_country,
		f.customer_type,
		f.event_id_traffic,
		f.event_name_traffic,
		marketing_card_traffic,
		f.widget_label_traffic,
		f.marketing_card_position_traffic,
		sw.marketing_channel, 
		f.product_viewed_event_id,
		f.product_viewed_time_start,
		f.product_sku,
		p.category_name,
		p.subcategory_name,
		p.brand,
		p.product_name,
		ROW_NUMBER() OVER (PARTITION BY pa.event_id ORDER BY f.product_viewed_time_start DESC) AS row_no,
		CASE 
			WHEN row_no = 1 THEN pa.event_id
			END AS product_added_event_id,
		CASE 
			WHEN row_no = 1 THEN pa.event_time_start
			END AS product_added_time_start,
		CASE 
			WHEN row_no = 1 THEN pa.order_id
			END AS order_id,
		CASE 
			WHEN row_no = 1 THEN pa.created_date
			END AS order_created_date,
		CASE 
			WHEN row_no = 1 THEN pa.submitted_date
			END AS order_submitted_date,
		CASE 
			WHEN row_no = 1 THEN pa.paid_date
			END AS order_paid_date,
		'web session' AS session_origin	
	FROM page_loaded_with_product_click f
	LEFT JOIN ods_production.product p 
		ON p.product_sku = f.product_sku
	LEFT JOIN segment.sessions_web sw 
		ON sw.session_id = f.session_id
	LEFT JOIN product_added pa 
		ON pa.session_id = f.session_id
		 AND pa.product_sku = f.product_sku
		 AND pa.event_time_start > f.product_viewed_time_start
)		 
, app_widget_clicks AS (
	SELECT 
		context_actions_amplitude_session_id AS session_id,
		user_id, 
		anonymous_id, 
		wc.id AS widget_clicked_event_id,
		original_timestamp AS widget_clicked_event_time_start,  
		widget AS marketing_card,
		CASE 
			WHEN wc.store_id <> '-1'
				THEN s.country_name 
			WHEN wc.store_id = '-1'
				THEN 'Germany'
		END AS store_name,
		CASE 
			WHEN c.customer_type = 'business_customer'
				THEN 'B2B'
			ELSE 'B2C'	
		END AS customer_type,
		NULLIF(CASE 
					WHEN destination_url IN ('/g-explore/apple-deals','/g-explore/january-deals','/g-explore/weekly-deals','/g-explore/deals-countdown','/g-explore/computer-deals')
						THEN split_part(destination_url,'/',3) -- hardcoding it if not problems with deals url
					WHEN destination_url LIKE '%referrals%' OR destination_url LIKE '%deals%' 
						THEN split_part(destination_url,'/',2)
					WHEN destination_url LIKE '%de-de%'	OR destination_url LIKE '%de-en%'
						THEN  split_part(destination_url,'/',4)
					ELSE split_part(destination_url,'/',3) 
			   END,'') AS marketing_card_title,
		widget_name AS widget_label,
		NULL AS marketing_card_position
	FROM react_native.widget_clicked wc 
	LEFT JOIN ods_production.store s
		ON s.id = wc.store_id 
	LEFT JOIN master.customer c 
		ON c.customer_id = wc.user_id
	WHERE wc.widget IN ('MC1','MC2','MC3')
		AND original_timestamp::date >= current_date - 3
		AND original_timestamp::date >= '2023-11-01'
)
, app_product_clicked AS (	
SELECT 
	c.id AS product_clicked_event_id,
	c.context_actions_amplitude_session_id AS session_id,
	c.anonymous_id,
	c.original_timestamp AS product_clicked_time_start,
	c.event_text,
	LAG(c.event_text) OVER (PARTITION BY c.context_actions_amplitude_session_id ORDER BY c.original_timestamp) AS previous_event_text,
	LAG(c.event_text,2) OVER (PARTITION BY c.context_actions_amplitude_session_id ORDER BY c.original_timestamp) AS previous_event_text2,
	COALESCE(pc.product_sku, pc.event_property_product_sku) AS product_sku
FROM react_native.tracks c
LEFT JOIN react_native.product_clicked pc 
	ON c.id = pc.id
	WHERE c.original_timestamp::date >= current_date - 3	
		AND c.original_timestamp::date >= '2023-11-01'
		AND EXISTS (SELECT NULL FROM app_widget_clicks w WHERE w.session_id = c.context_actions_amplitude_session_id)
)
, product_additions AS (
	SELECT 
		pc.session_id,
		pc.anonymous_id,
		pc.product_clicked_event_id,
		pc.product_clicked_time_start,
		pc.product_sku,
		pa.order_id, 
		ROW_NUMBER () OVER (PARTITION BY pc.product_clicked_event_id ORDER BY pa.original_timestamp) AS rowno,
		pa.id AS product_added_event_id,
		pa.original_timestamp AS product_added_time_start
	FROM app_product_clicked pc
	LEFT JOIN react_native.product_added_to_cart pa
		ON pc.session_id = pa.context_actions_amplitude_session_id 
			AND pc.product_clicked_time_start < pa.original_timestamp
			AND pc.product_sku = pa.product_sku
			AND pa.order_id IS NOT NULL	
	WHERE (pc.previous_event_text IN ('Widget Clicked') OR previous_event_text2 IN ('Widget Clicked'))
		AND pc.event_text = 'Product Clicked'
)
, app_product_added_to_cart AS (
	SELECT 
		session_id,
		anonymous_id,
		product_clicked_event_id,
		product_clicked_time_start,
		pac.product_sku, 
		product_added_event_id,
		product_added_time_start,
		pac.order_id,
		o.created_date,
		o.submitted_date,
		o.paid_date
	FROM product_additions pac
	LEFT JOIN master."order" o 
		ON o.order_id = pac.order_id
	WHERE rowno = 1		
)
, widget_clicks_to_product_clicked AS (
	SELECT 
		awc.session_id,
		awc.user_id,
		awc.anonymous_id,
		awc.widget_clicked_event_id AS event_id_page_loaded, --to match the columns in the union
		awc.widget_clicked_event_time_start AS event_time_page_loaded, --to match the columns in the union
		awc.marketing_card_title AS campaign_title,
		awc.store_name AS store_country,
		awc.customer_type,
		awc.widget_clicked_event_id,
		awc.widget_clicked_event_time_start,
		awc.marketing_card AS marketing_card_traffic,
		awc.widget_label AS widget_label_traffic,
		awc.marketing_card_position AS marketing_card_position_traffic,
		sa.marketing_channel,
		ptc.product_clicked_event_id, --instead of product viewed
		ptc.product_clicked_time_start, --instead of product viewed
		ptc.product_sku,
		NULL AS row_no, --to match the columns in the union
		ptc.product_added_event_id,
		ptc.product_added_time_start,
		ptc.order_id,
		ptc.created_date,
		ptc.submitted_date,
		ptc.paid_date,
		ROW_NUMBER () OVER (PARTITION BY widget_clicked_event_id,product_added_event_id ORDER BY product_clicked_time_start) AS rowno
	FROM app_widget_clicks awc
	LEFT JOIN app_product_added_to_cart ptc
		ON ptc.session_id = awc.session_id
			AND ptc.product_clicked_time_start > awc.widget_clicked_event_time_start
	LEFT JOIN segment.sessions_app sa 
		ON sa.session_id = awc.session_id		
)
, app_final AS (
	SELECT
		w.session_id,
		w.user_id,
		w.anonymous_id,
		w.event_id_page_loaded,
		w.event_time_page_loaded,
		w.campaign_title,
		w.store_country,
		w.customer_type,
		w.widget_clicked_event_id AS event_id_traffic,
		'Widget Clicked' AS event_name_traffic,
		w.marketing_card_traffic,
		w.widget_label_traffic,
		w.marketing_card_position_traffic,
		w.marketing_channel,
		w.product_clicked_event_id AS product_viewed_event_id, --matching name with metric in web sessions for Tableau calculations (not 100% accurate)
		w.product_clicked_time_start AS product_viewed_time_start, --matching name with metric in web sessions for Tableau calculations (not 100% accurate)
		w.product_sku,
		p.category_name,
		p.subcategory_name,
		p.brand,
		p.product_name,
		w.row_no,
		w.product_added_event_id,
		w.product_added_time_start,
		w.order_id,
		w.created_date AS order_created_date,
		w.submitted_date AS order_submitted_date,
		w.paid_date	AS order_paid_date,
		'app session' AS session_origin
	FROM widget_clicks_to_product_clicked w
	LEFT JOIN ods_production.product p 
			ON p.product_sku = w.product_sku
	WHERE campaign_title IS NOT NULL
		AND w.rowno = 1
) 
SELECT w.*
FROM web_final w 
UNION 
SELECT a.*
FROM app_final a 
;

BEGIN TRANSACTION;

DELETE FROM dm_commercial.commercial_campaigns_tracking 
WHERE event_time_page_loaded::date >= current_date-3;

INSERT INTO dm_commercial.commercial_campaigns_tracking
SELECT *
FROM tmp_commercial_campaigns_tracking;

END TRANSACTION;

DROP TABLE IF EXISTS tmp_commercial_campaigns_tracking;
