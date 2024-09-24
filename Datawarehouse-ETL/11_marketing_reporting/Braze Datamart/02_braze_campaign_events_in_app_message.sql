CREATE TEMP TABLE braze_events_in_app_message_temp
AS
WITH inapp_raw AS
(
	SELECT 
		i.user_id,
		i.external_user_id AS customer_id,
		i.platform,
		i.os_version,
		i.campaign_id,
		i.message_variation_id,
		i.canvas_id,
		i.canvas_variation_id,
		i.canvas_step_id,
		timestamp 'epoch' + i."time" * interval '1 second' AS impression_time ,
		CASE 
			WHEN i.campaign_id IS NOT NULL
			THEN LEAD(impression_time) OVER (PARTITION BY i.user_id, i.campaign_id, i.message_variation_id ORDER BY impression_time ASC)
		END AS next_impression_time_campaign,
		CASE 
			WHEN i.canvas_id IS NOT NULL
			THEN LEAD(impression_time) OVER (PARTITION BY i.user_id, i.canvas_id, i.canvas_variation_id, i.canvas_step_id ORDER BY impression_time ASC)
		END AS next_impression_time_canvas,
		i.id AS impression_id
	FROM stg_external_apis.braze_inappmessage_impression_event i
	WHERE (i.campaign_id IS NOT NULL OR i.canvas_id IS NOT NULL)
		AND impression_time::date >= DATEADD('day', -7, current_date)
),
inapp_data AS 
(
	SELECT 
		imp.*,
		timestamp 'epoch' + COALESCE(c."time", c_canv."time") * interval '1 second' AS clicked_time ,
		CASE WHEN clicked_time > impression_time
	    		THEN DATEDIFF('seconds', impression_time , clicked_time)
	    	 ELSE NULL
        END AS seconds_til_click,
		COALESCE(c.id, c_canv.id) AS clicked_id
	FROM inapp_raw imp	
	LEFT JOIN stg_external_apis.braze_inappmessage_click_event c
		ON imp.campaign_id = c.campaign_id 
		AND imp.message_variation_id = c.message_variation_id 
		AND imp.user_id = c.user_id
		AND timestamp 'epoch' + c."time" * interval '1 second' BETWEEN impression_time AND COALESCE(next_impression_time_campaign, current_date )
		AND impression_time::date >= DATEADD('day', -7, current_date)
	LEFT JOIN stg_external_apis.braze_inappmessage_click_event c_canv
		ON imp.canvas_id = c_canv.canvas_id 
		AND imp.canvas_variation_id = c_canv.canvas_variation_id
		AND imp.canvas_step_id = c_canv.canvas_step_id
		AND imp.user_id = c_canv.user_id
		AND timestamp 'epoch' + c_canv."time" * interval '1 second' BETWEEN impression_time AND COALESCE(next_impression_time_canvas, current_date )
		AND impression_time::date >= DATEADD('day', -7, current_date)
)
SELECT 
	campaign_id,
	message_variation_id ,
	canvas_id,
	canvas_variation_id,
	canvas_step_id,
	user_id ,
	customer_id ,
	platform,
	os_version,
	impression_time::date AS impression_date,
	MIN(clicked_time) AS first_time_clicked,
	MAX(CASE WHEN impression_id IS NOT NULL THEN 1 ELSE 0 END) AS is_impression,
	COUNT(DISTINCT impression_id) AS is_impression_overall,
	MAX(CASE WHEN clicked_id IS NOT NULL THEN 1 ELSE 0 END) AS is_clicked,
	COUNT(DISTINCT clicked_id) AS is_clicked_overall,
	MIN(seconds_til_click) AS seconds_til_first_click
FROM inapp_data  
GROUP BY 1,2,3,4,5,6,7,8,9,10;


BEGIN TRANSACTION;

-- Campaign Deletions
DELETE FROM dm_marketing.braze_campaign_events_in_app_message
USING braze_events_in_app_message_temp  tmp
WHERE 
	braze_campaign_events_in_app_message.campaign_id = tmp.campaign_id AND
	braze_campaign_events_in_app_message.message_variation_id = tmp.message_variation_id AND
	braze_campaign_events_in_app_message.user_id = tmp.user_id  AND 
	braze_campaign_events_in_app_message.customer_id = tmp.customer_id AND 
	braze_campaign_events_in_app_message.platform = tmp.platform AND
	braze_campaign_events_in_app_message.os_version = tmp.os_version AND
	braze_campaign_events_in_app_message.impression_date = tmp.impression_date ;

-- Canvas Deletions
DELETE FROM dm_marketing.braze_campaign_events_in_app_message
USING braze_events_in_app_message_temp  tmp
WHERE 
	braze_campaign_events_in_app_message.canvas_id = tmp.canvas_id  AND 
	braze_campaign_events_in_app_message.canvas_variation_id = tmp.canvas_variation_id  AND 
	braze_campaign_events_in_app_message.canvas_step_id = tmp.canvas_step_id  AND 
	braze_campaign_events_in_app_message.user_id = tmp.user_id  AND 
	braze_campaign_events_in_app_message.customer_id = tmp.customer_id AND 
	braze_campaign_events_in_app_message.platform = tmp.platform AND
	braze_campaign_events_in_app_message.os_version = tmp.os_version AND
	braze_campaign_events_in_app_message.impression_date = tmp.impression_date ;

INSERT INTO dm_marketing.braze_campaign_events_in_app_message
SELECT *
FROM braze_events_in_app_message_temp ;

END TRANSACTION;

DROP TABLE braze_events_in_app_message_temp ;

