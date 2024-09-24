CREATE TEMP TABLE braze_events_push_notification_temp  
AS 
WITH raw_push AS ( 
	SELECT 
		n.user_id,
		n.external_user_id AS customer_id,
		n.platform,
		n.campaign_id,
		n.message_variation_id, 
		n.canvas_id,
		n.canvas_variation_id,
		n.canvas_step_id,
		timestamp 'epoch' + n."time" * interval '1 second' AS sent_time ,
		timestamp 'epoch' + COALESCE(o."time", o_canv."time") * interval '1 second' AS opened_time ,
		timestamp 'epoch' + COALESCE(b."time", b_canv."time") * interval '1 second' AS bounced_time ,
		CASE WHEN opened_time > sent_time  
        	THEN DATEDIFF('seconds', sent_time, opened_time)
        END AS seconds_til_open,
		n.id AS sent_id,
		COALESCE(o.id, o_canv.id) AS opened_id,
		COALESCE(b.id, b_canv.id) AS bounced_id
	FROM stg_external_apis.braze_pushnotification_send_event n
	-- campaign joins
	LEFT JOIN stg_external_apis.braze_pushnotification_open_event o 
	  ON n.campaign_id = o.campaign_id 
	 AND n.message_variation_id = o.message_variation_id 
	 AND n.user_id = o.user_id
	LEFT JOIN stg_external_apis.braze_pushnotification_bounce_event b
	  ON n.campaign_id = b.campaign_id 
	 AND n.message_variation_id = b.message_variation_id 
	 AND n.user_id = b.user_id
	 -- canvas joins
 	LEFT JOIN stg_external_apis.braze_pushnotification_open_event o_canv 
	  ON n.canvas_id = o_canv.canvas_id 
	 AND n.canvas_variation_id = o_canv.canvas_variation_id
	 AND n.canvas_step_id = o_canv.canvas_step_id
	 AND n.user_id = o_canv.user_id
	LEFT JOIN stg_external_apis.braze_pushnotification_bounce_event b_canv
	  ON n.canvas_id = b_canv.canvas_id 
	 AND n.canvas_variation_id = b_canv.canvas_variation_id
	 AND n.canvas_step_id = b_canv.canvas_step_id
	 AND n.user_id = b_canv.user_id
	WHERE (n.campaign_id IS NOT NULL OR n.canvas_id IS NOT NULL)
		AND  sent_time::date >= DATEADD('day', -7, current_date)
)
SELECT 
	campaign_id,
	message_variation_id,
	canvas_id,
	canvas_variation_id,
	canvas_step_id,
	user_id,
	customer_id,
	platform,
	sent_time::date AS sent_date,
	MIN(sent_time::date) AS first_sent_date,
	MIN(opened_time) AS first_time_opened,
	MAX(CASE WHEN sent_id IS NOT NULL THEN 1 ELSE 0 END) AS is_sent,
	COUNT(DISTINCT sent_id) AS is_sent_overall,
	COUNT(DISTINCT opened_id) AS is_opened_overall,
	MAX(CASE WHEN opened_id IS NOT NULL THEN 1 ELSE 0 END) AS is_opened,
	COUNT(DISTINCT bounced_id) AS is_bounced_overall,
	MAX(CASE WHEN bounced_id IS NOT NULL THEN 1 ELSE 0 END) AS is_bounced,
	MIN(seconds_til_open) AS seconds_til_first_open
FROM raw_push 
GROUP BY 1,2,3,4,5,6,7,8,9;


BEGIN TRANSACTION;

-- Campaign Deletions
DELETE FROM dm_marketing.braze_campaign_events_push_notification
USING braze_events_push_notification_temp  tmp
WHERE 
	braze_campaign_events_push_notification.campaign_id = tmp.campaign_id AND
	braze_campaign_events_push_notification.message_variation_id = tmp.message_variation_id AND
	braze_campaign_events_push_notification.user_id = tmp.user_id  AND 
	braze_campaign_events_push_notification.customer_id = tmp.customer_id AND 
	braze_campaign_events_push_notification.platform = tmp.platform AND
	braze_campaign_events_push_notification.sent_date = tmp.sent_date ;

-- Canvas Deletions
DELETE FROM dm_marketing.braze_campaign_events_push_notification
USING braze_events_push_notification_temp  tmp
WHERE 
	braze_campaign_events_push_notification.canvas_id  = tmp.canvas_id AND
	braze_campaign_events_push_notification.canvas_variation_id  = tmp.canvas_variation_id AND
	braze_campaign_events_push_notification.canvas_step_id = tmp.canvas_step_id AND	
	braze_campaign_events_push_notification.user_id = tmp.user_id  AND 
	braze_campaign_events_push_notification.customer_id = tmp.customer_id AND 
	braze_campaign_events_push_notification.platform = tmp.platform AND
	braze_campaign_events_push_notification.sent_date = tmp.sent_date ;

INSERT INTO dm_marketing.braze_campaign_events_push_notification
SELECT *
FROM braze_events_push_notification_temp ;

END TRANSACTION;

DROP TABLE braze_events_push_notification_temp ;
