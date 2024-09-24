CREATE TEMP TABLE braze_events_email_temp
AS
WITH raw_ AS 
(
	SELECT
		s.campaign_id,
		s.message_variation_id ,
		s.canvas_id,
		s.canvas_variation_id,
		s.canvas_step_id,
		s.user_id ,
		s.external_user_id AS customer_id ,
		timestamp 'epoch' + s."time" * interval '1 second' AS sent_time ,
		timestamp 'epoch' + COALESCE(d."time", d_canv."time") * interval '1 second' AS delivered_time ,
		timestamp 'epoch' + COALESCE(o."time", o_canv."time") * interval '1 second' AS opened_time ,
		timestamp 'epoch' + COALESCE(c."time", c_canv."time") * interval '1 second' AS clicked_time ,
		timestamp 'epoch' + COALESCE(u."time", u_canv."time") * interval '1 second' AS unsubscribed_time ,
		CASE WHEN opened_time > delivered_time 
			THEN DATEDIFF('seconds', delivered_time, opened_time) 
		END AS seconds_til_open,
		CASE WHEN clicked_time > delivered_time 
			THEN DATEDIFF('seconds', delivered_time, clicked_time)
		END AS seconds_til_click,
		CASE WHEN clicked_time > opened_time 
			THEN DATEDIFF('seconds', opened_time, clicked_time)
		END AS seconds_open_to_click,
		s.id AS sent_id,
		COALESCE(d.id, d_canv.id) AS delivered_id,
		COALESCE(o.id, o_canv.id) AS opened_id,
		COALESCE(c.id, c_canv.id) AS clicked_id,
		COALESCE(u.id, u_canv.id) AS unsubscribed_id
	FROM stg_external_apis.braze_sent_event s
	-- Campaign Joins
	LEFT JOIN stg_external_apis.braze_delivery_event d 
	  ON s.campaign_id = d.campaign_id 
	 AND s.message_variation_id = d.message_variation_id 
	 AND s.user_id = d.user_id 
	LEFT JOIN stg_external_apis.braze_open_event o
	  ON s.campaign_id = o.campaign_id 
	 AND s.message_variation_id = o.message_variation_id 
	 AND s.user_id = o.user_id 
	LEFT JOIN stg_external_apis.braze_click_event c
	  ON s.campaign_id = c.campaign_id 
	 AND s.message_variation_id = c.message_variation_id 
	 AND s.user_id = c.user_id 
	LEFT JOIN stg_external_apis.braze_unsubscribe_event u 
	  ON s.campaign_id = u.campaign_id 
	 AND s.message_variation_id = u.message_variation_id 
	 AND s.user_id = u.user_id 
	-- Canvas Joins
	LEFT JOIN stg_external_apis.braze_delivery_event d_canv 
	  ON s.canvas_id = d_canv.canvas_id 
	 AND s.canvas_variation_id = d_canv.canvas_variation_id
	 AND s.canvas_step_id = d_canv.canvas_step_id 
	 AND s.user_id = d_canv.user_id 
	LEFT JOIN stg_external_apis.braze_open_event o_canv
	  ON s.canvas_id = o_canv.canvas_id 
	 AND s.canvas_variation_id = o_canv.canvas_variation_id
	 AND s.canvas_step_id = o_canv.canvas_step_id 
	 AND s.user_id = o_canv.user_id 
	LEFT JOIN stg_external_apis.braze_click_event c_canv
	  ON s.canvas_id = c_canv.canvas_id 
	 AND s.canvas_variation_id = c_canv.canvas_variation_id
	 AND s.canvas_step_id = c_canv.canvas_step_id 
	 AND s.user_id = c_canv.user_id 
	LEFT JOIN stg_external_apis.braze_unsubscribe_event u_canv 
	  ON s.canvas_id = u_canv.canvas_id 
	 AND s.canvas_variation_id = u_canv.canvas_variation_id
	 AND s.canvas_step_id = u_canv.canvas_step_id 
	 AND s.user_id = u_canv.user_id 
	WHERE (s.campaign_id IS NOT NULL OR s.canvas_id IS NOT NULL)
		AND  sent_time::date >= DATEADD('day', -7, current_date)
)
SELECT 
	campaign_id,
	message_variation_id ,
	canvas_id,
	canvas_variation_id,
	canvas_step_id,
	user_id ,
	customer_id ,
	sent_time::date AS sent_date,
	MIN(opened_time) as first_time_opened,
	MIN(clicked_time) as first_time_clicked,
	COUNT(DISTINCT sent_id) AS is_sent_overall,
	MAX(CASE WHEN sent_id IS NOT NULL THEN 1 ELSE 0 END) AS is_sent,
	COUNT(DISTINCT delivered_id) AS is_delivered_overall,
	MAX(CASE WHEN delivered_id IS NOT NULL THEN 1 ELSE 0 END) AS is_delivered,
	COUNT(DISTINCT opened_id) AS is_opened_overall,
	MAX(CASE WHEN opened_id IS NOT NULL THEN 1 ELSE 0 END) AS is_opened,
	COUNT(DISTINCT clicked_id) AS is_clicked_overall,
	MAX(CASE WHEN clicked_id IS NOT NULL THEN 1 ELSE 0 END) AS is_clicked,
	COUNT(DISTINCT unsubscribed_id) AS is_unsubscribed,
	MAX(CASE WHEN clicked_id IS NOT NULL THEN 1 ELSE 0 END) AS is_unsubscribed_overall,
	AVG(seconds_til_open) AS avg_seconds_til_open,
	AVG(seconds_til_click) AS avg_seconds_til_click,
	MIN(seconds_til_open) AS seconds_til_first_open,
	MIN(seconds_til_click) AS seconds_til_first_click
FROM raw_
GROUP BY 1,2,3,4,5,6,7,8;

BEGIN TRANSACTION;

-- Campaign Deletions
DELETE FROM dm_marketing.braze_campaign_events_email
USING braze_events_email_temp  tmp
WHERE 
	braze_campaign_events_email.campaign_id = tmp.campaign_id AND
	braze_campaign_events_email.message_variation_id = tmp.message_variation_id AND
	braze_campaign_events_email.user_id = tmp.user_id  AND 
	braze_campaign_events_email.customer_id = tmp.customer_id AND 
	braze_campaign_events_email.sent_date = tmp.sent_date ;

-- Canvas Deletions
DELETE FROM dm_marketing.braze_campaign_events_email
USING braze_events_email_temp  tmp
WHERE 
	braze_campaign_events_email.canvas_id = tmp.canvas_id AND
	braze_campaign_events_email.canvas_variation_id = tmp.canvas_variation_id AND
	braze_campaign_events_email.canvas_step_id = tmp.canvas_step_id AND	
	braze_campaign_events_email.user_id = tmp.user_id  AND 
	braze_campaign_events_email.customer_id = tmp.customer_id AND 
	braze_campaign_events_email.sent_date = tmp.sent_date ;

INSERT INTO dm_marketing.braze_campaign_events_email
SELECT *
FROM braze_events_email_temp ;

END TRANSACTION;

DROP TABLE braze_events_email_temp ;

