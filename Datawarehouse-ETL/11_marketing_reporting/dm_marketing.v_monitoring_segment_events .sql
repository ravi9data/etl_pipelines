CREATE OR REPLACE VIEW dm_marketing.v_monitoring_segment_events AS 
SELECT 
	event_name,
	min(event_time) AS first_event,
	max(event_time) AS last_event,
	DATEDIFF('day',last_event, current_date) AS days_since_last_event,
	count(DISTINCT CASE WHEN event_time >= DATEADD('day',-5,current_date) THEN event_id END) AS number_events_last_5_days,
	count(DISTINCT CASE WHEN event_time < DATEADD('day',-5,current_date)
							AND  event_time >= DATEADD('day',-10,current_date) THEN event_id END) AS number_events_last_10_days,
	count(DISTINCT CASE WHEN event_time < DATEADD('day',-10,current_date)
							AND  event_time >= DATEADD('day',-15,current_date) THEN event_id END) AS number_events_last_15_days
FROM segment.all_events
GROUP BY 1
ORDER BY 1
WITH NO SCHEMA BINDING;