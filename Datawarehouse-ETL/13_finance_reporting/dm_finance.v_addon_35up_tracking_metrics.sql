CREATE OR REPLACE VIEW dm_finance.v_addon_35up_tracking_metrics as 
WITH a AS (
SELECT
	*
FROM
	dm_finance.addon_35up_tracking
WHERE
	accessory_type LIKE '%external-physical-otp%'
)
SELECT 
	event_time::date AS reporting_date
	,count(DISTINCT 
		CASE
			WHEN event_name = 'Accessory Drawer Opened'
			THEN event_id
		END
	) AS number_of_drawer_open
	,count(DISTINCT 
		CASE
			WHEN event_name in ('Accessory Details Clicked','Accessory Added')
			THEN event_id
		END
	) AS number_of_clicks
	,count(DISTINCT 
		CASE
			WHEN event_name = 'Accessory Added'
			THEN event_id
		END
	) AS number_of_added_to_cart
	,count(DISTINCT 
		CASE
			WHEN event_name = 'Accessory Order Submitted'
			THEN event_id
		END
	) AS number_of_order_submitted
FROM 
	a
GROUP BY 
	1
;

GRANT SELECT  on  dm_finance.v_addon_35up_tracking_metrics TO tableau;
	
