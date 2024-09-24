
SELECT 
	gc.order_submitted_date::date,
	gc.country,
	CASE
		WHEN oc.os ilike 'web' THEN 'web'
		WHEN oc.os in ('android','ios') THEN 'app'
		WHEN o.device = 'n/a' THEN 'app'
		ELSE NULL
	END AS app_web,
	COALESCE(count(DISTINCT gc.subscription_id),0) AS subscriptions_submitted,
	-- Subscriptions Submitted
	-- Orders Submitted
	COALESCE(count(DISTINCT gc.order_id),0) AS orders_submitted,
FROM 
LEFT JOIN 
	master.order o
	ON o.order_id = gc.order_id
LEFT JOIN 
	traffic.order_conversions oc  
    ON o.order_id = oc.order_id
WHERE 
	gc.country in ('Austria','United States','Germany')
GROUP BY 
	1,2,3
)
,
pdp AS 
(SELECT 
	created_date,
	country,
	app_web,
	sum(viewed) viewed,
	sum(traffic_daily_unique_sessions) traffic_daily_unique_sessions
FROM 
	dm_finance.grove_care_pdp 
WHERE 
	AND country in ('Austria','United States','Germany')
GROUP BY 
	1,2,3
)
SELECT 
	COALESCE(a.order_submitted_date,b.created_date) AS order_submitted_date,
	COALESCE(a.country,b.country) AS country,
	COALESCE(a.app_web,b.app_web) AS app_web,
	COALESCE(viewed,0) AS viewed,
	COALESCE(subscriptions_submitted,0) AS subscriptions_submitted,
	COALESCE(subscriptions_submitted_gc,0) AS subscriptions_submitted_gc,
	COALESCE(orders_submitted,0) AS orders_submitted,
	COALESCE(orders_submitted_gc,0) AS orders_submitted_gc,
	COALESCE(traffic_daily_unique_sessions,0) AS traffic_daily_unique_sessions,
	COALESCE(subscriptions_submitted_0+subscriptions_submitted_50+subscriptions_submitted_90,0) AS subscriptions_submitted_0_50_90,
	COALESCE(orders_submitted_0+orders_submitted_50+orders_submitted_90,0) AS orders_submitted_0_50_90
FROM 
FULL JOIN 
	pdp b ON a.order_submitted_date = b.created_date
	AND a.country = b.country
	AND a.app_web = b.app_web
WHERE 
	COALESCE(a.order_submitted_date,b.created_date) < (current_Date - 1)   
WITH NO SCHEMA BINDING  
;
