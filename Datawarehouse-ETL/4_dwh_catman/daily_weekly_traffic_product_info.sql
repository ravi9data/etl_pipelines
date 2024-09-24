CREATE TEMP TABLE tmp_commercial_daily_weekly_sessions_product_info AS  
WITH daily_traffic AS (	
	SELECT 
		page_view_date,
		page_type,
		page_type_detail,
		CASE 
			WHEN pv.store_label IN ('Grover - Spain online')
		 		THEN 'Spain'
		 	WHEN pv.store_label IN ('Grover - UK online')
		 		THEN 'Grover-UK'
		 	WHEN pv.store_label IN ('Grover - Austria online')
		 		THEN 'Austria'
		 	WHEN pv.store_label IN ('Grover - Netherlands online')
		 		THEN 'Netherlands'
		 	WHEN pv.store_label IN ('Grover - USA old online')
		 		THEN 'Grover-US old'
		 	WHEN pv.store_label IN ('Grover - United States online')
		 		THEN 'United States'
		 	ELSE 'Germany'
		 	END AS store_label,
		count(DISTINCT page_view_id) AS page_views,
		count(DISTINCT session_id) AS sessions_number
	FROM traffic.page_views pv
	WHERE page_view_date::date >= current_date -3 --BETWEEN DATEADD('month', -3, current_date) AND DATEADD('day', -1, current_date)
		AND page_type IN ('pdp','category','sub-category','landing-page','home')
		 AND store_label NOT IN ('Grover - USA old online','Grover - UK online')
	GROUP BY 1,2,3,4
)
SELECT 
	t.page_view_date,
	t.page_type,
	t.page_type_detail,
	t.store_label,
	p.category_name,
	p.subcategory_name,
	p.brand,
	p.product_sku,
	p.product_name,
	sum(page_views) AS page_views,
	sum(sessions_number) AS sessions_number
FROM daily_traffic t
LEFT JOIN ods_production.product p
	ON p.product_sku = t.page_type_detail	
GROUP BY 1,2,3,4,5,6,7,8,9
;

BEGIN TRANSACTION;

DELETE FROM dm_commercial.daily_weekly_sessions_product_info
WHERE page_view_date::date >= current_date -3;

INSERT INTO dm_commercial.daily_weekly_sessions_product_info
SELECT *
FROM tmp_commercial_daily_weekly_sessions_product_info;

END TRANSACTION;

DROP TABLE IF EXISTS tmp_commercial_daily_weekly_sessions_product_info;
