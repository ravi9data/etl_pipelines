CREATE OR REPLACE VIEW dm_risk.v_recommendation_engine_recommendations AS 
WITH total_recommendations_seen AS (
	SELECT
		event_timestamp::date AS event_date,
		is_recommendation_session,
		COUNT(*) AS events,
		SUM(CASE WHEN products_recommended IS NOT NULL THEN 1 ELSE 0 END) AS recommendations
	FROM dm_risk.recommendation_engine_recommendations
	GROUP BY 1,2
)
,raw_ AS (
	SELECT DISTINCT
		r.event_id,
		r.is_recommendation_session ,
		r.session_id,
		r.snowplow_user_id ,
		r.event_timestamp ,
		r.product_sku ,
		r.subcategory ,
		p2.product_name,
		p.product_name product_name_recommended,
		p.category_name AS category_recommended ,
		p.subcategory_name AS subcategory_recommended,
		trs.events AS num_events_per_day,
		trs.recommendations AS num_recommendations_per_day
	FROM dm_risk.recommendation_engine_recommendations r
	LEFT JOIN ods_production.product p 
	  ON POSITION(p.product_sku IN r.products_recommended) > 0
	LEFT JOIN ods_production.product p2 
	  ON p2.product_sku = r.product_sku 
	LEFT JOIN total_recommendations_seen trs
	  ON trs.event_date = r.event_timestamp::date
	 AND trs.is_recommendation_session = r.is_recommendation_session 
	WHERE products_recommended IS NOT NULL
)
SELECT 
	event_timestamp::date AS event_date,
	is_recommendation_session ,
	num_events_per_day ,
	num_recommendations_per_day ,
	CASE WHEN is_recommendation_session = 1
		 THEN subcategory 
		 ELSE 'alsoLike'
 	END AS subcategory ,
	CASE WHEN is_recommendation_session = 1
		 THEN product_name  
		 ELSE 'alsoLike'
 	END AS product_name  ,
	CASE WHEN is_recommendation_session = 1
		 THEN category_recommended  
		 ELSE 'alsoLike'
 	END AS category_recommended  ,
	CASE WHEN is_recommendation_session = 1
		 THEN subcategory_recommended  
		 ELSE 'alsoLike'
 	END AS subcategory_recommended ,
	CASE WHEN is_recommendation_session = 1
		 THEN product_name_recommended 
		 ELSE 'alsoLike'
 	END AS product_name_recommended  ,
	COUNT(DISTINCT event_id) num_events,
	COUNT(DISTINCT session_id) num_sessions,
	COUNT(DISTINCT snowplow_user_id) num_users
FROM raw_
GROUP BY 1,2,3,4,5,6,7,8,9
WITH NO SCHEMA BINDING;