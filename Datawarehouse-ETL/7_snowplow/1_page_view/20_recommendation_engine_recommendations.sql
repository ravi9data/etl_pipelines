CREATE  TEMP TABLE recommended_engine_recommendation_temp
AS
WITH recommendation_engine AS (
	SELECT 
		session_id, 
		max(is_recommendation_enabled) AS is_recommendation_session 
	FROM dm_risk.recommendation_engine
	WHERE  event_date ::date >= DATEADD('day', -7, current_date)
	GROUP BY 1
)
,pdp_views AS (
	SELECT
		f.domain_sessionid session_id, 
		f.domain_userid snowplow_user_id, 
		f.event_id,
		f.collector_tstamp event_timestamp,
		f.product_sku,
		f.sub_category AS subcategory,
		re.is_recommendation_session
	FROM recommendation_engine re
	INNER JOIN scratch.se_events_flat f
	   ON f.domain_sessionid = re.session_id 
	WHERE f.se_action = 'pdpView'  
	 AND f.geo_country = 'DE'
	AND f.collector_tstamp::date >= DATEADD('day', -7, current_date)
)
,impressions AS (
	SELECT
		f.domain_sessionid session_id, 
		f.domain_userid snowplow_user_id, 
		f.collector_tstamp event_timestamp,
		f.product_sku
	FROM recommendation_engine re
	INNER JOIN scratch.se_events_flat f
	   ON f.domain_sessionid = re.session_id 
	WHERE f.se_action = 'productImpression' 
	  AND f.product_click = 'pdp'
	   AND f.geo_country = 'DE'
	  AND f.collector_tstamp::date >= DATEADD('day', -7, current_date)
) 
SELECT 
	pv.session_id ,
	pv.snowplow_user_id ,
	pv.event_id ,
	pv.event_timestamp ,
	pv.product_sku ,
	pv.subcategory ,
	pv.is_recommendation_session ,
	LISTAGG(DISTINCT i.product_sku, ',') WITHIN GROUP (ORDER BY i.event_timestamp) AS products_recommended
FROM pdp_views pv
LEFT JOIN impressions i
  ON pv.session_id = i.session_id
 AND pv.snowplow_user_id = i.snowplow_user_id 
 AND pv.event_timestamp < i.event_timestamp
 AND i.event_timestamp <= DATEADD(MINUTE, 2, pv.event_timestamp)
GROUP BY 1,2,3,4,5,6,7;

BEGIN TRANSACTION;
DELETE FROM dm_risk.recommendation_engine_recommendations
USING recommended_engine_recommendation_temp  tmp
WHERE recommendation_engine_recommendations.event_id = tmp.event_id ;

INSERT INTO dm_risk.recommendation_engine_recommendations
SELECT *
FROM recommended_engine_recommendation_temp ;

END TRANSACTION;

DROP TABLE recommended_engine_recommendation_temp ;