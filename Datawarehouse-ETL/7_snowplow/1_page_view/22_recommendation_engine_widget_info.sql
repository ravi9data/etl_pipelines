CREATE TEMP TABLE recommendation_engine_widget_temp
AS 
WITH recommendation_clicks_raw AS (
	SELECT 
	    domain_sessionid AS session_id
	    , domain_userid AS snowplow_user_id
	    , user_id AS customer_id
	    , collector_tstamp AS event_timestamp
	    , event_id
	    , sub_category 
	    , product_sku 
	    , pc_position
	    , json_extract_path_text(se_property, 'tracingId') as tracing_id
	FROM scratch.se_events_flat
	WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	  AND se_action = 'productClick'
	  AND product_click = 'recommendation engine' 
	  AND geo_country = 'DE'
)
, recommendation_clicks AS (
	SELECT 
		rcr.session_id,
		rcr.snowplow_user_id,
		rcr.customer_id,
		rcr.event_timestamp,
		rcr.event_id,
		rcr.sub_category,
		rcr.product_sku,
		rcr.pc_position,
		rcr.tracing_id,
		remr.is_recommended_by_model 
	FROM recommendation_clicks_raw rcr
	LEFT JOIN dm_risk.recommendation_engine_model_response remr
		ON rcr.tracing_id = remr.tracing_id 
	   AND rcr.product_sku = remr.product_sku
)
, recommendation_product_views AS (
	SELECT 
	    domain_sessionid AS session_id
	    , domain_userid AS snowplow_user_id
	    , user_id AS customer_id
	    , collector_tstamp AS event_timestamp
	    , event_id
	    , sub_category AS subcategory
	    , product_sku
	FROM scratch.se_events_flat f
	WHERE f.collector_tstamp::date >= DATEADD('day', -7, current_date)
	  AND EXISTS (SELECT NULL FROM recommendation_clicks rs
	  			  WHERE f.domain_sessionid = rs.session_id)
	  AND f.se_action = 'pdpView'
	  AND f.geo_country = 'DE'
)
, raw1 AS (
	SELECT 
		rc.session_id,
		rc.snowplow_user_id ,
		rc.customer_id ,
		rc.pc_position::int + 1 AS widget_position, --POSITION starting WITH 0
		rc.event_id AS recom_event_id,
		rc.event_timestamp AS recom_event_timestamp,
		rc.sub_category AS recom_subcategory,
		rc.product_sku AS recom_product_sku,
		rc.is_recommended_by_model,
		rpv.event_timestamp AS pdp_event_timestamp,
		rpv.event_id AS pdp_event_id,
		rpv.subcategory AS pdp_subcategory,
		rpv.product_sku AS pdp_product_sku,
		ROW_NUMBER() OVER (PARTITION BY rc.session_id, rc.event_timestamp ORDER BY rpv.event_timestamp DESC) AS rowno
	FROM recommendation_clicks rc
	INNER JOIN recommendation_product_views rpv
	  ON rc.session_id = rpv.session_id
	 AND rc.event_timestamp > rpv.event_timestamp 
)
, submissions AS (
	SELECT DISTINCT 
		f.domain_sessionid session_id,
		f.se_label,
		f.order_id,
		f.product_sku product_sku_list,
		oi.product_sku 
	FROM scratch.se_events_flat f
	INNER JOIN (SELECT domain_sessionid session_id, count(DISTINCT se_label) AS label_check
				FROM scratch.se_events_flat 
				WHERE se_action = 'orderSubmitted' 
				  AND se_label IN ('enter', 'exit.confirm')
				  AND collector_tstamp::date >= DATEADD('day', -7, current_date)
				GROUP BY 1 HAVING label_check > 1 --make sure the product was submitted 
	) f2
		ON f2.session_id = f.domain_sessionid 
	LEFT JOIN ods_production.order_item oi 
	  ON f.order_id = oi.order_id 
	WHERE se_action = 'orderSubmitted' 
	  AND se_label = 'enter' --the enter event includes the product_sku 
	  AND collector_tstamp::date >= DATEADD('day', -7, current_date)
)
, raw2 AS ( 
		SELECT 
		r.session_id,
		r.snowplow_user_id ,
		r.customer_id ,
		r.pdp_event_timestamp ,
		r.pdp_event_id ,
		r.pdp_subcategory ,
		r.pdp_product_sku ,
		r.recom_event_timestamp ,
		r.recom_event_id ,
		r.recom_subcategory,
		r.recom_product_sku ,
		r.is_recommended_by_model ,
		r.widget_position,
		datediff('seconds', r.pdp_event_timestamp, r.recom_event_timestamp) AS diff_in_sec_until_click,
		sub.order_id as order_id,
		CASE WHEN s.subscription_id IS NOT NULL THEN 1 ELSE 0 END is_recomm_order_paid
	FROM raw1 r
	LEFT JOIN submissions sub
		ON sub.session_id = r.session_id 
	   AND sub.product_sku = r.recom_product_sku 
	LEFT JOIN master.subscription s 
        ON s.order_id = sub.order_id
       AND s.product_sku = sub.product_sku 
	WHERE rowno = 1
)
SELECT DISTINCT
	r.session_id,
	r.snowplow_user_id ,
	r.customer_id ,
	r.pdp_event_timestamp ,
	r.pdp_event_id,
	r.pdp_subcategory ,
	r.pdp_product_sku ,
	r.recom_event_timestamp ,
	r.recom_event_id ,
	r.recom_subcategory,
	r.recom_product_sku ,
	r.is_recommended_by_model ,
	r.widget_position,
	r.diff_in_sec_until_click,
	r.order_id,
	r.is_recomm_order_paid
FROM raw2 r;

BEGIN TRANSACTION;
DELETE FROM dm_risk.recommendation_engine_widget_info
USING recommendation_engine_widget_temp  tmp
WHERE recommendation_engine_widget_info.session_id = tmp.session_id AND
recommendation_engine_widget_info.snowplow_user_id = tmp.snowplow_user_id AND
recommendation_engine_widget_info.pdp_event_id = tmp.pdp_event_id AND 
recommendation_engine_widget_info.recom_event_id = tmp.recom_event_id ;

INSERT INTO dm_risk.recommendation_engine_widget_info
SELECT *
FROM recommendation_engine_widget_temp ;

END TRANSACTION;

DROP TABLE recommendation_engine_widget_temp ;

