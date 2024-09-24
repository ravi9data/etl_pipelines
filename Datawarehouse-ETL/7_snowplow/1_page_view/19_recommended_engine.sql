CREATE TEMP TABLE recommended_engine_temp_table
AS
WITH all_sessions AS (
	SELECT 
	    domain_sessionid AS session_id
	    , domain_userid AS snowplow_user_id
	    , collector_tstamp::date AS event_date
	    , MIN(user_id) AS customer_id
	    , MAX(CASE WHEN recommendation_engine  = 'enabled' THEN 1 ELSE 0 END) is_recommendation_enabled
	    , COUNT(DISTINCT CASE WHEN product_sku <> '' AND se_action = 'pdpView' THEN event_id END) product_views
	    , COUNT(DISTINCT CASE WHEN product_click = 'pdp' AND se_action = 'productClick' THEN event_id END) alsoLike_clicks
	FROM scratch.se_events_flat f
	WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	  AND domain_sessionid IS NOT NULL
	AND f.geo_country = 'DE'
	GROUP BY 1,2,3
)
, recommendation_sessions AS (
	SELECT 
	    domain_sessionid AS session_id
	    , domain_userid AS snowplow_user_id
	    , collector_tstamp::date AS event_date
		, MAX(CASE WHEN recommendation_engine  = 'enabled' THEN 1 ELSE 0 END) recommendation_enabled
		, COUNT(DISTINCT CASE WHEN product_sku <> '' AND se_action = 'pdpView' THEN event_id END) product_views_from_recom_sessions
		, COUNT(DISTINCT CASE WHEN product_click = 'recommendation engine' AND se_action = 'productClick' THEN event_id END) recommendation_clicks
	FROM scratch.se_events_flat 
	WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	AND geo_country = 'DE'
	GROUP BY 1,2,3
	HAVING recommendation_enabled = 1 AND product_views_from_recom_sessions >= 1
)
, click_products AS (
        SELECT            
                domain_sessionid AS session_id
            , domain_userid AS snowplow_user_id
            , collector_tstamp::date AS event_date
            , LISTAGG(DISTINCT CASE WHEN product_click = 'recommendation engine' AND se_action = 'productClick' THEN product_sku END, ',') AS recommendation_click_products
            , LISTAGG(DISTINCT CASE WHEN product_click = 'pdp' AND se_action = 'productClick' THEN product_sku END, ',') AS alsoLike_click_products
        FROM scratch.se_events_flat 
        WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
        AND geo_country = 'DE'
        GROUP BY 1,2,3
        HAVING recommendation_click_products IS NOT NULL OR alsoLike_click_products IS NOT NULL
)
, cartActions AS (
	SELECT 
	    se.domain_sessionid AS session_id
	    , se.collector_tstamp::date event_date
	    , se.se_action
	    , COUNT(DISTINCT se.se_label) AS num_cart_actions
	    , COUNT(DISTINCT rs.recom_product) AS num_cart_actions_recommendation 
	    , COUNT(DISTINCT al.alsoLike_product) AS num_cart_actions_alsoLike
	FROM scratch.se_events_flat se 
	LEFT JOIN (SELECT DISTINCT 
				   domain_sessionid session_id, 
				   collector_tstamp::date event_date, 
				   CASE WHEN product_click = 'recommendation engine' AND se_action = 'productClick' THEN product_sku END AS recom_product
	           FROM scratch.se_events_flat
	           WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	           AND geo_country = 'DE') rs
	  ON rs.session_id = se.domain_sessionid 
	 AND rs.event_date = se.collector_tstamp::date
	 AND rs.recom_product = se.se_label
	LEFT JOIN (SELECT DISTINCT 
				   domain_sessionid session_id, 
				   collector_tstamp::date event_date, 
				   CASE WHEN product_click = 'pdp' AND se_action = 'productClick' THEN product_sku END AS alsoLike_product
	           FROM scratch.se_events_flat
	           WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	           AND geo_country = 'DE') al
	  ON al.session_id = se.domain_sessionid 
	 AND al.event_date = se.collector_tstamp::date
	 AND al.alsoLike_product = se.se_label
	WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	AND geo_country = 'DE'
	  AND se_action IN ('addToCart', 'removeFromCart')
	GROUP BY 1,2,3
)
, createdOrders AS (
	SELECT 
	    se.domain_sessionid AS session_id
	    , se.collector_tstamp::date event_date
	    , COUNT(DISTINCT se.order_id) AS num_orders
	    , MAX(CASE WHEN rs.recom_product IS NOT NULL THEN 1 ELSE 0 END) AS num_orders_containing_recommendation
	    , MAX(CASE WHEN al.alsoLike_product IS NOT NULL THEN 1 ELSE 0 END) AS num_orders_containing_alsoLike
	FROM scratch.se_events_flat se 
	LEFT JOIN (SELECT DISTINCT 
				   domain_sessionid session_id, 
				   collector_tstamp::date event_date, 
				   CASE WHEN product_click = 'recommendation engine' AND se_action = 'productClick' THEN product_sku END AS recom_product
	           FROM scratch.se_events_flat
	           WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	           and  geo_country = 'DE') rs
	  ON rs.session_id = se.domain_sessionid 
	 AND rs.event_date = se.collector_tstamp::date
	 AND rs.recom_product = se.se_label
	LEFT JOIN (SELECT DISTINCT 
				   domain_sessionid session_id, 
				   collector_tstamp::date event_date, 
				   CASE WHEN product_click = 'pdp' AND se_action = 'productClick' THEN product_sku END AS alsoLike_product
	           FROM scratch.se_events_flat
	           WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	           AND geo_country = 'DE') al
	  ON al.session_id = se.domain_sessionid 
	 AND al.event_date = se.collector_tstamp::date
	 AND al.alsoLike_product = se.se_label
	WHERE collector_tstamp::date >= DATEADD('day', -7, current_date)
	  AND se_action IN ('addToCart')
	  AND se.geo_country = 'DE'
	GROUP BY 1,2
)
, submissions AS (
	SELECT DISTINCT 
		f.domain_sessionid session_id
		, f.collector_tstamp::date AS event_date
		, f.se_label
		, f.order_id
		, f.product_sku product_sku_list
		, oi.product_sku
		, s3.subscription_id --paid subscriptions
	FROM scratch.se_events_flat f
	INNER JOIN (SELECT domain_sessionid session_id, count(DISTINCT se_label) AS label_check
				FROM scratch.se_events_flat 
				WHERE se_action = 'orderSubmitted' 
				  AND se_label IN ('enter', 'exit.confirm')
				  AND collector_tstamp::date >= DATEADD('day', -7, current_date)
				  AND geo_country = 'DE'
				GROUP BY 1 HAVING label_check > 1 --make sure the product was submitted 
	) f2
		ON f2.session_id = f.domain_sessionid 
	LEFT JOIN ods_production.order_item oi 
	  ON f.order_id = oi.order_id 
	LEFT JOIN master.subscription s3 
	  ON f.order_id = s3.order_id
	 AND oi.product_sku = s3.product_sku 
	WHERE se_action = 'orderSubmitted' 
	  AND se_label = 'enter' --the enter event includes the product_sku 
	  AND collector_tstamp::date >= DATEADD('day', -7, current_date)
	  AND geo_country = 'DE'
)
, submittedOrders AS (
	SELECT 
		a.session_id
		, a.event_date
		, COUNT(DISTINCT s.order_id) AS submitted_orders
		, COUNT(DISTINCT
		  CASE WHEN POSITION(s.product_sku IN cp.recommendation_click_products) > 0
		       THEN s.order_id
		  END
		  ) AS is_recommendation_product_submitted
		, COUNT(DISTINCT
		  CASE WHEN POSITION(s.product_sku IN cp.alsolike_click_products) > 0
		       THEN s.order_id
		  END
		  ) AS is_alsoLike_product_submitted
		, COUNT(DISTINCT s.subscription_id) AS products_paid
		, COUNT(DISTINCT
		  CASE WHEN POSITION(s.product_sku IN cp.recommendation_click_products) > 0
		       THEN s.subscription_id 
		  END
		  ) AS is_recommendation_product_paid
		, COUNT(DISTINCT
		  CASE WHEN POSITION(s.product_sku IN cp.alsolike_click_products) > 0
		       THEN s.subscription_id 
		  END
		  ) AS is_alsoLike_product_paid
	FROM all_sessions a
	INNER JOIN submissions s
	  ON s.session_id = a.session_id 
	 AND s.event_date = a.event_date 
	LEFT JOIN click_products cp
	  ON a.session_id = cp.session_id
	 AND a.event_date = cp.event_date
	GROUP BY 1,2
)
SELECT 
	s.event_date
	, s.session_id
	, s.snowplow_user_id
	, s.customer_id 
	, s.is_recommendation_enabled 
	, s.product_views 
	, s.alsoLike_clicks 
	, cp.alsoLike_click_products
	, COALESCE(vs.recommendation_clicks, 0) AS recommendation_clicks
	, cp.recommendation_click_products
	, COALESCE(vs.product_views_from_recom_sessions, 0) product_views_from_recom_sessions
	, COALESCE(caa.num_cart_actions, 0) AS cart_additions
	, COALESCE(caa.num_cart_actions_recommendation, 0) AS cart_additions_recommendation
	, COALESCE(caa.num_cart_actions_alsoLike, 0) AS cart_additions_alsoLike
	, COALESCE(car.num_cart_actions, 0) AS cart_removals
	, COALESCE(car.num_cart_actions_recommendation, 0) AS cart_removals_recommendation
	, COALESCE(car.num_cart_actions_alsoLike, 0) AS cart_removals_alsoLike
	, COALESCE(co.num_orders, 0) AS created_orders
	, COALESCE(co.num_orders_containing_recommendation, 0) AS created_orders_containing_recommendation
	, COALESCE(co.num_orders_containing_alsoLike, 0) AS created_orders_containing_alsoLike
	, COALESCE(so.submitted_orders ,0) AS submitted_orders
	, COALESCE(so.is_recommendation_product_submitted ,0) AS is_recommendation_product_submitted
	, COALESCE(so.is_alsoLike_product_submitted ,0) AS is_alsoLike_product_submitted
	, COALESCE(so.products_paid ,0) AS products_paid
	, COALESCE(so.is_recommendation_product_paid , 0) AS is_recommendation_product_paid
	, COALESCE(so.is_alsoLike_product_paid , 0) AS is_alsoLike_product_paid
FROM all_sessions s
LEFT JOIN recommendation_sessions vs
  ON s.session_id = vs.session_id
 AND s.event_date = vs.event_date 
 AND s.snowplow_user_id = vs.snowplow_user_id 
LEFT JOIN click_products cp
  ON s.session_id = cp.session_id
 AND s.event_date = cp.event_date
 AND s.snowplow_user_id = cp.snowplow_user_id
LEFT JOIN cartActions caa
  ON s.session_id = caa.session_id
 AND s.event_date = caa.event_date
 AND caa.se_action = 'addToCart'
LEFT JOIN cartActions car
  ON s.session_id = car.session_id
 AND s.event_date = car.event_date
 AND car.se_action = 'removeFromCart'
LEFT JOIN createdOrders co
  ON co.session_id = s.session_id
 AND co.event_date = s.event_date
LEFT JOIN submittedOrders so
  ON so.session_id = s.session_id
 AND so.event_date = s.event_date 
;

BEGIN TRANSACTION;
DELETE FROM dm_risk.recommendation_engine
USING recommended_engine_temp_table  tmp
WHERE recommendation_engine.session_id = tmp.session_id AND
recommendation_engine.snowplow_user_id = tmp.snowplow_user_id AND
recommendation_engine.event_date = tmp.event_date  ;

INSERT INTO dm_risk.recommendation_engine
SELECT *
FROM recommended_engine_temp_table ;

END TRANSACTION;

DROP TABLE recommended_engine_temp_table ;