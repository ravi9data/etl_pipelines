DROP VIEW dm_risk.v_recommendation_engine_report;
CREATE VIEW dm_risk.v_recommendation_engine_report AS
SELECT 
	event_date,
	count(DISTINCT session_id) AS num_sessions,
	count(DISTINCT snowplow_user_id) AS num_users,
	count(DISTINCT customer_id) AS num_customers,
	SUM(is_recommendation_enabled) AS is_recommendation_enabled ,
	SUM(product_views) AS product_views ,
	SUM(alsoLike_clicks) AS alsoLike_clicks ,
	SUM(recommendation_clicks) AS recommendation_clicks,
	SUM(product_views_from_recom_sessions) AS product_views_from_recom_sessions,
	SUM(cart_additions) AS cart_additions,
	SUM(cart_additions_recommendation) AS cart_additions_recommendation,
	SUM(cart_additions_alsoLike) AS cart_additions_alsoLike,
	SUM(cart_removals) AS cart_removals,
	SUM(cart_removals_recommendation) AS cart_removals_recommendation,
	SUM(cart_removals_alsoLike) AS cart_removals_alsoLike,
	SUM(created_orders) AS created_orders,
	SUM(created_orders_containing_recommendation) AS created_orders_containing_recommendation,
	SUM(created_orders_containing_alsoLike) AS created_orders_containing_alsoLike,
	SUM(submitted_orders) AS submitted_orders,
	SUM(is_recommendation_product_submitted) AS is_recommendation_product_submitted,
	SUM(is_alsoLike_product_submitted) AS is_alsoLike_product_submitted,
	SUM(products_paid) AS products_paid,
	SUM(is_recommendation_product_paid) AS is_recommendation_product_paid,
	SUM(is_alsoLike_product_paid) AS is_alsoLike_product_paid
FROM dm_risk.recommendation_engine
GROUP BY 1
WITH NO SCHEMA BINDING; 