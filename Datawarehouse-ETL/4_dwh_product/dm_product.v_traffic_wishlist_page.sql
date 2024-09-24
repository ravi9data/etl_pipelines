CREATE OR REPLACE VIEW dm_product.v_traffic_wishlist_page AS 
WITH traffic AS (
	SELECT 
		date_trunc('day', page_view_start)::date AS fact_date,
		count(DISTINCT anonymous_id ) AS users,
		count(DISTINCT session_id ) AS sessions,
		count(DISTINCT page_view_id ) AS events,
		count(DISTINCT CASE WHEN t.page_type = 'favorites' THEN anonymous_id END) AS users_favorite,
		count(DISTINCT CASE WHEN t.page_type = 'favorites' THEN session_id END) AS sessions_favorite,
		count(DISTINCT CASE WHEN t.page_type = 'favorites' THEN page_view_id END) AS events_favorite
	FROM traffic.page_views t
	WHERE  page_view_start::date >= dateadd('month', -6, current_date)
		AND page_view_start::date < current_date
	GROUP BY 1
	ORDER BY 1 DESC, 2
)
, orders AS (
	SELECT 
		date_trunc('day', o.submitted_date)::date AS fact_date,
		count(DISTINCT o.order_id ) AS submitted_orders,
		count(DISTINCT CASE WHEN t.page_type = 'favorites' THEN o.order_id END) AS submitted_orders_favorites
	FROM master.ORDER o 
	LEFT JOIN traffic.sessions s 
		ON o.customer_id = s.customer_id
		AND s.session_start::date = o.submitted_date::date
	LEFT JOIN traffic.page_views t
		ON t.session_id = s.session_id
	WHERE o.submitted_date < current_date
		AND o.submitted_date::date >= dateadd('month', -6, current_date)
	GROUP BY 1 
	ORDER BY 1 DESC 
)
SELECT 
	COALESCE(t.fact_date, o.fact_date) AS fact_date,
	COALESCE(t.users, 0) AS traffic_users,
	COALESCE(t.sessions, 0) AS traffic_sessions,
	COALESCE(t.events, 0) AS traffic_events,
	COALESCE(t.users_favorite, 0) AS traffic_users_wishlist,
	COALESCE(t.sessions_favorite, 0) AS traffic_sessions_wishlist,
	COALESCE(t.events_favorite, 0) AS traffic_events_wishlist,
	COALESCE(o.submitted_orders, 0 ) AS submitted_orders,
	COALESCE(o.submitted_orders_favorites, 0 ) AS submitted_orders_wishlist
FROM traffic t 
FULL JOIN orders o 
	ON t.fact_date = o.fact_date
WITH NO SCHEMA BINDING;


GRANT SELECT ON dm_product.v_traffic_wishlist_page TO tableau;
