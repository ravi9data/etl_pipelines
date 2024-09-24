
CREATE OR REPLACE VIEW dm_product.v_conversions_homepage_widgets AS
WITH prep AS (
	SELECT DISTINCT 
		'detail' AS info_level,
		s.session_start::date,
		s.session_id,
		s.marketing_channel,
		s.store_id,
		s.store_label,
		s.store_name,
		CASE WHEN s.customer_id IS NOT NULL THEN 1 ELSE 0 END AS is_customer_logged_in,
		CASE WHEN te.session_id IS NOT NULL THEN 1 ELSE 0 END AS is_there_widget_click,
		CASE WHEN IS_VALID_JSON(te.properties) THEN json_extract_path_text(te.properties, 'widget') end  AS widget,
		CASE WHEN IS_VALID_JSON(te.properties) THEN json_extract_path_text(te.properties, 'widget_name') end AS widget_name,
		CASE WHEN IS_VALID_JSON(te.properties) THEN json_extract_path_text(te.properties, 'recommended_for_sku') end AS recommended_for_sku,
		--
		ROW_NUMBER () OVER (PARTITION BY ac.event_id ORDER BY te.event_time DESC) AS row_no_product_added_to_cart,
		CASE WHEN ac.event_id IS NOT NULL AND row_no_product_added_to_cart = 1 THEN 1 ELSE 0 END AS is_there_product_add_to_cart,
		--
		CASE WHEN row_no_product_added_to_cart = 1 AND o.completed_orders > 0 THEN 1 ELSE 0 END AS is_order_submitted,
		CASE WHEN row_no_product_added_to_cart = 1 AND o.paid_orders > 0 THEN 1 ELSE 0 END AS is_order_paid,
		o.order_id,
		te.event_id AS event_id_product_clicked
	FROM segment.sessions_web s
	LEFT JOIN segment.track_events te 
		ON s.session_id = te.session_id
		AND te.event_name = 'Product Clicked'
		AND CASE WHEN IS_VALID_JSON(te.properties) THEN json_extract_path_text(te.properties, 'widget') end 
			IN ('Countdown Widget', 'Product Listing Widget', 'Lifestyle Widget')
		AND CASE WHEN IS_VALID_JSON(te.properties) THEN lower(json_extract_path_text(te.properties, 'widget_name')) end 
			NOT ILIKE  'alle%'
		AND CASE WHEN IS_VALID_JSON(te.properties) THEN lower(json_extract_path_text(te.properties, 'widget_name')) end 
			NOT ILIKE  'todos los%'
	LEFT JOIN segment.track_events ac 
		ON te.session_id = ac.session_id
		AND te.event_time < ac.event_time
		AND CASE WHEN IS_VALID_JSON(te.properties) THEN 
			COALESCE(NULLIF(json_extract_path_text(te.properties, 'product_sku'), ''),
				NULLIF(json_extract_path_text(te.properties, 'productSKU'), ''),
				NULLIF(json_extract_path_text(te.properties, 'sku'), ''))
				END  =
			CASE WHEN IS_VALID_JSON(ac.properties) THEN 
				COALESCE(NULLIF(json_extract_path_text(ac.properties, 'product_sku'), ''),
				NULLIF(json_extract_path_text(ac.properties, 'productSKU'), ''),
				NULLIF(json_extract_path_text(ac.properties, 'sku'), ''))
				END 
		AND ac.event_name = 'Product Added to Cart'
	LEFT JOIN master.ORDER o 
		ON ac.order_id = o.order_id
	WHERE s.session_start::date >= '2023-11-20'
		AND s.session_start::date >= DATEADD('month', -6, current_date)
		AND te.event_id IS NOT NULL 
)
SELECT 
	info_level,
	session_start,
	marketing_channel,
	store_id,
	store_label,
	store_name,
	is_customer_logged_in,
	is_there_widget_click,
	widget,
	widget_name,
	recommended_for_sku,
	is_there_product_add_to_cart,
	is_order_submitted,
	is_order_paid,
	order_id,
	count(DISTINCT event_id_product_clicked) AS product_clicked_events,
	count(DISTINCT session_id) AS sessions
FROM prep 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
UNION 
SELECT 
	'total sessions without widget' AS info_level,
	s.session_start::date,
	s.marketing_channel,
	s.store_id,
	s.store_label,
	s.store_name,
	CASE WHEN s.customer_id IS NOT NULL THEN 1 ELSE 0 END AS is_customer_logged_in,
	0::int AS is_there_widget_click,
	NULL::Varchar AS widget,
	NULL::Varchar AS widget_name,
	NULL::Varchar AS recommended_for_sku,
	0::int  AS is_there_product_add_to_cart,
	0::int  AS is_order_submitted,
	0::int  AS is_order_paid,
	NULL::varchar AS order_id,
	NULL::int AS product_clicked_events,
	count(DISTINCT s.session_id) AS sessions
FROM segment.sessions_web  s
LEFT JOIN segment.track_events te 
	ON s.session_id = te.session_id
	AND te.event_name = 'Product Clicked'
	AND CASE WHEN IS_VALID_JSON(te.properties) THEN json_extract_path_text(te.properties, 'widget') end 
		IN ('Countdown Widget', 'Product Listing Widget', 'Lifestyle Widget')
	AND CASE WHEN IS_VALID_JSON(te.properties) THEN lower(json_extract_path_text(te.properties, 'widget_name')) end 
		NOT ILIKE  'alle%'
	AND CASE WHEN IS_VALID_JSON(te.properties) THEN lower(json_extract_path_text(te.properties, 'widget_name')) end 
		NOT ILIKE  'todos los%'
WHERE te.session_id IS NULL 
	AND s.session_start >= '2023-11-20'
	AND s.session_start::date >= DATEADD('month', -6, current_date)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_product.v_conversions_homepage_widgets TO tableau;