CREATE OR REPLACE VIEW dm_commercial.v_add_on_paid_orders AS 
WITH submitted_add_on AS (
	SELECT
		order_id,
		event_timestamp,
		addon_id,
		CASE
			WHEN store_code='de' THEN 'Germany'
			WHEN store_code='us' THEN 'United States'
			WHEN store_code='nl' THEN 'Netherlands'
			WHEN store_code='es' THEN 'Spain'
			WHEN store_code='at' THEN 'Austria'
		END AS country_name
	FROM stg_curated.checkout_addons_submitted_v1 casv 
)
, status_change_add_on AS (
	SELECT 
		order_id,
		event_name,
		price,
		addon_name,
		quantity,
		duration,
		event_timestamp,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_timestamp DESC) AS row_no
	FROM stg_curated.addons_order_status_change_v1
)	
, last_event AS (
	SELECT *
	FROM status_change_add_on
	WHERE row_no = 1
)
, dates_ AS (
	SELECT 
		s.order_id, 
		min(s.event_timestamp) AS submitted_date, 
		min(CASE WHEN sc.event_name = 'pending approval' THEN sc.event_timestamp ELSE NULL END) AS pending_approval_date, 
		min(CASE WHEN sc.event_name = 'approved' THEN sc.event_timestamp ELSE NULL END) AS approved_date,
		min(CASE WHEN p.order_id IS NOT NULL THEN p.paid_date ELSE NULL END) AS paid_date
	FROM submitted_add_on s
	LEFT JOIN status_change_add_on sc 
		ON	s.order_id = sc.order_id
	LEFT JOIN ods_production.payment_addon p
		ON p.order_id = s.order_id
	GROUP BY 1
)
, add_on_skus AS (
	SELECT DISTINCT
		CASE 
			WHEN is_valid_json(te.properties) 
				THEN json_extract_path_text(te.properties, 'sku') 
		END AS add_on_sku,		
		order_id
	FROM segment.track_events te 
	WHERE event_name = 'Add-On Added'
		AND properties LIKE '%DP00%'
)
SELECT 
	COALESCE(sk.add_on_sku,lag(add_on_sku) OVER (PARTITION BY le.addon_name ORDER BY s.order_id)) AS add_on_sku,
	s.order_id,
	CASE WHEN s.addon_id=1 THEN 'Gigs' ELSE le.addon_name END addon_name,
	d.paid_date::date,
	le.quantity::int,
	s.country_name,
	le.price::decimal(30, 2) AS addon_amount
FROM submitted_add_on s
LEFT JOIN ods_production.order o
	ON s.order_id = o.order_id
LEFT JOIN dates_ d 
	ON s.order_id = d.order_id
LEFT JOIN last_event le
	ON le.order_id = s.order_id
LEFT JOIN add_on_skus sk
	ON sk.order_id = s.order_id
WHERE d.paid_date IS NOT NULL
	AND addon_name <> 'Grover Addon Test - Only available for switch - Pick me!'
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_commercial.v_add_on_paid_orders TO hightouch;