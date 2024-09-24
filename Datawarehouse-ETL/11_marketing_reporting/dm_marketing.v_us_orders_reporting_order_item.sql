CREATE OR REPLACE VIEW dm_marketing.v_us_orders_reporting_order_item AS
WITH numbers AS (
	SELECT ordinal
	FROM public.numbers
	WHERE ordinal < 20
)
, order_items AS (
	SELECT DISTINCT
		event_timestamp ,
		JSON_EXTRACT_PATH_text(payload,'order_number') AS order_number,
		JSON_EXTRACT_PATH_text(payload,'order_mode') AS order_mode,
		JSON_EXTRACT_PATH_text(payload,'shipping_address', 'country') AS shipping_country,
		JSON_EXTRACT_PATH_text(payload,'line_items') AS line_items,
		JSON_ARRAY_LENGTH(line_items,true) as total_order_items,
	    json_extract_array_element_text(line_items,numbers.ordinal::int, true) as line_items_json,
	    line_items,to_timestamp (event_timestamp, 'yyyy-mm-dd HH24:MI:SS') as event_timestamp_adj
	FROM stg_kafka_events_full.stream_internal_order_placed_v2 op
	CROSS JOIN numbers
	WHERE numbers.ordinal < total_order_items
	  AND order_mode = 'FLEX'
	  AND shipping_country = 'United States'
)
SELECT 
    JSON_EXTRACT_PATH_text(line_items_json,'line_item_id')::int as order_item_id,
    event_timestamp_adj::timestamp as submitted_date,
    'none' as status,
    JSON_EXTRACT_PATH_text(line_items_json,'quantity')::int as quantity,
    product_name
FROM order_items oi
LEFT JOIN ods_production.variant v
	ON v.variant_id=JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(oi.line_items_json,'variant'),'variant_id')
LEFT JOIN ods_production.product p
	ON v.product_id=p.product_id
WHERE (Date_trunc('day',submitted_date::timestamp) >= current_date-8
   OR (Date_trunc('day',submitted_date::timestamp)) = '2020-11-27'
   OR (Date_trunc('day',submitted_date::timestamp)) = '2020-11-26'
   OR ((date_part('week',submitted_date::timestamp) = date_part('week',current_date)-4))
   OR ((date_part('week',submitted_date::timestamp) = date_part('week',current_date)) 
   		AND (date_part('year',submitted_date::timestamp) = date_part('year',current_date)-1))
   OR ((date_part('week',submitted_date::timestamp) = date_part('week',current_date)+1) 
   		AND (date_part('year',submitted_date::timestamp) = date_part('year',current_date)-1)))
WITH NO SCHEMA BINDING;