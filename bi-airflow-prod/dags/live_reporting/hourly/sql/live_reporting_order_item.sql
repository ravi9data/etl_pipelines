
DROP TABLE IF EXISTS tmp_live_reporting_order_item;
CREATE TEMP TABLE tmp_live_reporting_order_item AS 
--new_infra    
WITH numbers AS (
  	SELECT
		*
	FROM public.numbers
  	WHERE ordinal < 20
)
, order_items_all AS (
	SELECT DISTINCT
		JSON_EXTRACT_PATH_TEXT(v.payload, 'order_number') AS order_number,
		JSON_EXTRACT_PATH_TEXT(v.payload, 'line_items') AS line_items, -- Note we ARE ONLY extracting this AS it IS used IN other columns.
		JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT, true) AS line_items_json,
		JSON_EXTRACT_PATH_TEXT(line_items_json, 'line_item_id') AS order_item_id,
		JSON_EXTRACT_PATH_TEXT(v.payload, 'order_mode') AS order_mode,
		CASE WHEN order_mode  = 'MIX' AND order_item_id = 0  THEN 1 ELSE JSON_ARRAY_LENGTH(line_items, true) END AS total_order_items,
		-- submitted date
	    to_timestamp (v.event_timestamp, 'yyyy-mm-dd HH24:MI:SS') AS event_timestamp_adj,
	    o.submitted_date_berlin_time,
		o.submitted_date_us_time,
		o.store_commercial,
		--
	    JSON_EXTRACT_PATH_TEXT(line_items_json,'variants') AS variants,
		JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(line_items_json,'variant'),'variant_id') AS variant_id,
		JSON_EXTRACT_PATH_TEXT(line_items_json,'quantity')::INT AS quantity,
		o.current_local_timestamp,
		NULLIF(JSON_EXTRACT_PATH_text(line_items_json,'committed_months'),'')::INTEGER AS plan_duration
	FROM live_reporting.order o
	--filtering the same orders we have IN live_reporting.ORDER
	LEFT JOIN stg_kafka_events_full.stream_internal_order_placed_v2 v
		ON JSON_EXTRACT_PATH_TEXT(v.payload, 'order_number') = o.order_id
	CROSS JOIN numbers 
	WHERE numbers.ordinal < total_order_items
)
SELECT
	oi.order_item_id::INT AS order_item_id,
	oi.order_number AS order_id,
	oi.submitted_date_berlin_time,
	oi.submitted_date_us_time,
	oi.store_commercial,
	oi.quantity,
	pp.product_sku,
	pp.product_name,
	oi.current_local_timestamp,
	oi.plan_duration
FROM order_items_all oi
LEFT JOIN bi_ods.variant v
	ON v.variant_id = oi.variant_id
LEFT JOIN bi_ods.product pp
	ON v.product_id = pp.product_id
WHERE pp.product_name IS NOT NULL
--
UNION ALL 
--old_infra
SELECT DISTINCT
	i.id AS order_item_id,
	oi.order_id,
	oi.submitted_date_berlin_time,
	oi.submitted_date_us_time,
	oi.store_commercial,
	i.quantity,
	p.product_sku,
	p.product_name,
	oi.current_local_timestamp,
	CASE WHEN COALESCE(i.minimum_term_months,0)=0 THEN 1 ELSE i.minimum_term_months END AS plan_duration
FROM live_reporting."order" oi
--filtering the same orders we have IN live_reporting."order"
LEFT JOIN stg_api_production.spree_orders o
	ON oi.order_id = o."number"
INNER JOIN stg_api_production.spree_line_items i
	ON i.order_id = o.id
LEFT JOIN stg_salesforce.order osf
	ON osf.spree_order_number__c = oi.order_id
LEFT JOIN stg_salesforce.orderitem sf
	ON i.id = sf.spree_order_line_id__c
LEFT JOIN bi_ods.variant v
	ON v.variant_id = i.variant_id
LEFT JOIN bi_ods.product p
	ON v.product_id = p.product_id
WHERE TRUE 
-- Adding condition to remove carts that do not exist in sf (provided that order exists in sf
	AND  (CASE WHEN osf.spree_order_number__c IS NOT NULL  --when order exists in sf
					AND sf.id IS NULL -- but order item doesn't exist in sf
					THEN FALSE ELSE TRUE END)
	AND p.product_name IS NOT NULL
 ;

BEGIN TRANSACTION;

DELETE FROM live_reporting.order_item WHERE 1=1;

INSERT INTO live_reporting.order_item
SELECT * FROM tmp_live_reporting_order_item;

END TRANSACTION;