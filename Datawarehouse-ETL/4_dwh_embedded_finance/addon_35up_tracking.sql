DROP TABLE IF EXISTS dm_finance.addon_35up_tracking;
CREATE TABLE dm_finance.addon_35up_tracking AS
WITH RECURSIVE tmp_skus (properties,event_name, session_id, event_time, user_id, event_id, accessory_count, skus, num_actions,idx, sku, order_id, device_type, store_code, main_sku, accessory_types) AS (
SELECT
	DISTINCT properties,
	event_name,
	session_id,
	event_time,
	COALESCE(user_id, anonymous_id) AS user_id,
	event_id,
	json_extract_path_text(properties, 'accessory_count')  AS accessory_count,
    REPLACE(REPLACE(REPLACE(json_extract_path_text(properties, 'skus'),'[',''),']',''),'"','') AS skus,
    regexp_count(skus, ',') + 1 AS num_actions,
    1 AS idx,
    split_part(skus, ',', 1) AS sku,
	json_extract_path_text(properties,'order_id') AS order_id,
	device_type,
	store_code,
	json_extract_path_text(properties,'main_product_sku') AS main_sku,
	REPLACE(REPLACE(REPLACE(json_extract_path_text(properties, 'accessory_types'),'[',''),']',''),'"','') AS accessory_types
FROM
	segment.track_events
WHERE
	event_name IN (
    'Accessory Added',
    'Accessory Details Clicked',
    'Accessory Drawer Opened',
    'Accessory Order Submitted'
    )
UNION ALL
SELECT
	a.properties,
	a.event_name,
	a.session_id,
	a.event_time,
	a.user_id,
	a.event_id,
	a.accessory_count,
	a.skus,
	a.num_actions,
	a.idx + 1 AS idx,
	TRIM(SPLIT_PART(a.skus, ',', a.idx + 1)) AS sku,
	order_id,
	device_type,
	store_code,
	main_sku,
	accessory_types
FROM
	tmp_skus a
WHERE
	a.idx < a.num_actions
       )
SELECT
	session_id,
	properties,
	event_time,
	user_id,
	event_id,
	accessory_count,
	event_name,
	COALESCE(NULLIF(sku,''),json_extract_path_text(properties, 'sku')) AS accessory_sku,
	order_id,
	device_type,
	store_code,
	main_sku AS order_sku,
	COALESCE(NULLIF(accessory_types,''),json_extract_path_text(properties,'accessory_type')) AS accessory_type,
	NULLIF(json_extract_path_text(properties,'accessory_price'),'')::int / 100::double PRECISION AS accessory_price,
	json_extract_path_text(properties,'store_id') AS store_id
FROM
	tmp_skus;
