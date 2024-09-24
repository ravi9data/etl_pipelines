DROP TABLE IF EXISTS staging.wms_ingram_events;

CREATE TABLE staging.wms_ingram_events AS
SELECT
	uid,
	created_at,
	serial_number,
	asset_serial_number,
	wh_goods_order_id,
	JSON_EXTRACT_PATH_text(event_payload,'damage') AS damage,
	JSON_EXTRACT_PATH_text(event_payload,'country') AS country,
	JSON_EXTRACT_PATH_text(event_payload,'hc_code') AS hc_code,
	JSON_EXTRACT_PATH_text(event_payload,'customer') AS customer,
	JSON_EXTRACT_PATH_text(event_payload,'assetname') AS assetname,
	JSON_EXTRACT_PATH_text(event_payload,'questions') AS questions,
	JSON_EXTRACT_PATH_text(event_payload,'event_type') AS disposition_code,
	JSON_EXTRACT_PATH_text(event_payload,'partner_id') AS partner_id,
	JSON_EXTRACT_PATH_text(event_payload,'added_items') AS added_items,
	JSON_EXTRACT_PATH_text(event_payload,'carrier_out') AS carrier_out,
	JSON_EXTRACT_PATH_text(event_payload,'item_number') AS item_number,
	JSON_EXTRACT_PATH_text(event_payload,'received_at') AS received_at,
	JSON_EXTRACT_PATH_text(event_payload,'status_code') AS status_code,
	JSON_EXTRACT_PATH_text(event_payload,'order_number') AS order_number,
	JSON_EXTRACT_PATH_text(event_payload,'awaited_parts') AS awaited_parts,
	JSON_EXTRACT_PATH_text(event_payload,'shipment_type') AS shipment_type,
	JSON_EXTRACT_PATH_text(event_payload,'carrier_service') AS carrier_service,
	JSON_EXTRACT_PATH_text(event_payload,'shipping_number') AS shipping_number,
	JSON_EXTRACT_PATH_text(event_payload,'track_and_trace') AS track_and_trace,
	JSON_EXTRACT_PATH_text(event_payload,'new_serial_number') AS new_serial_number,
	JSON_EXTRACT_PATH_text(event_payload,'customer_order_number') AS customer_order_number,
	JSON_EXTRACT_PATH_text(event_payload,'package_serial_number') AS package_serial_number,
	JSON_EXTRACT_PATH_text(event_payload,'license_plate') AS license_plate,
	case
	when
		JSON_EXTRACT_PATH_text(event_payload,'received_at') != ''
	then
		TIMESTAMP 'epoch' + JSON_EXTRACT_PATH_text(event_payload,'received_at') / 1000 * INTERVAL '1 second'
	else
		null
	end as source_timestamp

FROM airbyte_oltp.wms_ingram_micro_webhook_log
WHERE created_at>(SELECT max(created_at) FROM ods_operations.wms_ingram_events);


INSERT INTO ods_operations.wms_ingram_events
SELECT *
FROM staging.wms_ingram_events
WHERE uid NOT IN ( SELECT uid FROM ods_operations.wms_ingram_events);