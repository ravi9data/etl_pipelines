INSERT INTO recommerce.ingram_micro_send_order_grading_status(
SELECT
	reporting_date::date AS reporting_date,
	order_number,
	serial_number,
	item_number,
	shipping_number,
	disposition_code,
	status_code,
	source_timestamp::timestamp,
	partner_id,
	asset_serial_number,
	package_serial_number,
	questions,
	shipment_type,
	current_date AS date_of_processing,
	carrier_out,
	carrier_service,
	track_and_trace,
	customer,
	country,
	NULL AS package_no,
	NULL AS serial_no,
	assetname,
	damage,
	hc_code,
	NULL AS item_category,
	source_id,
	NULL AS batch_id,
	reporting_date::timestamp AS extracted_at,
	added_items,
	awaited_parts,
	new_serial_number
FROM  staging.ingram_sftp_daily_events
WHERE source_id NOT IN(SELECT source_id FROM recommerce.ingram_micro_send_order_grading_status));

TRUNCATE staging.ingram_sftp_daily_events;