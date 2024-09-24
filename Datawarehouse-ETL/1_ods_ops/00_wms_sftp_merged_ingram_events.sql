TRUNCATE TABLE ods_operations.wms_sftp_merged_ingram_events;

INSERT INTO ods_operations.wms_sftp_merged_ingram_events
WITH wms_ingram_events AS (
SELECT 
	source_timestamp,
	NULLIF(hc_code,''),
	NULLIF(disposition_code,''),
	NULLIF(status_code,''),
	NULLIF(order_number,''),
	NULLIF(serial_number,''),
	NULLIF(asset_serial_number,''),
	NULLIF(package_serial_number,''),
	NULLIF(new_serial_number,''),
	NULLIF(assetname,''),
	NULLIF(item_number,''),
	NULLIF(shipping_number,''),
	NULLIF(shipment_type,''),
	NULLIF(carrier_out,''),
	NULLIF(carrier_service,''),
	NULLIF(track_and_trace,''),
	NULLIF(customer,''),
	NULLIF(country,''),
	NULLIF(partner_id,''),
	NULLIF(added_items,''),
	NULLIF(awaited_parts,''),
	NULLIF(questions,'')
FROM ods_operations.wms_ingram_events
WHERE source_timestamp::date>'2023-08-04') --as we started TO receive structured events through Webhook 2023-08-04 

,sftp_ingram_events AS (
SELECT
	source_timestamp,
	hc_code,
	disposition_code,
	status_code,
	order_number,
	serial_number,
	asset_serial_number,
	package_serial_number,
	new_serial_number,
	assetname,
	item_number,
	shipping_number,
	shipment_type,
	carrier_out,
	carrier_service,
	track_and_trace,
	customer,
	country,
	partner_id,
	added_items,
	awaited_parts,
	questions
FROM recommerce.ingram_micro_send_order_grading_status)

,merged_ingram_events AS (
SELECT DISTINCT 
	COALESCE(sftp.source_timestamp,wms.source_timestamp) AS source_timestamp,
	COALESCE(sftp.hc_code,wms.hc_code) AS hc_code,
	COALESCE(sftp.disposition_code,wms.disposition_code) AS disposition_code,
	COALESCE(sftp.status_code,wms.status_code) AS status_code,
	COALESCE(sftp.order_number,wms.order_number) AS order_number,
	COALESCE(sftp.serial_number,wms.serial_number) AS serial_number,
	COALESCE(sftp.asset_serial_number,wms.asset_serial_number) AS asset_serial_number,
	COALESCE(sftp.package_serial_number,wms.package_serial_number) AS package_serial_number,
	COALESCE(sftp.new_serial_number,wms.new_serial_number) AS new_serial_number,
	COALESCE(sftp.assetname,wms.assetname) AS assetname,
	COALESCE(sftp.item_number,wms.item_number) AS item_number,
	COALESCE(sftp.shipping_number,wms.shipping_number) AS shipping_number,
	COALESCE(sftp.shipment_type,wms.shipment_type) AS shipment_type,
	COALESCE(sftp.carrier_out,wms.carrier_out) AS carrier_out,
	COALESCE(sftp.carrier_service,wms.carrier_service) AS carrier_service,
	COALESCE(sftp.track_and_trace,wms.track_and_trace) AS track_and_trace,
	COALESCE(sftp.customer,wms.customer) AS customer,
	COALESCE(sftp.country,wms.country) AS country,
	COALESCE(sftp.partner_id,wms.partner_id) AS partner_id,
	COALESCE(sftp.added_items,wms.added_items) AS added_items,
	COALESCE(sftp.awaited_parts,wms.awaited_parts) AS awaited_parts,
	COALESCE(sftp.questions,wms.questions) AS questions
FROM sftp_ingram_events sftp
FULL OUTER JOIN wms_ingram_events wms 
ON sftp.source_timestamp = wms.source_timestamp 
AND sftp.serial_number =  wms.serial_number
AND sftp.order_number = wms.order_number
AND sftp.disposition_code =wms.disposition_code
AND sftp.status_code = wms.status_code
)

,deduplicated_merged_ingram_events AS (
SELECT 
	source_timestamp,
	serial_number,
	order_number,
	disposition_code,
	status_code,
	MAX(hc_code) AS hc_code,
	MAX(asset_serial_number) AS asset_serial_number,
	MAX(package_serial_number) AS package_serial_number,
	MAX(new_serial_number) AS new_serial_number,
	MAX(assetname) AS assetname,
	MAX(item_number) AS item_number,
	MAX(shipping_number) AS shipping_number,
	MAX(shipment_type) AS shipment_type,
	MAX(carrier_out) AS carrier_out,
	MAX(carrier_service) AS carrier_service,
	MAX(track_and_trace) AS track_and_trace,
	MAX(customer) AS customer,
	MAX(country) AS country,
	MAX(partner_id) AS partner_id,
	MAX(added_items) AS added_items,
	MAX(awaited_parts) AS awaited_parts,
	MAX(questions) AS questions
FROM merged_ingram_events
GROUP BY 1,2,3,4,5)

SELECT *
FROM deduplicated_merged_ingram_events;