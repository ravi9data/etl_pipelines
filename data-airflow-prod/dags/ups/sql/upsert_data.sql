BEGIN;

DELETE FROM stg_external_apis.ups_us_quarantine_inventory
WHERE reporting_date = '{{ti.xcom_pull(key='reporting_date')}}'::DATE;

INSERT INTO stg_external_apis.ups_us_quarantine_inventory (
	reporting_date,
	account,
	item_number,
	item_description,
	warehouse,
	location,
	onhand_qty,
	reserved_qty,
	available_qty,
	vendor_serial,
	vendor_lot,
	lpn,
	revision,
	date_received_gmt,
	rec_ref_1,
	rec_ref_2,
	rec_ref_3,
	attribute_1 ,
	attribute_3,
	attribute_4,
	attribute_6,
	batch_id,
	extracted_at)
SELECT
	reporting_date,
	account,
	item_number,
	item_description,
	warehouse,
	location,
	onhand_qty,
	reserved_qty,
	available_qty,
	vendor_serial,
	vendor_lot,
	lpn,
	revision,
	date_received_gmt,
	rec_ref_1,
	rec_ref_2,
	rec_ref_3,
	attribute_1 ,
	attribute_3,
	attribute_4,
	attribute_6,
	batch_id,
	extracted_at
FROM stg_external_apis_dl.ups_us_quarantine_inventory;

COMMIT;
