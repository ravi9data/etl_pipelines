CREATE TABLE IF NOT EXISTS stg_external_apis.ups_us_quarantine_inventory
(
	reporting_date DATE,
	account INTEGER,
	item_number VARCHAR(50),
	item_description VARCHAR(200),
	warehouse VARCHAR(100),
	location VARCHAR(200),
	onhand_qty INTEGER,
	reserved_qty INTEGER,
	available_qty INTEGER,
	vendor_serial VARCHAR(100),
	vendor_lot VARCHAR(100),
	lpn VARCHAR(100),
	revision INTEGER,
	date_received_gmt TIMESTAMP,
	rec_ref_1 VARCHAR(100),
	rec_ref_2 VARCHAR(100),
	rec_ref_3 VARCHAR(100),
	attribute_1 VARCHAR(100),
	attribute_3 VARCHAR(100),
	attribute_4 VARCHAR(100),
	attribute_6 VARCHAR(100),
	batch_id VARCHAR(50),
	extracted_at TIMESTAMP
)
DISTSTYLE KEY
 DISTKEY (reporting_date)
 SORTKEY (reporting_date);
