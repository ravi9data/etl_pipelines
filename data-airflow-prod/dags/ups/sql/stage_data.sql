DROP TABLE IF EXISTS stg_external_apis_dl.ups_us_quarantine_inventory;

CREATE TABLE stg_external_apis_dl.ups_us_quarantine_inventory
    DISTKEY(reporting_date)
    SORTKEY(reporting_date)
AS SELECT
    '{{ti.xcom_pull(key='reporting_date')}}'::DATE AS reporting_date,
    account::INTEGER,
    item_number::VARCHAR(50),
    item_description::VARCHAR(200),
    warehouse::VARCHAR(100),
    location::VARCHAR(200),
    NULLIF(onhand_qty, '')::INTEGER,
    NULLIF(reserved_qty, '')::INTEGER,
    NULLIF(available_qty, '')::INTEGER,
    vendor_serial::VARCHAR(100),
    NULLIF(vendor_lot, '')::VARCHAR(100),
    lpn::VARCHAR(100),
    NULLIF(revision, '')::INTEGER,
    NULLIF(date_received_gmt, '')::TIMESTAMP,
	rec_ref_1::VARCHAR(100),
	rec_ref_2::VARCHAR(100),
	rec_ref_3::VARCHAR(100),
	attribute_1::VARCHAR(100),
	attribute_3::VARCHAR(100),
	attribute_4::VARCHAR(100),
	NULLIF(attribute_6, '')::VARCHAR(100),
    batch_id::VARCHAR(100),
    extracted_at::TIMESTAMP
FROM s3_spectrum_ups.us_quarantine_inventory
    WHERE year = '{{ti.xcom_pull(key='year')}}'
    AND month = '{{ti.xcom_pull(key='month')}}'
    AND day = '{{ti.xcom_pull(key='day')}}'
    AND batch_id = '{{ti.xcom_pull(key='batch_id')}}';
