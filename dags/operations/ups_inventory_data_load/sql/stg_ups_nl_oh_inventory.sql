DELETE FROM stg_external_apis.ups_nl_oh_inventory
WHERE report_date = current_date::date;

INSERT INTO stg_external_apis.ups_nl_oh_inventory
SELECT DISTINCT
	pallet_id,
    original_pallet_id,
    sku,
    description,
    disposition_cd,
    translate(unit_srl_no , CHR(29), '') AS unit_srl_no,
    unit_status,
    to_date(REPLACE(replace(rcpt_closed,'.',''),'NULL',''), 'YYYYMMDD', FALSE) AS rcpt_closed,
	current_date::date AS report_date,
    po_no
FROM staging.stock_ups___raw_data
WHERE unit_srl_no IS NOT NULL;

GRANT ALL ON stg_external_apis.ups_nl_oh_inventory TO GROUP bi;
GRANT SELECT ON stg_external_apis.ups_nl_oh_inventory TO GROUP recommerce;
