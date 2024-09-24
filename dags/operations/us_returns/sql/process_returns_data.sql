TRUNCATE TABLE ods_operations.us_returns;

INSERT
	INTO
	ods_operations.us_returns
SELECT
	"Date"::date,
	CAST( "LPN" AS text) AS LPN,
	"Serial Number",
	"Part Number",
	replace(replace("Price",'$',''),',','')::double PRECISION as price,
	"Item Description",
	"Blancco",
	"Extra Sanitation",
	"Usage",
	"Packaging Description",
	"Appearance Description",
	"Missing Parts (Notate specific part in comments)",
	"Function Description",
	"Comments",
	"Disposition",
	"New Location",
	"ETPS Updates",
	"Quarantine Updates",
	"AP Updates",
	"Notes"
FROM
	staging.airbyte_us_returns_log
