--snapshot of everyday
DELETE FROM master.variant_historical
WHERE date = current_date - 1;

INSERT INTO master.variant_historical
SELECT
*,
current_date - 1 as date
FROM master.variant
WHERE variant_updated_at < current_date;
