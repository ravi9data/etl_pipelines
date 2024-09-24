--delete snapshot of everyday
DELETE FROM master.variant_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE IF EXISTS master.variant_historical_backup;
CREATE TABLE master.variant_historical_backup AS
SELECT * FROM master.variant_historical;

--creating temp table
DROP TABLE IF EXISTS master.variant_historical_old;
ALTER TABLE master.variant_historical RENAME TO variant_historical_old;

--creating new historical table with new columns
CREATE TABLE master.variant_historical AS
SELECT *, current_date as date FROM master.variant;

-- Add sort key
ALTER TABLE master.variant_historical add SORT_KEY "date";

--adding old historical data to new historical table
ALTER TABLE master.variant_historical APPEND FROM master.variant_historical_old 
filltarget;

GRANT SELECT ON master.variant_historical TO tableau;
GRANT SELECT ON master.variant_historical_backup TO tableau;
