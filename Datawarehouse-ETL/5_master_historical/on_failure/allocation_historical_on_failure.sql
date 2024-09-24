--delete snapshot of everyday
DELETE FROM master.allocation_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE IF EXISTS master.allocation_historical_backup;
CREATE TABLE master.allocation_historical_backup AS
SELECT * FROM master.allocation_historical;

--creating temp table
DROP TABLE IF EXISTS master.allocation_historical_old;
ALTER TABLE master.allocation_historical RENAME TO allocation_historical_old;

--creating new historical table with new columns
CREATE TABLE master.allocation_historical AS
SELECT *,current_date as date FROM master.allocation;

--adding old historical data to new historical table
ALTER TABLE master.allocation_historical APPEND FROM master.allocation_historical_old
FILLTARGET;

GRANT SELECT ON master.allocation_historical TO tableau;
GRANT SELECT ON master.allocation_historical_backup TO tableau;
