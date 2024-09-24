--delete snapshot of everyday
DELETE FROM master.asset_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE IF EXISTS master.asset_historical_backup;
CREATE TABLE master.asset_historical_backup AS
SELECT * FROM master.asset_historical;

--creating temp table#
DROP TABLE IF EXISTS master.asset_historical_old;
ALTER TABLE master.asset_historical RENAME TO asset_historical_old;

--creating new historical table with new columns
CREATE TABLE master.asset_historical AS
SELECT *,current_date as date,CURRENT_TIMESTAMP::TIMESTAMP as snapshot_time FROM master.asset;

--adding old historical data to new historical table
ALTER TABLE master.asset_historical APPEND FROM master.asset_historical_old
FILLTARGET;

GRANT SELECT ON master.asset_historical TO tableau;
GRANT SELECT ON master.asset_historical_backup TO tableau;
