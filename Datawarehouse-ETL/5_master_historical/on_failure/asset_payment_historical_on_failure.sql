--delete snapshot of everyday
DELETE FROM master.asset_payment_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE if exists master.asset_payment_historical_backup;
CREATE TABLE master.asset_payment_historical_backup AS
SELECT * FROM master.asset_payment_historical;

--creating temp table
DROP TABLE IF EXISTS master.asset_payment_historical_old;
ALTER TABLE master.asset_payment_historical RENAME TO asset_payment_historical_old;

--creating new historical table with new columns
CREATE TABLE master.asset_payment_historical AS
SELECT *,current_date as date FROM master.asset_payment;

--adding old historical data to new historical table
ALTER TABLE master.asset_payment_historical APPEND FROM master.asset_payment_historical_old
FILLTARGET;

GRANT SELECT ON master.asset_payment_historical TO tableau;
GRANT SELECT ON master.asset_payment_historical_backup TO tableau;
