-- delete snapshot of everyday
DELETE FROM master.subscription_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE IF EXISTS master.subscription_historical_backup;
CREATE TABLE master.subscription_historical_backup AS
SELECT * FROM master.subscription_historical;

--creating temp table
DROP TABLE IF EXISTS master.subscription_historical_old;
ALTER TABLE master.subscription_historical RENAME TO subscription_historical_old;

--creating new historical table with new columns
CREATE TABLE master.subscription_historical AS
SELECT *,current_date as date FROM master.subscription;

--adding old historical data to new historical table
ALTER TABLE master.subscription_historical APPEND FROM master.subscription_historical_old 
filltarget;

GRANT SELECT ON master.subscription_historical TO tableau, anjali_savlani, mahmoudmando;
GRANT SELECT ON master.subscription_historical_backup TO tableau;
