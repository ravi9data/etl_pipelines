--delete snapshot of everyday
DELETE FROM master.customer_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE IF EXISTS master.customer_historical_backup;
CREATE TABLE master.customer_historical_backup AS
SELECT * FROM master.customer_historical;

--creating temp table
DROP TABLE IF EXISTS master.customer_historical_old;
ALTER TABLE master.customer_historical RENAME TO customer_historical_old;

--creating new historical table with new columns
CREATE TABLE master.customer_historical AS
SELECT *,current_date as date FROM master.customer;

--adding old historical data to new historical table
ALTER TABLE master.customer_historical APPEND FROM master.customer_historical_old 
filltarget;

GRANT SELECT ON master.customer_historical TO tableau;
GRANT SELECT ON master.customer_historical_backup TO tableau;
