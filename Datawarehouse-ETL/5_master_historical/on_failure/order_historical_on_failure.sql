--delete snapshot of everyday
DELETE FROM master.order_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE IF EXISTS master.order_historical_backup;
CREATE TABLE master.order_historical_backup AS
SELECT * FROM master.order_historical;

--creating temp table
DROP TABLE IF EXISTS master.order_historical_old;
ALTER TABLE master.order_historical RENAME TO order_historical_old;

--creating new historical table with new columns
CREATE TABLE master.order_historical AS
SELECT *,current_date as date FROM master.order;

--adding old historical data to new historical table
ALTER TABLE master.order_historical APPEND FROM master.order_historical_old
FILLTARGET;

GRANT SELECT ON master.order_historical_backup TO tableau;
GRANT SELECT ON master.order_historical TO tableau;
