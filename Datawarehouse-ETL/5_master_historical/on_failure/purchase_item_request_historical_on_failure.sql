--delete snapshot of everyday
DELETE FROM master.purchase_request_item_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE IF EXISTS master.purchase_request_item_historical_backup;
CREATE TABLE master.purchase_request_item_historical_backup AS
SELECT * FROM master.purchase_request_item_historical;

--creating temp table
DROP TABLE IF EXISTS master.purchase_request_item_historical_old;
ALTER TABLE master.purchase_request_item_historical RENAME TO purchase_request_item_historical_old;

--creating new historical table with new columns
CREATE TABLE master.purchase_request_item_historical AS
SELECT *,current_date as date FROM ods_production.purchase_request_item;

--adding old historical data to new historical table
ALTER TABLE master.purchase_request_item_historical APPEND FROM master.purchase_request_item_historical_old
FILLTARGET;