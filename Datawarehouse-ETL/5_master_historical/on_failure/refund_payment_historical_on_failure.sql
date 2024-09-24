--delete snapshot of everyday
DELETE FROM master.refund_payment_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE if exists master.refund_payment_historical_backup;
CREATE TABLE master.refund_payment_historical_backup AS
SELECT * FROM master.refund_payment_historical;

--creating temp table
DROP TABLE IF EXISTS master.refund_payment_historical_old;
ALTER TABLE master.refund_payment_historical RENAME TO refund_payment_historical_old;

--creating new historical table with new columns
CREATE TABLE master.refund_payment_historical AS
SELECT *,current_date as date FROM master.refund_payment;

--adding old historical data to new historical table
ALTER TABLE master.refund_payment_historical APPEND FROM master.refund_payment_historical_old
FILLTARGET;