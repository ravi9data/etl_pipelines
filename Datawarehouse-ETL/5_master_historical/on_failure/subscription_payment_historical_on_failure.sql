
--delete snapshot of everyday
DELETE FROM master.subscription_payment_historical
WHERE date = current_date;

--creating backup of old historical
DROP TABLE if exists master.subscription_payment_historical_backup;
CREATE TABLE master.subscription_payment_historical_backup AS
SELECT * FROM master.subscription_payment_historical;

--creating temp table
DROP TABLE IF EXISTS master.subscription_payment_historical_old;
ALTER TABLE master.subscription_payment_historical RENAME TO subscription_payment_historical_old;

--creating new historical table with new columns
CREATE TABLE master.subscription_payment_historical AS
SELECT *,current_date as date FROM master.subscription_payment;

--adding old historical data to new historical table
ALTER TABLE master.subscription_payment_historical APPEND FROM master.subscription_payment_historical_old 
filltarget;

GRANT SELECT ON master.subscription_payment_historical TO tableau;
GRANT SELECT ON master.subscription_payment_historical_backup TO tableau;
