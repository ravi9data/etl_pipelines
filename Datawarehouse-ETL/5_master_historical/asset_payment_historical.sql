DELETE FROM master.asset_payment_historical
WHERE date = current_date - 1;

INSERT INTO master.asset_payment_historical
SELECT 
*,
current_date - 1 as date
FROM master.asset_payment
WHERE created_at < current_date;