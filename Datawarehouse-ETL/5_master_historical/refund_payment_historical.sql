DELETE FROM master.refund_payment_historical
WHERE date = current_date - 1;

INSERT INTO master.refund_payment_historical
SELECT 
*,
current_date - 1 as date
FROM master.refund_payment
WHERE created_at < current_date;