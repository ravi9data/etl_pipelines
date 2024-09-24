--snapshot of everyday
DELETE FROM master.order_historical
WHERE date = current_date - 1;

INSERT INTO master.order_historical
SELECT 
*,
current_date - 1 as date
FROM master.order
WHERE created_date < current_date;


--removing duplicates