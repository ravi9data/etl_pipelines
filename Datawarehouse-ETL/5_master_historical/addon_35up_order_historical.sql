BEGIN;

DELETE FROM master.addon_35up_order_historical
WHERE DATE = current_date - 1;

INSERT INTO master.addon_35up_order_historical
SELECT order_id, 
customer_id, 
created_date, 
submitted_date, 
paid_date, status, 
order_value, 
new_recurring, 
store_country, 
customer_type, 
order_item_count, 
store_code, 
refund_date, 
addon_item_count, 
addon_price,
current_date -1 AS date
FROM master.addon_35up_order
WHERE created_date < current_date;

COMMIT;