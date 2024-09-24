BEGIN;
TRUNCATE TABLE master.addon_35up_order;

INSERT INTO master.addon_35up_order
SELECT 
    order_id, 
    customer_id, 
    created_date, 
    submitted_date, 
    paid_date, 
    status, 
    order_value, 
    new_recurring, 
    store_country, 
    customer_type, 
    order_item_count, 
    store_code, 
    refund_date, 
    addon_item_count, 
    addon_price 
FROM 
    ods_production.addon_35up_order;

COMMIT;