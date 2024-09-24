DROP TABLE IF EXISTS stg_r_etl_affiliate_validated_orders;

CREATE TEMP TABLE stg_r_etl_affiliate_validated_orders AS
SELECT * FROM marketing.r_etl_affiliate_validated_orders;

DELETE FROM marketing.r_etl_affiliate_validated_orders
WHERE 1=1;

INSERT INTO marketing.r_etl_affiliate_validated_orders (
SELECT
affiliate_network,
order_id,
new_recurring,
order_status,
created_date,
submitted_date,
currency,
commission_approval,
total_order_value_grover_local_currency,
loaded_at,
customer_id,
affiliate_network_sync_status,
click_id
FROM marketing.affiliate_validated_orders);

ALTER TABLE stg_r_etl_affiliate_validated_orders RENAME COLUMN order_id TO order_id_;

UPDATE marketing.r_etl_affiliate_validated_orders
SET affiliate_network_sync_status = stg_r_etl.affiliate_network_sync_status
FROM stg_r_etl_affiliate_validated_orders stg_r_etl
WHERE order_id = stg_r_etl.order_id_;
