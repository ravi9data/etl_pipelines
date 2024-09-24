SELECT
    customer_id:: VARCHAR AS customer_id,
    ffp_orders_latest AS FFP
FROM ods_production.customer_orders_details
WHERE customer_id IN {customer_ids};
