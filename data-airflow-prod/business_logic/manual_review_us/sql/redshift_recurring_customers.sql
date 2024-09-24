SELECT
    DISTINCT customer_id:: VARCHAR AS customer_id
FROM ods_data_sensitive.order_manual_review
WHERE store_country='United States'
AND order_id IN {list_orders};
