WITH orders_shipping_address AS (
    SELECT
        order_id,
        customer_id,
        CAST(json_extract(json_extract(raw_data, '$.shipping_address'), '$.address1') AS varchar) as address,
        CAST(json_extract(json_extract(raw_data, '$.shipping_address'), '$.state') AS varchar) as state,
        CAST(json_extract(json_extract(raw_data, '$.shipping_address'), '$.zipcode') AS varchar) as zip_code,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY CONSUMED_AT DESC, updated_at DESC) AS rows
    FROM risk_internal_us_risk_flex_order_v1
    WHERE year||month||day >= date_format(current_date - interval ':lookback_days;' day, '%Y%m%d')
), deduplicate_shipping_address_orders AS (
    SELECT
        order_id,
        customer_id,
        address,
        state,
        zip_code
    FROM orders_shipping_address
    WHERE rows = 1
)
SELECT
    shipping_address_1.order_id ,
    count(distinct shipping_address_2.customer_id )as nb_addr_matching_customer_id,
    array_agg(distinct shipping_address_2.customer_id) as addr_matching_customer_id
FROM deduplicate_shipping_address_orders shipping_address_1
INNER JOIN
deduplicate_shipping_address_orders shipping_address_2
ON
    shipping_address_1.address = shipping_address_2.address
    and
    shipping_address_1.state = shipping_address_2.state
    and
    shipping_address_1.zip_code = shipping_address_2.zip_code
    and shipping_address_1.customer_id <> shipping_address_2.customer_id
GROUP BY 1
