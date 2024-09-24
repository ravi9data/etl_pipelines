WITH data_process AS(
    SELECT
        created_at,
        updated_at,
        order_id,
        customer_id,
        verification_state,
        ROW_NUMBER()OVER (PARTITION BY id ORDER BY CONSUMED_AT, updated_at DESC) AS rows
    FROM risk_internal_us_risk_id_verification_request_v1
    WHERE year||month||day >= date_format(current_date - interval ':lookback_days;' day , '%Y%m%d')
    AND order_id IN :list_orders;
    )
SELECT
    created_at,
    updated_at,
    order_id,
    customer_id,
    verification_state
FROM data_process
WHERE rows = 1
