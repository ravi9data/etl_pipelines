WITH orders AS (
    SELECT
        order_id,
        customer_id,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY CONSUMED_AT DESC, updated_at DESC) AS rows
    FROM risk_internal_us_risk_flex_order_v1
    WHERE year||month||day >= date_format(current_date - interval ':lookback_days;' day , '%Y%m%d')
), deduplicate_orders AS (
    SELECT
        order_id,
        customer_id
    FROM orders
    WHERE rows = 1
), decision_result AS (
    SELECT
        order_id,
        code,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY consumed_at DESC, updated_at DESC) AS rows
    FROM risk_internal_us_risk_decision_result_v1
    WHERE year||month||day >= date_format(current_date - interval ':lookback_days;' day , '%Y%m%d')
),recent_decision_result AS (
    SELECT
        order_id,
        CAST(code AS int) as code
    FROM decision_result
    WHERE rows = 1
)
SELECT
    DISTINCT
    order_flex.CUSTOMER_ID,
    COUNT(order_flex.ORDER_ID) OVER (PARTITION BY order_flex.CUSTOMER_ID) AS TOTAL_ORDERS,
    SUM(CASE
        WHEN (RIURMROV.STATUS = 'DECLINED' OR RIURDRV.CODE = 2000) THEN 1
    ELSE 0 END) OVER (PARTITION BY order_flex.CUSTOMER_ID) AS DECLINED_ORDERS,
    SUM(CASE
        WHEN RIURMROV.STATUS = 'APPROVED' THEN 1
        ELSE 0 END) OVER (PARTITION BY order_flex.CUSTOMER_ID) AS APPROVED_ORDERS,
    COUNT(RIUROSV.ORDER_ID) OVER (PARTITION BY order_flex.CUSTOMER_ID) AS CANCELLED_ORDERS
FROM deduplicate_orders order_flex
LEFT JOIN RISK_INTERNAL_US_RISK_MANUAL_REVIEW_ORDER_V1 RIURMROV
ON order_flex.ORDER_ID = RIURMROV.ORDER_ID
LEFT JOIN
    RISK_INTERNAL_US_RISK_ORDER_STATUS_V1 RIUROSV
ON
    RIUROSV.ORDER_ID = order_flex.ORDER_ID
LEFT JOIN
    recent_decision_result RIURDRV
ON RIURDRV.ORDER_ID = order_flex.ORDER_ID
WHERE RIURMROV.year||RIURMROV.month||RIURMROV.day >= date_format(current_date - interval ':lookback_days;' day , '%Y%m%d')
