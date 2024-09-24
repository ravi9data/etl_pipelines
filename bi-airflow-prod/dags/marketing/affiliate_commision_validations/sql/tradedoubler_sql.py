td_unload_sql = """
WITH prep_data AS (SELECT
    order_id AS order_number,
    affiliate AS source_name,
    total_order_value_grover_local_currency AS affiliate_order_value,
    order_country AS country,
    CASE WHEN commission_approval = 'APPROVED' THEN 'APPROVED'
    ELSE 'DECLINED' END AS approved_declined,
    new_recurring,
    total_order_value_grover_local_currency as grover_order_value,
    ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY approved_declined) AS rn
FROM marketing.affiliate_validated_orders
WHERE
    affiliate_network = 'Tradedoubler'
    AND DATE(submitted_date) BETWEEN
    DATE_ADD('month', -1, date_trunc('month',current_date)) AND
    DATE_ADD('day', -1, date_trunc('month',current_date))
)
    SELECT order_number,
        source_name,
        affiliate_order_value,
        country,
        approved_declined,
        new_recurring,
        grover_order_value
    FROM prep_data
    WHERE rn = 1
"""

td_pending_sql = """
WITH prep_data AS (
SELECT
    order_id AS order_number,
    affiliate AS source_name,
    total_order_value_grover_local_currency AS affiliate_order_value,
    order_country AS country,
    commission_approval AS status,
    CASE WHEN commission_approval = 'APPROVED' THEN 1
         WHEN commission_approval = 'APPROVED - WAITING' THEN 2
         WHEN commission_approval = 'PENDING' THEN 3
         ELSE 4 END AS sort_status,
    new_recurring,
    total_order_value_grover_local_currency AS grover_order_value,
    ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY sort_status) AS rn
FROM marketing.affiliate_validated_orders
WHERE
    affiliate_network = 'Tradedoubler'
    AND DATE(submitted_date) BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
)
    SELECT order_number,
        source_name,
        affiliate_order_value,
        country,
        status,
        new_recurring,
        grover_order_value
    FROM prep_data
    WHERE rn = 1
"""
