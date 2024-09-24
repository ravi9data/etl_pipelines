cj_eu_status = """
SELECT
    order_id,
    total_order_value_grover_local_currency as order_value,
    commission_approval
FROM marketing.affiliate_validated_orders
WHERE affiliate_network = 'CJ'
    AND order_country != 'United States'
    AND DATE(submitted_date) BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE;
"""

cj_eu_status_final = """
SELECT
    order_id,
    total_order_value_grover_local_currency as order_value,
    CASE WHEN commission_approval = 'APPROVED' THEN 'APPROVED'
    ELSE 'DECLINED' END AS approved_declined
FROM marketing.affiliate_validated_orders
WHERE affiliate_network = 'CJ'
    AND order_country != 'United States'
    AND DATE(submitted_date) BETWEEN
    DATE_ADD('month', -1, date_trunc('month',current_date)) AND
    DATE_ADD('day', -1, date_trunc('month',current_date));
"""

cj_us_status = """
SELECT
    order_id,
    total_order_value_grover_local_currency as order_value,
    commission_approval
FROM marketing.affiliate_validated_orders
WHERE affiliate_network = 'CJ'
    AND order_country = 'United States'
    AND DATE(submitted_date) BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE;
"""

cj_us_status_final = """
SELECT
    order_id,
    total_order_value_grover_local_currency as order_value,
    CASE WHEN commission_approval = 'APPROVED' THEN 'APPROVED'
    ELSE 'DECLINED' END AS approved_declined
FROM marketing.affiliate_validated_orders
WHERE affiliate_network = 'CJ'
    AND order_country = 'United States'
    AND DATE(submitted_date) BETWEEN
    DATE_ADD('month', -1, date_trunc('month',current_date)) AND
    DATE_ADD('day', -1, date_trunc('month',current_date));
"""
