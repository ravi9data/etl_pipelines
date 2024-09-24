ef_everflow_orders = """
SELECT DISTINCT
    affiliate,
    order_id,
    new_recurring,
    store_country,
    customer_type,
    voucher_code,
    basket_size,
    submitted_date::DATE AS submitted_date,
    currency,
    commission_approval,
    csv_with_discount,
    commission_type,
    commission_amount,
    commission_total_grover_eur
FROM marketing.partnership_validated_orders
WHERE commission_approval = 'APPROVED'
    AND affiliate_network = 'Everflow'
    AND DATE(submitted_date) BETWEEN
    DATE_ADD('month', -1, date_trunc('month',current_date)) AND
    DATE_ADD('day', -1, date_trunc('month',current_date));
"""

ef_everflow_orders_historical = """
SELECT DISTINCT
    affiliate,
    order_id,
    new_recurring,
    store_country,
    customer_type,
    voucher_code,
    basket_size,
    submitted_date::DATE AS submitted_date,
    currency,
    commission_approval,
    csv_with_discount,
    commission_type,
    commission_amount,
    commission_total_grover_eur
FROM marketing.partnership_validated_orders
WHERE commission_approval = 'APPROVED'
    AND affiliate_network = 'Everflow'
    AND DATE(submitted_date) >= '2023-09-01'
"""
