ef_payload_revenue = """
SELECT DISTINCT
    b.conversion_id,
    a.commission_total_grover_eur AS payout,
    a.total_order_value_grover_local_currency AS revenue,
    0 AS sale_amount
FROM marketing.affiliate_validated_orders a
INNER JOIN marketing.affiliate_everflow_submitted_orders b USING (order_id)
WHERE a.affiliate_network = 'Everflow'
    AND a.commission_approval = 'APPROVED'
    AND DATE(a.submitted_date) BETWEEN
    DATE_ADD('month', -1, date_trunc('month',current_date)) AND
    DATE_ADD('day', -1, date_trunc('month',current_date));
"""

ef_status = """
SELECT DISTINCT
    b.conversion_id,
    CASE WHEN a.commission_approval = 'APPROVED' THEN 'approved'
        ELSE 'rejected' END AS status
FROM marketing.affiliate_validated_orders a
INNER JOIN marketing.affiliate_everflow_submitted_orders b USING (order_id)
WHERE a.affiliate_network = 'Everflow'
    AND DATE(a.submitted_date) BETWEEN
    DATE_ADD('month', -1, date_trunc('month',current_date)) AND
    DATE_ADD('day', -1, date_trunc('month',current_date));
"""
