dc_cancelled_orders = """SELECT DISTINCT order_id
FROM marketing.affiliate_validated_orders
WHERE affiliate_network = 'Daisycon'
  AND commission_approval != 'APPROVED'
  AND DATE(submitted_date) BETWEEN
  DATE_ADD('month', -1, date_trunc('month',current_date)) AND
      DATE_ADD('day', -1, date_trunc('month',current_date));
  """

dc_new_recurring_orders = """
SELECT DISTINCT order_id, total_order_value_grover_local_currency AS order_value
FROM marketing.affiliate_validated_orders
WHERE affiliate_network = 'Daisycon'
  AND commission_approval = 'APPROVED'
  AND new_recurring = 'RECURRING'
  AND DATE(submitted_date) BETWEEN
  DATE_ADD('month', -1, date_trunc('month',current_date)) AND
      DATE_ADD('day', -1, date_trunc('month',current_date));
"""

dc_new_paid_orders = """
SELECT DISTINCT order_id, total_order_value_grover_local_currency AS order_value
FROM marketing.affiliate_validated_orders
WHERE affiliate_network = 'Daisycon'
  AND commission_approval = 'APPROVED'
  AND new_recurring = 'NEW'
  AND DATE(submitted_date) BETWEEN
  DATE_ADD('month', -1, date_trunc('month',current_date)) AND
      DATE_ADD('day', -1, date_trunc('month',current_date));
"""
