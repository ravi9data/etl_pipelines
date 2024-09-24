WITH payment_methods AS (
    SELECT
        id,
        customer_id,
        payment_type,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY CONSUMED_AT DESC, updated_at DESC) AS rows
    FROM risk_internal_us_risk_order_payment_method_v1
    WHERE year||month||day >= date_format(current_date - interval ':lookback_days;' day , '%Y%m%d')
), deduplicate_payment_methods AS (
    SELECT
        id,
        customer_id,
        payment_type
    FROM payment_methods
    WHERE rows = 1
)
SELECT
    customer_id,
    count(distinct case when payment_type = 'credit_card' then id else null end)as num_diff_cc,
    count(distinct case when payment_type = 'paypal' then id else null end) as num_diff_pp
from
    deduplicate_payment_methods
group by
    1
