--yakk_b2b_cost cost

TRUNCATE marketing.marketing_yakk_b2b_cost;
INSERT INTO marketing.marketing_yakk_b2b_cost
SELECT
    REPLACE(date, '/', '-')::DATE AS date,
    order_id,
    REPLACE(total_order_value,',','')::DOUBLE PRECISION AS total_order_value,
    REPLACE(commission_share,',','')::DOUBLE PRECISION AS commission_share,
    REPLACE(paid_commission,',','')::DOUBLE PRECISION AS paid_commission
FROM staging.marketing_cost_yakk_b2b
WHERE total_order_value IS NOT NULL;
