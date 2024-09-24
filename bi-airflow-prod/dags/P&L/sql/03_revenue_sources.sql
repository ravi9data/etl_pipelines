DROP TABLE IF EXISTS finance.profit_and_loss_revenue_sources;

CREATE TABLE finance.profit_and_loss_revenue_sources as
WITH base AS (
    SELECT 
        rs.dimension
        ,rs.date_
        ,rs.cost_entity 
        ,i.amount - (sum(rs.amount)/1000) AS base
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input i 
    ON rs.dimension = i.dimension
        AND rs.date_ = i.date_ 
    WHERE TRUE
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity = 'United States'
    GROUP BY 1,2,3, i.amount
)
,gross_subscription_revenue AS (
    SELECT
         rs.date_
        ,rs.cost_entity
        ,round (sum(rs.amount) OVER (PARTITION BY rs.dimension, rs.cost_entity)/sum(rs.amount)OVER (PARTITION BY rs.dimension) *  b.base) AS gross_subscription_revenue
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN base b
    ON rs.dimension = b.dimension
    AND rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity <> 'United States'
UNION ALL  
    SELECT 
         date_
        ,cost_entity
        ,round(sum(amount/1000)) AS gross_subscription_revenue
    FROM finance.profit_and_loss_redshift
    WHERE TRUE 
        AND dimension = 'gross_subscription_revenue'
        AND cost_entity = 'United States'
    GROUP BY 1,2
)
, voucher_discount AS(
    SELECT 
        date_
        ,cost_entity
        ,round(sum(amount) / 1000) AS voucher_discount
    FROM finance.profit_and_loss_redshift
    WHERE TRUE 
        AND dimension = 'voucher_discount_amount'
    GROUP BY 1,2
) 
, voucher_discount_reallocation AS (
    SELECT 
         date_
        ,cost_entity
        ,round(sum(amount) / 1000) AS voucher_discount_reallocation
    FROM finance.profit_and_loss_input
    WHERE TRUE 
        AND dimension = 'voucher_discount_reallocation'
        AND cost_entity <> 'group_consol'
    GROUP BY 1,2--, amount
)
, internal_customer AS (
    SELECT
         date_
        ,cost_entity
        ,round(amount / 1000) AS internal_customer
    FROM finance.profit_and_loss_redshift
    WHERE dimension = 'internal_customer_asset_sale'
    GROUP BY 1,2, amount
)
,external_customer AS (
    SELECT
         rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS external_customer
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_subscriptions'
        AND rs.cost_entity IN ('B2B-Germany', 'B2C-Germany')
        AND b.dimension = 'external_customer'
        AND b.cost_entity = 'group_consol'
)
, shipping_revenue AS (
    SELECT 
         date_
        ,cost_entity
        ,round(amount / 1000) AS shipping_revenue
    FROM finance.profit_and_loss_redshift
    WHERE TRUE 
        AND dimension = 'shipping_revenue'
    GROUP BY 1,2, amount
)
, vendor_support AS (
    SELECT 
         rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity)/sum(rs.amount) OVER () * b.amount) AS vendor_support
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b 
    ON rs.date_ = b.date_
    --AND rs.cost_entity = b.cost_entity
    WHERE TRUE 
        AND rs.dimension = 'new_subscriptions'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany')
        AND b.dimension = 'vendor_support'
        AND b.cost_entity = 'group_consol'
)
, other_revenue AS (
    SELECT 
         rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity)/sum(rs.amount) OVER () * b.amount) AS other_revenue
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b 
    ON rs.date_ = b.date_
    --AND rs.cost_entity = b.cost_entity
    WHERE TRUE 
        AND rs.dimension = 'new_subscriptions'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany')
        AND b.dimension = 'other_revenue'
        AND b.cost_entity = 'group_consol'
)
, ic_office AS (
    SELECT 
     rs.date_
    ,rs.cost_entity
    ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS ic_office
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity <> 'United States'
        AND b.dimension = 'ic_office'
        AND b.cost_entity = 'group_consol'
)
SELECT 
DISTINCT
     rs.date_
    ,rs.cost_entity
    ,COALESCE (sub.gross_subscription_revenue,0) AS gross_subscription_revenue  
    ,COALESCE (a.voucher_discount,0) AS voucher_discount
    ,COALESCE (b.voucher_discount_reallocation,0) AS voucher_discount_reallocation 
    ,COALESCE (c.internal_customer,0) AS internal_customer
    ,COALESCE (d.external_customer,0) AS external_customer
    ,COALESCE (e.shipping_revenue,0) AS shipping_revenue
    ,COALESCE (f.vendor_support,0) AS vendor_support
    ,COALESCE (g.other_revenue,0) AS other_revenue
    ,COALESCE (h.ic_office,0) AS ic_office
FROM finance.profit_and_loss_redshift rs
LEFT JOIN gross_subscription_revenue sub
    ON rs.cost_entity = sub.cost_entity
    AND rs.date_ = sub.date_
LEFT JOIN voucher_discount a
    ON rs.cost_entity = a.cost_entity
    AND rs.date_ = a.date_
LEFT JOIN voucher_discount_reallocation b
    ON rs.cost_entity = b.cost_entity
    AND rs.date_ = b.date_
LEFT JOIN internal_customer c 
    ON rs.cost_entity = c.cost_entity
    AND rs.date_ = c.date_
LEFT JOIN external_customer d 
    ON rs.cost_entity = d.cost_entity
    AND rs.date_ = d.date_
LEFT JOIN shipping_revenue e 
    ON rs.cost_entity = e.cost_entity
    AND rs.date_ = e.date_
LEFT JOIN vendor_support f 
    ON rs.cost_entity = f.cost_entity
    AND rs.date_ = f.date_
LEFT JOIN other_revenue g 
    ON rs.cost_entity = g.cost_entity
    AND rs.date_ = g.date_
LEFT JOIN ic_office h 
    ON rs.cost_entity = h.cost_entity
    AND rs.date_ = h.date_
WHERE rs.cost_entity IN ('B2C-Germany', 'Austria', 'Netherlands', 'Spain', 'B2B-Germany', 'United States')
;


GRANT SELECT ON finance.profit_and_loss_revenue_sources TO tableau;