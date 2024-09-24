DROP TABLE IF EXISTS finance.profit_and_loss_variable_cost;

CREATE TABLE finance.profit_and_loss_variable_cost AS 
WITH incoming_goods AS (
    SELECT
         rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS incoming_goods
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_standard_shipments'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'incoming_goods'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'incoming_goods'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT
         b.date_
        ,b.cost_entity
        ,round(b.amount) AS incoming_goods
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'incoming_goods'
        AND b.cost_entity = 'United States'
)
,refurishment_testing_grading AS (
    SELECT 
         rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS refurishment_testing_grading
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'returns'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'refurishment_testing_grading'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'refurishment_testing_grading'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT  
         b.date_
        ,b.cost_entity
        ,round(b.amount) AS refurishment_testing_grading
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'refurishment_testing_grading'
        AND b.cost_entity = 'United States'
)
,warehouse_management as(
    SELECT 
         rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS warehouse_management
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_standard_shipments'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'warehouse_management'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'warehouse_management'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT 
         b.date_
        ,b.cost_entity
        ,b.amount AS warehouse_management
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'warehouse_management'
        AND b.cost_entity = 'United States'
)
, outgoing_goods AS (
    SELECT 
         rs.date_
        ,rs.cost_entity
        ,round (sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS outgoing_goods
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_standard_shipments'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'outgoing_goods'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'outgoing_goods'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT 
         b.date_
        ,b.cost_entity
        ,b.amount AS outgoing_goods
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'outgoing_goods'
        AND b.cost_entity = 'United States'
)
, repairs AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS repairs
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'returns'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'repairs'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'repairs'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT 
        b.date_
        ,b.cost_entity
        ,b.amount AS repairs
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'repairs'
        AND b.cost_entity = 'United States'
)
,recommerce AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount)) AS recommerce 
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'returns'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'recommerce'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'recommerce'
        AND b2.cost_entity = 'United States'
    UNION ALL  
    SELECT 
        b.date_
        ,b.cost_entity
        ,b.amount AS recommerce 
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'recommerce'
        AND b.cost_entity = 'United States'
)
, standard_shipping AS (
    SELECT
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS standard_shipping   
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_standard_shipments'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'standard_shipping'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'standard_shipping'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT
        b.date_
        ,b.cost_entity
        ,b.amount AS standard_shipping   
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'standard_shipping'
        AND b.cost_entity = 'United States'
)
, two_man_handling AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS "2_man_handling" 
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_bulky_shipments'
        AND rs.cost_entity <> 'United States'
        AND b.dimension = '2_man_handling'
        AND b.cost_entity = 'group_consol'
)
, other_shipping_costs AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS other_shipping_costs 
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_standard_shipments'
        AND rs.cost_entity <> 'United States'
        AND b.dimension = 'other_shipping_costs'
        AND b.cost_entity = 'group_consol'
)
, payment_costs AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS payment_costs
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_subscriptions'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'payment_costs'
        AND b.cost_entity = 'group_consol'
)
, accessories AS (
    SELECT
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS accessories 
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_standard_shipments'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'accessories'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'accessories'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT
        b.date_
        ,b.cost_entity
        ,b.amount AS accessories 
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'accessories'
        AND b.cost_entity = 'United States'
)
, packaging_materials AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS packaging_materials
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
    ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'nr_of_standard_shipments'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'packaging_materials'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'packaging_materials'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT
        b.date_
        ,b.cost_entity
        ,b.amount AS packaging_materials
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'packaging_materials'
        AND b.cost_entity = 'United States'
)
, standard_depreciation AS (
    SELECT 
        b.date_
        ,b.cost_entity
        ,round(b.amount / - 1000) AS standard_depreciation
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'standard_depreciation_split'
)
, non_active_assets_standard_depreciation AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount * 0.8)) AS non_active_assets_standard_depreciation
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity IN ('B2C-Germany', 'Spain', 'Netherlands', 'Austria','United States')
        AND b.dimension = 'non_active_asset_standard_depreciation'
        AND b.cost_entity = 'group_consol'
    UNION ALL 
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(b.amount * 0.2) AS non_active_assets_standard_depreciation
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity = 'B2B-Germany'
        AND b.dimension = 'non_active_asset_standard_depreciation'
        AND b.cost_entity = 'group_consol'
)
, lost AS (
    SELECT 
        b.date_
        ,b.cost_entity
        ,round (b.amount / -1000) AS lost 
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'lost_split'
)
, sold AS (
    SELECT 
        b.date_
        ,b.cost_entity
        ,round(b.amount / -1000) AS sold
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'sold_split'
)
, dc_split AS (
    SELECT 
        b.date_
        ,b.cost_entity
        ,round(b.amount / -1000) AS dc 
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'dc_split'
)
, asset_disposal_non_active_assets AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount * 0.8)) AS asset_disposal_non_active_assets
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity IN ('B2C-Germany', 'Spain', 'Netherlands', 'Austria','United States')
        AND b.dimension = 'asset_disposal_non_active_assets'
        AND b.cost_entity = 'group_consol'
    UNION ALL 
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(b.amount * 0.2) AS asset_disposal_non_active_assets
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
    ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity = 'B2B-Germany'
        AND b.dimension = 'asset_disposal_non_active_assets'
        AND b.cost_entity = 'group_consol'
)
,transfer_pricing_base AS (
    SELECT 
        b.date_
        ,b.cost_entity
        ,(b.amount / 100) AS transfer_pricing_base
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'transfer_pricing'
        AND b.cost_entity IN ('Austria', 'Netherlands', 'Spain', 'B2B-Germany', 'United States')
)
SELECT DISTINCT 
    rs.date_
    ,rs.cost_entity
    ,COALESCE (a.incoming_goods,0) AS incoming_goods
    ,COALESCE (b.refurishment_testing_grading,0) AS refurishment_testing_grading
    ,COALESCE (c.warehouse_management,0) AS warehouse_management
    ,COALESCE (d.outgoing_goods,0) AS outgoing_goods
   -- ,incoming_goods + refurishment_testing_grading + warehouse_management + outgoing_goods AS warehouse
    ,COALESCE (e.repairs,0) AS repairs
    ,COALESCE (f.recommerce,0) AS recommerce
    ,COALESCE (g.standard_shipping,0) AS standard_shipping
    ,COALESCE (h."2_man_handling",0) AS "2_man_handling"
    ,COALESCE (i.other_shipping_costs,0) AS other_shipping_costs
   -- ,COALESCE (standard_shipping + "2_man_handling" + other_shipping_costs,0) AS shipping_costs
    ,COALESCE (j.payment_costs,0) AS payment_costs
    ,COALESCE (k.accessories,0) AS accessories
    ,COALESCE (l.packaging_materials,0) AS packaging_materials
    --,accessories + packaging_materials AS accessories_and_packaging
    ,COALESCE (m.standard_depreciation,0) AS standard_depreciation
    ,COALESCE (n.non_active_assets_standard_depreciation,0) AS non_active_assets_standard_depreciation
    ,COALESCE (o.lost,0) AS lost
    ,COALESCE (p.sold,0) AS sold
    ,COALESCE (q.dc,0) AS dc
    ,COALESCE (r.asset_disposal_non_active_assets,0) AS asset_disposal_non_active_assets
    --,lost + sold + dc + asset_disposal_non_active_assets AS gain_loss_from_asset_disposal
   -- ,standard_depreciation + non_active_assets_standard_depreciation + gain_loss_from_asset_disposal  AS depreciation_and_amortization
    ,COALESCE (s.transfer_pricing_base,0) AS transfer_pricing_base
FROM finance.profit_and_loss_redshift rs
LEFT JOIN incoming_goods a
    ON rs.cost_entity = a.cost_entity
    AND rs.date_ = a.date_
LEFT JOIN refurishment_testing_grading b
    ON rs.cost_entity = b.cost_entity
    AND rs.date_ = b.date_
LEFT JOIN warehouse_management c
    ON rs.cost_entity = c.cost_entity
    AND rs.date_ = c.date_
LEFT JOIN outgoing_goods d
    ON rs.cost_entity = d.cost_entity
    AND rs.date_ = d.date_
LEFT JOIN repairs e
    ON rs.cost_entity = e.cost_entity
    AND rs.date_ = e.date_
LEFT JOIN recommerce f
    ON rs.cost_entity = f.cost_entity
    AND rs.date_ = f.date_
LEFT JOIN standard_shipping g
    ON rs.cost_entity = g.cost_entity
    AND rs.date_ = g.date_
LEFT JOIN two_man_handling h
    ON rs.cost_entity = h.cost_entity
    AND rs.date_ = h.date_
LEFT JOIN other_shipping_costs i
    ON rs.cost_entity = i.cost_entity
    AND rs.date_ = i.date_
LEFT JOIN payment_costs j
    ON rs.cost_entity = j.cost_entity
    AND rs.date_ = j.date_
LEFT JOIN accessories k
    ON rs.cost_entity = k.cost_entity
    AND rs.date_ = k.date_
LEFT JOIN packaging_materials l
    ON rs.cost_entity = l.cost_entity
    AND rs.date_ = l.date_
LEFT JOIN standard_depreciation m
    ON rs.cost_entity = m.cost_entity
    AND rs.date_ = m.date_
LEFT JOIN non_active_assets_standard_depreciation n
    ON rs.cost_entity = n.cost_entity
    AND rs.date_ = n.date_
LEFT JOIN lost o
    ON rs.cost_entity = o.cost_entity
    AND rs.date_ = o.date_
LEFT JOIN sold p
    ON rs.cost_entity = p.cost_entity
    AND rs.date_ = p.date_
LEFT JOIN dc_split q
    ON rs.cost_entity = q.cost_entity
    AND rs.date_ = q.date_
LEFT JOIN asset_disposal_non_active_assets r
    ON rs.cost_entity = r.cost_entity
    AND rs.date_ = r.date_
LEFT JOIN transfer_pricing_base s
    ON rs.cost_entity = s.cost_entity
    AND rs.date_ = s.date_
WHERE rs.cost_entity IN ('B2C-Germany', 'Austria', 'Spain', 'Netherlands', 'B2B-Germany', 'United States')
;


GRANT SELECT ON finance.profit_and_loss_variable_cost TO tableau;