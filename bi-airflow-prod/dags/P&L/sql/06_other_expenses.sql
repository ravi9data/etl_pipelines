DROP TABLE IF EXISTS finance.profit_and_loss_other_expenses;

CREATE TABLE finance.profit_and_loss_other_expenses AS 
WITH asset_insurance AS ( 
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round (sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS asset_insurance
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        --AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'asset_insurance'
        AND b.cost_entity = 'group_consol'
)
, liability_insurance AS ( 
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS liability_insurance
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        --AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'liability_insurance'
        AND b.cost_entity = 'group_consol'
)
, credit_and_fraud_insurance AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS credit_and_fraud_insurance
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        --AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'credits_and_fraud_insurance'
        AND b.cost_entity = 'group_consol'
)
, warehouse_insurance AS ( 
    SELECT
        rs.date_
        ,rs.cost_entity
        ,round (sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS warehouse_insurance
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        --AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'warehouse_insurance'
        AND b.cost_entity = 'group_consol'
)
, d_o_company_insurance AS ( 
    SELECT
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS d_o_company_insurance
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        --AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'DO_company_insurance'
        AND b.cost_entity = 'group_consol'
)
, other_insurances AS ( 
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round (sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS other_insurances
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        --AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'other_insurance'
        AND b.cost_entity = 'group_consol'
)
, accounting_tax_audit_services AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS accounting_tax_audit_services
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        --AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'accounting_tax_audit_services'
        AND b.cost_entity = 'group_consol'
)
,legal_complaince_without_fundraising AS (
    SELECT
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS legal_complaince_without_fundraising
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        --AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Netherlands', 'Austria', 'Spain', 'United States')
        AND b.dimension = 'legal_complaince_without_fundraising'
        AND b.cost_entity = 'group_consol'
)
, other_consulting AS (
    SELECT 
        b.date_
        ,b.cost_entity 
        ,round (b.amount / -1000) AS other_consulting
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'colpari_cost_split'
)
, office_rent AS ( 
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS office_rent
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
        ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria')
        AND b.dimension = 'office_rent'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'office_rent'
        AND b2.cost_entity = 'United States'
    UNION ALL 
    SELECT 
        b.date_
        ,b.cost_entity
        ,round (b.amount) AS office_rent
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'office_rent'
        AND b.cost_entity = 'United States'
)
, office_management AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round (sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER () * b.amount) AS office_management
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND b.dimension = 'office_management'
        AND b.cost_entity = 'group_consol'
)
, other_general_expenses AS (
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,round(sum(rs.amount) OVER (PARTITION BY rs.cost_entity) / sum(rs.amount) OVER ()  * (b.amount - b2.amount)) AS other_general_expenses
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_ = b.date_
    LEFT JOIN finance.profit_and_loss_input b2
        ON rs.date_ = b2.date_
    WHERE TRUE 
        AND rs.dimension = 'gross_subscription_revenue'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Spain', 'Netherlands', 'Austria', 'United States')
        AND b.dimension = 'other_g&a_expenses'
        AND b.cost_entity = 'group_consol'
        AND b2.dimension = 'other_g&a_expenses'
        AND b2.cost_entity = 'Card'
)
, financing_costs_assets AS (
    SELECT
        b.date_
        ,b.cost_entity 
        ,round(b.amount / -1000) AS financing_costs_assets
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'financing_cost_split'
)
, non_cash_marketing_expenses AS ( 
    SELECT 
        rs.date_
        ,rs.cost_entity
        ,b.amount * round(sum(rs.amount/1000) OVER (PARTITION BY rs.cost_entity)/ sum(rs.amount/1000) OVER ()) AS non_cash_marketing_expenses
    FROM finance.profit_and_loss_redshift rs
    LEFT JOIN finance.profit_and_loss_input b
        ON rs.date_=b.date_
    WHERE TRUE
        AND rs.dimension ='gross_subscription_revenue'
        AND rs.cost_entity IN ('B2C-Germany', 'B2B-Germany')
        AND b.dimension='non_cash_marketing_expenses'
        AND b.cost_entity='group_consol'
)
, country_launch_costs AS (
    SELECT 
        b.date_
        ,b.cost_entity
        ,round(b.amount / 1000) AS country_launch_costs
    FROM finance.profit_and_loss_input b
    WHERE TRUE 
        AND b.dimension = 'country_launch_costs'
)
SELECT DISTINCT 
rs.date_
,rs.cost_entity 
,COALESCE (a.asset_insurance,0) AS asset_insurance
,COALESCE (b.liability_insurance,0) AS liability_insurance 
,COALESCE (c.credit_and_fraud_insurance,0) AS credit_and_fraud_insurance
,COALESCE (d.warehouse_insurance,0) AS warehouse_insurance
,COALESCE (e.d_o_company_insurance,0) AS d_o_company_insurance
,COALESCE (f.other_insurances,0) AS other_insurances
,COALESCE (g.accounting_tax_audit_services,0) AS accounting_tax_audit_services
,COALESCE (h.legal_complaince_without_fundraising,0) AS legal_complaince_without_fundraising
,COALESCE (i.other_consulting,0) AS other_consulting
,COALESCE (j.office_rent,0) AS office_rent  
,COALESCE (k.office_management,0) AS office_management
,COALESCE (l.other_general_expenses,0) AS other_general_expenses
,COALESCE (m.financing_costs_assets,0) AS financing_costs_assets
,COALESCE (n.non_cash_marketing_expenses,0) AS non_cash_marketing_expenses
,COALESCE (o.country_launch_costs,0) AS country_launch_costs
FROM finance.profit_and_loss_redshift rs 
LEFT JOIN asset_insurance a
    ON rs.cost_entity = a.cost_entity
    AND rs.date_ = a.date_
LEFT JOIN liability_insurance b
    ON rs.cost_entity = b.cost_entity
     AND rs.date_ = b.date_
LEFT JOIN credit_and_fraud_insurance c
    ON rs.cost_entity = c.cost_entity
     AND rs.date_ = c.date_
LEFT JOIN warehouse_insurance d
    ON rs.cost_entity = d.cost_entity
     AND rs.date_ = d.date_
LEFT JOIN d_o_company_insurance e
    ON rs.cost_entity = e.cost_entity
     AND rs.date_ = e.date_
LEFT JOIN other_insurances f
    ON rs.cost_entity = f.cost_entity
     AND rs.date_ = f.date_
LEFT JOIN accounting_tax_audit_services g
    ON rs.cost_entity = g.cost_entity
     AND rs.date_ = g.date_
LEFT JOIN legal_complaince_without_fundraising h
    ON rs.cost_entity = h.cost_entity
     AND rs.date_ = h.date_
LEFT JOIN other_consulting  i
    ON rs.cost_entity = i.cost_entity
     AND rs.date_ = i.date_
LEFT JOIN office_rent  j
    ON rs.cost_entity = j.cost_entity
     AND rs.date_ = j.date_
LEFT JOIN office_management k
    ON rs.cost_entity = k.cost_entity
     AND rs.date_ = k.date_
LEFT JOIN other_general_expenses  l
    ON rs.cost_entity = l.cost_entity
     AND rs.date_ = l.date_
LEFT JOIN financing_costs_assets  m
    ON rs.cost_entity = m.cost_entity
     AND rs.date_ = m.date_
LEFT JOIN non_cash_marketing_expenses n
    ON rs.cost_entity = n.cost_entity
     AND rs.date_ = n.date_
LEFT JOIN country_launch_costs  o
    ON rs.cost_entity = o.cost_entity
     AND rs.date_ = o.date_
WHERE rs.cost_entity IN ('B2C-Germany', 'Austria', 'Spain', 'Netherlands', 'B2B-Germany', 'United States')
;


GRANT SELECT ON finance.profit_and_loss_other_expenses TO tableau;