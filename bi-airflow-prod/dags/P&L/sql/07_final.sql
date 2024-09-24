DROP TABLE IF EXISTS finance.profit_and_loss_final;
 
CREATE TABLE finance.profit_and_loss_final AS 
WITH country_view AS (
SELECT
    rs.date_
    ,rs.cost_entity
    ,sum(rs.gross_subscription_revenue) as gross_subscription_revenue
    ,sum(rs.voucher_discount) as voucher_discount
    ,sum(rs.voucher_discount_reallocation) as voucher_discount_reallocation
    ,sum(rs.internal_customer) as internal_customer
    ,sum(rs.external_customer) as external_customer
    ,sum(rs.shipping_revenue) as shipping_revenue
    ,sum(rs.vendor_support) as vendor_support 
    ,sum(rs.other_revenue) as other_revenue
    ,sum(rs.ic_office) as ic_office
    ,sum(vc.incoming_goods) as incoming_goods
    ,sum(vc.refurishment_testing_grading) as refurishment_testing_grading
    ,sum(vc.warehouse_management) as warehouse_management
    ,sum(vc.outgoing_goods) as outgoing_goods
    ,sum(vc.repairs) AS repairs
    ,sum(vc.recommerce) as recommerce
    ,sum(vc.standard_shipping) as standard_shipping
    ,sum(vc."2_man_handling") as "2_man_handling"
    ,sum(vc.other_shipping_costs) as other_shipping_costs
    ,sum(vc.payment_costs) as payment_costs
    ,sum(vc.accessories) as accessories
    ,sum(vc.packaging_materials) as packaging_materials
    ,sum(vc.standard_depreciation) as standard_depreciation
    ,sum(vc.non_active_assets_standard_depreciation) as non_active_assets_standard_depreciation
    ,sum(vc.lost) as lost
    ,sum(vc.sold) as sold
    ,sum(vc.dc) as dc
    ,sum(vc.asset_disposal_non_active_assets) as asset_disposal_non_active_assets
    ,sum(vc.transfer_pricing_base) as transfer_pricing_base
    ,sum(mc.performance_marketing) as performance_marketing
    ,sum(mc.branding) as branding
    ,sum(mc.partners) as partners
    ,sum(mc.other_marketing_costs) as other_marketing_costs
    ,sum(mc.b2b_salaries) as b2b_salaries
    ,sum(mc.internal_personnel_cost_t_and_d) as internal_personnel_cost_t_and_d
    ,sum(mc.external_personnel_cost_t_and_d) as external_personnel_cost_t_and_d
    ,sum(mc.tools_and_infrastructure) as tools_and_infrastructure
    ,sum(mc.risk_data) as risk_data
    ,sum(mc.other_t_and_d_costs) as other_t_and_d_costs
    ,sum(mc.personnel_costs_non_t_and_d) as personnel_costs_non_t_and_d
    ,sum(oe.asset_insurance) as asset_insurance
    ,sum(oe.liability_insurance) as liability_insurance
    ,sum(oe.credit_and_fraud_insurance) as credit_and_fraud_insurance
    ,sum(oe.warehouse_insurance) as warehouse_insurance
    ,sum(oe.d_o_company_insurance) as d_o_company_insurance
    ,sum(oe.other_insurances) as other_insurances
    ,sum(oe.accounting_tax_audit_services) as accounting_tax_audit_services
    ,sum(oe.legal_complaince_without_fundraising) as legal_complaince_without_fundraising
    ,sum(oe.other_consulting) as other_consulting
    ,sum(oe.office_rent) as office_rent
    ,sum(oe.office_management) as office_management
    ,sum(oe.other_general_expenses) as other_general_expenses
    ,sum(oe.financing_costs_assets) as financing_costs_assets
    ,sum(oe.non_cash_marketing_expenses) as non_cash_marketing_expenses
    ,sum(oe.country_launch_costs) as country_launch_costs  
FROM finance.profit_and_loss_revenue_sources rs 
LEFT JOIN finance.profit_and_loss_variable_cost vc
    ON rs.cost_entity = vc.cost_entity
    AND rs.date_ = vc.date_
LEFT JOIN finance.profit_and_loss_marketing_costs mc 
    ON rs.cost_entity = mc.cost_entity
    AND rs.date_ = mc.date_
LEFT JOIN finance.profit_and_loss_other_expenses oe 
    ON vc.cost_entity = oe.cost_entity
    AND rs.date_ = oe.date_
GROUP BY 1,2  
)
,base AS (
    SELECT 
        DISTINCT
            date_
            ,cost_entity
            ,sum (gross_subscription_revenue) + sum (voucher_discount) + sum(voucher_discount_reallocation) AS subscription 
            ,sum (internal_customer) + sum(COALESCE (external_customer,0)) AS asset
            ,sum(incoming_goods) + sum(refurishment_testing_grading) + sum (warehouse_management) + sum(outgoing_goods) AS warehouse 
            ,sum(accessories) + sum(packaging_materials) AS accessories_and_packaging
            ,sum(COALESCE(standard_shipping,0)) +  sum(COALESCE("2_man_handling",0)) + sum(COALESCE (other_shipping_costs,0)) AS shipping_costs
            ,CASE 
                WHEN cost_entity IN ('B2B-Germany', 'Austria', 'Netherlands', 'Spain') 
                    THEN (warehouse + sum(repairs) + sum(recommerce) + shipping_costs + sum(payment_costs) + accessories_and_packaging) * sum(transfer_pricing_base)
                WHEN cost_entity = 'United States' 
                    THEN (sum(payment_costs) + sum(accessories)) * sum(transfer_pricing_base) 
             END AS transfer_pricing
            ,warehouse + sum(repairs) + sum(recommerce) + shipping_costs + sum(payment_costs) + accessories_and_packaging + COALESCE (transfer_pricing,0) AS variable_cost
            ,sum(standard_depreciation) + sum(non_active_assets_standard_depreciation) + sum(lost) + sum(sold) + sum(dc) + sum(asset_disposal_non_active_assets) AS depreciation_and_amortization
            ,variable_cost + depreciation_and_amortization  AS cost_of_revenue
            ,subscription + sum(internal_customer)+sum(COALESCE (external_customer,0))+sum(COALESCE (shipping_revenue,0))+sum(COALESCE (vendor_support,0))+sum(COALESCE (other_revenue,0))+sum(COALESCE (ic_office,0)) AS net_revenue
            ,net_revenue + cost_of_revenue AS gross_profit
            ,sum(performance_marketing) AS performance_marketing
            ,sum(branding) AS branding
            ,sum(internal_personnel_cost_t_and_d) + sum(external_personnel_cost_t_and_d) + sum(tools_and_infrastructure) + sum(risk_data) + COALESCE (sum(other_t_and_d_costs),0) AS technology_and_development
            ,sum(accounting_tax_audit_services) + sum(legal_complaince_without_fundraising) + sum(other_consulting) AS legal_and_consulting
            ,sum(asset_insurance) + sum(liability_insurance) + sum(credit_and_fraud_insurance) + sum(warehouse_insurance) +sum(d_o_company_insurance) + sum(other_insurances) AS insurance
            ,sum(other_general_expenses) + sum(office_management) + sum(office_rent) + legal_and_consulting + insurance + sum(personnel_costs_non_t_and_d) AS general_and_admin
            ,sum(performance_marketing) + sum(branding) + sum(partners) + sum(other_marketing_costs) + sum(b2b_salaries) AS marketing
            ,gross_profit + marketing + technology_and_development + general_and_admin AS ebit_adjusted
            ,sum(financing_costs_assets) AS financing_costs
            ,sum(coalesce(non_cash_marketing_expenses,0)) + sum(COALESCE (country_launch_costs,0)) AS extraordinary_items
            ,ebit_adjusted + financing_costs + extraordinary_items AS net_income
        FROM country_view
    WHERE cost_entity IN ('B2C-Germany', 'B2B-Germany', 'Austria', 'Netherlands', 'Spain', 'United States')
    GROUP BY 1,2  
)
,transfer_pricing_b2c AS (
    SELECT 
       b.date_
       ,b.cost_entity
        --b.*                                                                                                                                                                                                                                             
       ,-(sum(b.transfer_pricing) OVER (PARTITION BY b.date_)) AS transfer_pricing_b2c
    FROM base b   
    )
SELECT 
   c.date_
    ,c.cost_entity
    ,c.gross_subscription_revenue
    ,c.voucher_discount
    ,c.voucher_discount_reallocation
    ,b.subscription
    ,c.internal_customer 
    ,c.external_customer
    ,b.asset
    ,c.shipping_revenue
    ,c.vendor_support
    ,c.other_revenue
    ,c.ic_office
    ,b.net_revenue
    ,c.incoming_goods
    ,c.refurishment_testing_grading
    ,c.warehouse_management
    ,c.outgoing_goods
    ,b.warehouse
    ,c.repairs
    ,c.recommerce
    ,c.standard_shipping
    ,c."2_man_handling"
    ,c.other_shipping_costs
    ,b.shipping_costs
    ,c.payment_costs
    ,c.accessories
    ,c.packaging_materials
    ,b.accessories_and_packaging
    ,c.standard_depreciation
    ,c.non_active_assets_standard_depreciation
    ,c.lost
    ,c.sold
    ,c.dc
    ,c.asset_disposal_non_active_assets
    ,b.depreciation_and_amortization
    ,Coalesce(b.transfer_pricing,tp.transfer_pricing_b2c) AS transfer_pricing
    ,CASE WHEN c.cost_entity <> 'B2C-Germany' 
            THEN b.variable_cost
          WHEN c.cost_entity = 'B2C-Germany'
            THEN b.warehouse + c.repairs + c.recommerce + b.shipping_costs + c.payment_costs + b.accessories_and_packaging + tp.transfer_pricing_b2c 
     END AS variable_cost       
    ,CASE WHEN c.cost_entity <> 'B2C-Germany' 
            THEN b.depreciation_and_amortization + b.variable_cost 
          WHEN c.cost_entity = 'B2C-Germany'
            THEN (b.warehouse + c.repairs + c.recommerce + b.shipping_costs + c.payment_costs + b.accessories_and_packaging + tp.transfer_pricing_b2c) + b.depreciation_and_amortization
     END AS cost_of_revenue
    ,CASE WHEN c.cost_entity <> 'B2C-Germany'
            THEN b.gross_profit
          WHEN c.cost_entity = 'B2C-Germany' 
            THEN (b.net_revenue + (b.warehouse + c.repairs + c.recommerce + b.shipping_costs + c.payment_costs + b.accessories_and_packaging + tp.transfer_pricing_b2c) + b.depreciation_and_amortization)
     END AS gross_profit 
    ,c.performance_marketing
    ,c.branding
    ,c.partners
    ,c.other_marketing_costs
    ,c.b2b_salaries
    ,b.marketing
    ,c.internal_personnel_cost_t_and_d
    ,c.external_personnel_cost_t_and_d
    ,c.tools_and_infrastructure
    ,c.risk_data
    ,c.other_t_and_d_costs
    ,b.technology_and_development
    ,c.personnel_costs_non_t_and_d 
    ,c.asset_insurance
    ,c.liability_insurance
    ,c.credit_and_fraud_insurance
    ,c.warehouse_insurance
    ,c.d_o_company_insurance
    ,c.other_insurances
    ,b.insurance
    ,c.accounting_tax_audit_services
    ,c.legal_complaince_without_fundraising
    ,c.other_consulting
    ,b.legal_and_consulting
    ,b.general_and_admin
    ,CASE WHEN c.cost_entity <> 'B2C-Germany' 
            THEN b.ebit_adjusted
          WHEN c.cost_entity = 'B2C-Germany' 
             THEN (NULLIF(b.net_revenue,0) + (b.warehouse + c.repairs + c.recommerce + b.shipping_costs + c.payment_costs + b.accessories_and_packaging + tp.transfer_pricing_b2c) 
                + b.depreciation_and_amortization) + b.marketing + b.technology_and_development + b.general_and_admin 
     END AS ebit_adjusted
    ,CASE WHEN c.cost_entity <> 'B2C-Germany'
            THEN b.ebit_adjusted /NULLIF(b.net_revenue,0)
          WHEN c.cost_entity = 'B2C-Germany'  
            THEN ((NULLIF(b.net_revenue,0) + (b.warehouse + c.repairs + c.recommerce + b.shipping_costs + c.payment_costs + b.accessories_and_packaging + tp.transfer_pricing_b2c) 
                + b.depreciation_and_amortization) + b.marketing + b.technology_and_development + b.general_and_admin) /NULLIF(b.net_revenue,0)   
      END AS ebit_margin_adjusted
    ,c.office_rent
    ,c.office_management
    ,c.other_general_expenses
    ,c.financing_costs_assets
    ,b.financing_costs
    ,c.non_cash_marketing_expenses
    ,c.country_launch_costs
    ,b.extraordinary_items 
    ,CASE WHEN c.cost_entity <> 'B2C-Germany'
            THEN b.net_income
          WHEN c.cost_entity = 'B2C-Germany' 
            THEN (NULLIF(b.net_revenue,0) + (b.warehouse + c.repairs + c.recommerce + b.shipping_costs + c.payment_costs + b.accessories_and_packaging + tp.transfer_pricing_b2c) 
                + b.depreciation_and_amortization) + b.marketing + b.technology_and_development + b.general_and_admin + b.financing_costs + b.extraordinary_items
     END AS net_income 
    ,CASE WHEN c.cost_entity <> 'B2C-Germany'
            THEN NULLIF(b.net_revenue,0)/NULLIF(b.net_revenue,0)
          WHEN c.cost_entity = 'B2C-Germany'
          --THEN b.net_income/b.net_revenue
            THEN ((NULLIF(b.net_revenue,0) + (b.warehouse + c.repairs + c.recommerce + b.shipping_costs + c.payment_costs + b.accessories_and_packaging + tp.transfer_pricing_b2c) 
                + b.depreciation_and_amortization) + b.marketing + b.technology_and_development + b.general_and_admin + b.financing_costs + b.extraordinary_items) /NULLIF(b.net_revenue,0)
     END AS net_income_margin  
FROM country_view c
LEFT JOIN base b
    ON c.cost_entity = b.cost_entity
    AND c.date_ = b.date_
LEFT JOIN transfer_pricing_b2c tp 
    ON b.cost_entity= tp.cost_entity
    AND b.date_= tp.date_
;   


GRANT SELECT ON finance.profit_and_loss_final TO tableau;