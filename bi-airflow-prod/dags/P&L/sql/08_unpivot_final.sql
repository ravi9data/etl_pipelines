DROP TABLE IF EXISTS finance.unpivoted_profit_and_loss_final;

CREATE TABLE finance.unpivoted_profit_and_loss_final AS 
WITH unpivoted_profit_and_loss_final AS (
    SELECT *
    FROM (SELECT *
          FROM radwa_hosny.pnl_testing) UNPIVOT (
            amount FOR metric  
            IN (gross_subscription_revenue
                ,voucher_discount
                ,voucher_discount_reallocation
                ,subscription
                ,internal_customer
                ,external_customer
                ,asset
                ,shipping_revenue
                ,vendor_support
                ,other_revenue
                ,ic_office
                ,net_revenue
                ,incoming_goods
                ,refurishment_testing_grading
                ,warehouse_management
                ,outgoing_goods
                ,warehouse
                ,repairs
                ,recommerce
                ,standard_shipping
                ,"2_man_handling"
                ,other_shipping_costs
                ,shipping_costs
                ,payment_costs
                ,accessories
                ,packaging_materials
                ,accessories_and_packaging
                ,standard_depreciation
                ,non_active_assets_standard_depreciation
                ,lost
                ,sold
                ,dc
                ,asset_disposal_non_active_assets
                ,depreciation_and_amortization
                ,transfer_pricing
                ,variable_cost      
                ,cost_of_revenue
                ,gross_profit     
                ,performance_marketing
                ,branding
                ,partners
                ,other_marketing_costs
                ,b2b_salaries
                ,marketing
                ,internal_personnel_cost_t_and_d
                ,external_personnel_cost_t_and_d
                ,tools_and_infrastructure
                ,risk_data
                ,other_t_and_d_costs
                ,technology_and_development
                ,personnel_costs_non_t_and_d 
                ,asset_insurance
                ,liability_insurance
                ,credit_and_fraud_insurance
                ,warehouse_insurance
                ,d_o_company_insurance
                ,other_insurances
                ,insurance
                ,accounting_tax_audit_services
                ,legal_complaince_without_fundraising
                ,other_consulting
                ,legal_and_consulting
                ,general_and_admin
                ,ebit_adjusted
                ,ebit_margin_adjusted          
                ,office_rent
                ,office_management
                ,other_general_expenses
                ,financing_costs_assets
                ,financing_costs
                ,non_cash_marketing_expenses
                ,country_launch_costs
                ,extraordinary_items
                ,net_income 
                ,net_income_margin))
)
SELECT *
    ,CASE 
        WHEN metric IN ('gross_subscription_revenue', 'voucher_discount', 'voucher_discount_reallocation') 
            THEN 'Subscription'
        WHEN metric IN ('internal_customer', 'external_customer')
            THEN 'Assets'
        WHEN metric = 'shipping_revenue' 
            THEN 'Shipping Revenue'
        WHEN metric = 'vendor_support'
            THEN 'Vendor Support'
        WHEN metric = 'other_revenue'
            THEN 'Other Revenue'
        WHEN metric = 'ic_office'
            THEN 'IC Office'
        WHEN metric = 'net_revenue'
            THEN 'Net Revenue'
        WHEN metric IN ('incoming_goods','refurishment_testing_grading','warehouse_management','outgoing_goods') 
            THEN 'Warehouse'
        WHEN metric IN ( 'standard_shipping','2_man_handling', 'other_shipping_costs')
            THEN 'Shipping Costs'
        WHEN metric IN ('accessories','packaging_materials')
            THEN 'Accessories & Packaging'
        WHEN metric IN ('standard_depreciation' ,'non_active_assets_standard_depreciation','lost','sold','dc','asset_disposal_non_active_assets')
            THEN 'Depreciation & Amortization '
        WHEN metric = 'repairs'
            THEN 'Repairs'
        WHEN metric = 'recommerce'
            THEN 'Recommerce'
        WHEN metric = 'payment_costs'
            THEN 'Payment Costs'
        WHEN metric = 'transfer_pricing'
            THEN 'Transfer Pricing'
        WHEN metric = 'variable_cost'
            THEN 'Variable Cost'
        WHEN metric = 'cost_of_revenue'
            THEN 'Cost of Revenue'
        WHEN metric = 'gross_profit'
            THEN 'Gross Profit'
        WHEN metric IN ('performance_marketing','branding','partners','other_marketing_costs', 'b2b_salaries')
            THEN 'Marketing'
        WHEN metric = 'internal_personnel_cost_t_and_d'
            THEN 'Internal Personnel Costs T&D'
        WHEN metric = 'external_personnel_cost_t_and_d'
            THEN 'External Personnel Costs T&D'
        WHEN metric = 'tools_and_infrastructure'
            THEN 'Tools and Infrastructure'
        WHEN metric = 'risk_data'
            THEN 'Risk Data'
        WHEN metric = 'other_t_and_d_costs'
            THEN 'Other T&D Costs'
        WHEN metric = 'personnel_costs_non_t_and_d'  
            THEN 'Personnel Costs Non T&D'
        WHEN metric = 'technology_and_development' 
            THEN 'Technology & Development'
        WHEN metric IN ('asset_insurance','liability_insurance','credit_and_fraud_insurance','warehouse_insurance','d_o_company_insurance','other_insurances')
            THEN 'Insurance'
        WHEN metric IN ('accounting_tax_audit_services','legal_complaince_without_fundraising','other_consulting')  
            THEN 'Legal & Consulting'
        WHEN metric = 'office_rent' 
            THEN 'Office Rent'
        WHEN metric = 'office_management'
            THEN 'Office Management'
        WHEN  metric =  'other_general_expenses'
            THEN 'Other G&A Expenses'
        WHEN metric = 'general_and_admin'
            THEN 'General & Adminstrative'
        WHEN metric = 'ebit_adjusted'
            THEN 'EBIT Adjusted'
        WHEN metric = 'ebit_margin_adjusted'
            THEN 'EBIT Margin Adjusted'
        WHEN metric =  'financing_costs_assets' 
            THEN 'Financing Costs'
        WHEN metric IN ('non_cash_marketing_expenses','country_launch_costs' ) 
            THEN 'Extraordinary Items'
        WHEN metric = 'net_income'
            THEN 'Net Income'
        WHEN metric = 'net_income_margin'
            THEN 'Net Income Margin'
    END AS main_label
FROM unpivoted_profit_and_loss_final
WHERE TRUE 
    AND main_label IS NOT NULL 
;


GRANT SELECT ON finance.unpivoted_profit_and_loss_final TO tableau;