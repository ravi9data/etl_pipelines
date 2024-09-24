CREATE TABLE IF NOT EXISTS ods_spv_historical.luxco_reporting_{{ params.tbl_suffix }}_snapshot
(
reporting_date DATE ENCODE az64
,asset_id VARCHAR(18) ENCODE lzo
,serial_number VARCHAR(80) ENCODE lzo
,brand VARCHAR(65535) ENCODE lzo
,asset_name VARCHAR(255) ENCODE lzo
,product_sku VARCHAR(255) ENCODE lzo
,warehouse VARCHAR(255) ENCODE lzo
,capital_source_name VARCHAR(80) ENCODE lzo
,category VARCHAR(65535) ENCODE lzo
,subcategory VARCHAR(65535) ENCODE lzo
,purchased_date DATE ENCODE az64
,purchase_price DOUBLE PRECISION ENCODE RAW
,condition_on_purchase VARCHAR(4) ENCODE lzo
,condition VARCHAR(4) ENCODE lzo
,asset_added_to_portfolio INTEGER ENCODE az64
,asset_added_to_portfolio_m_and_g INTEGER ENCODE az64
,days_in_stock DOUBLE PRECISION ENCODE RAW
,last_allocation_dpd INTEGER ENCODE az64
,dpd_bucket_r VARCHAR(20) ENCODE lzo
,asset_position VARCHAR(20) ENCODE lzo
,asset_classification VARCHAR(8) ENCODE lzo
,asset_classification_m_and_g VARCHAR(8) ENCODE lzo
,asset_status_original VARCHAR(255) ENCODE lzo
,allocation_status_original VARCHAR(255) ENCODE lzo
,m_since_last_valuation_price BIGINT ENCODE az64
,valuation_method VARCHAR(20) ENCODE lzo
,currency VARCHAR(255) ENCODE lzo
,discounted_purchase_price DOUBLE PRECISION ENCODE RAW
,original_valuation DOUBLE PRECISION ENCODE RAW
,impairment_rate NUMERIC(3,2) ENCODE az64
,final_valuation DOUBLE PRECISION ENCODE RAW
,previous_original_valuation DOUBLE PRECISION ENCODE RAW
,previous_final_valuation DOUBLE PRECISION ENCODE RAW
,committed_subscription_value DOUBLE PRECISION ENCODE RAW
,commited_sub_revenue_future DOUBLE PRECISION ENCODE RAW
,depreciation DOUBLE PRECISION ENCODE RAW
,depreciation_buckets VARCHAR(12) ENCODE lzo
,subscription_id VARCHAR(18) ENCODE lzo
,status VARCHAR(9) ENCODE lzo
,subscription_plan VARCHAR(31) ENCODE lzo
,start_date DATE ENCODE az64
,year_r DOUBLE PRECISION ENCODE RAW
,month_r INTEGER ENCODE az64
,day_r DOUBLE PRECISION ENCODE RAW
,replacement_date DATE ENCODE az64
,maturity_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,committed_duration DOUBLE PRECISION ENCODE RAW
,current_duration_calculated BIGINT ENCODE az64
,effective_duration DOUBLE PRECISION ENCODE RAW
,outstanding_duration_calcuated DOUBLE PRECISION ENCODE RAW
,current_subscription_amount DOUBLE PRECISION ENCODE RAW
,avg_subscription_amount DOUBLE PRECISION ENCODE RAW
,max_subscription_amount DOUBLE PRECISION ENCODE RAW
,total_subscriptions_per_asset BIGINT ENCODE az64
,customer_id VARCHAR(255) ENCODE lzo
,customer_type VARCHAR(65535) ENCODE lzo
,country VARCHAR(510) ENCODE lzo
,city VARCHAR(128) ENCODE lzo
,postal_code VARCHAR(20) ENCODE lzo
,customer_risk_category VARCHAR(15) ENCODE lzo
,customer_active_subscriptions BIGINT ENCODE az64
,customer_net_revenue_paid DOUBLE PRECISION ENCODE RAW
,customer_acquisition_month TIMESTAMP WITHOUT TIME ZONE ENCODE az64
,cust_acquisition_channel VARCHAR(65535) ENCODE lzo
,subscription_revenue_paid_reporting_month DOUBLE PRECISION ENCODE RAW
,other_revenue_paid_reporting_month DOUBLE PRECISION ENCODE RAW
,total_asset_inflow_reporting_month DOUBLE PRECISION ENCODE RAW
,refunds_chb_subscription_paid_reporting_month DOUBLE PRECISION ENCODE RAW
,refunds_chb_others_paid_reporting_month_r DOUBLE PRECISION ENCODE RAW
,total_refunds_chb_reporting_month DOUBLE PRECISION ENCODE RAW
,total_net_asset_inflow_reporting_month DOUBLE PRECISION ENCODE RAW
,subscription_revenue_due_reporting_month DOUBLE PRECISION ENCODE RAW
,other_revenue_due_reporting_month DOUBLE PRECISION ENCODE RAW
,total_revenue_due_reporting_month DOUBLE PRECISION ENCODE RAW
,subscription_revenue_overdue_reporting_month DOUBLE PRECISION ENCODE RAW
,other_revenue_overdue_reporting_month DOUBLE PRECISION ENCODE RAW
,total_revenue_overdue_reporting_month DOUBLE PRECISION ENCODE RAW
,subscription_revenue_paid_lifetime DOUBLE PRECISION ENCODE RAW
,other_charges_paid_lifetime_r DOUBLE PRECISION ENCODE RAW
,total_asset_inflow_lifetime DOUBLE PRECISION ENCODE RAW
,refunds_chb_subscription_paid_lifetime DOUBLE PRECISION ENCODE RAW
,refunds_chb_others_paid_lifetime_r DOUBLE PRECISION ENCODE RAW
,total_refunds_chb_lifetime DOUBLE PRECISION ENCODE RAW
,total_net_inflow_lifetime DOUBLE PRECISION ENCODE RAW
,subscription_revenue_due_lifetime DOUBLE PRECISION ENCODE RAW
,other_revenue_due_lifetime DOUBLE PRECISION ENCODE RAW
,total_revenue_due_lifetime DOUBLE PRECISION ENCODE RAW
,subscription_revenue_overdue_lifetime DOUBLE PRECISION ENCODE RAW
,other_revenue_overdue_lifetime DOUBLE PRECISION ENCODE RAW
,overdue_balance_lifetime DOUBLE PRECISION ENCODE RAW
,is_sold INTEGER ENCODE az64
,is_sold_reporting_month INTEGER ENCODE az64
,sold_price DOUBLE PRECISION ENCODE RAW
,sold_date DATE ENCODE az64
,asset_status_detailed VARCHAR(255) ENCODE lzo
,sold_asset_status_classification VARCHAR(42) ENCODE lzo
,last_market_valuation DOUBLE PRECISION ENCODE RAW
,total_profit DOUBLE PRECISION ENCODE RAW
,return_on_asset DOUBLE PRECISION ENCODE RAW
,active_subscriptions BIGINT ENCODE az64
,active_subscriptions_bom BIGINT ENCODE az64
,acquired_subscriptions BIGINT ENCODE az64
,rollover_subscriptions BIGINT ENCODE az64
,cancelled_subscriptions BIGINT ENCODE az64
,active_subscriptions_eom BIGINT ENCODE az64
,active_subscription_value DOUBLE PRECISION ENCODE RAW
,acquired_subscription_value DOUBLE PRECISION ENCODE RAW
,rollover_subscription_value DOUBLE PRECISION ENCODE RAW
,cancelled_subscription_value DOUBLE PRECISION ENCODE RAW
,active_subscription_value_eom DOUBLE PRECISION ENCODE RAW
,months_rqd_to_own INTEGER ENCODE az64
,date_to_own DATE ENCODE az64
,months_left_required_to_own BIGINT ENCODE az64
,shifted_creation_cohort DATE ENCODE az64
,customer_score VARCHAR(16383) ENCODE lzo
,credit_buro VARCHAR(11) ENCODE lzo
,is_risky_customer BOOLEAN ENCODE RAW
,asset_purchase_discount_percentage DOUBLE PRECISION ENCODE RAW
,market_price_at_purchase_date DOUBLE PRECISION ENCODE RAW
,rentalco VARCHAR(510) ENCODE lzo
,asset_transfer BIGINT ENCODE az64
,asset_value_type VARCHAR(2000) ENCODE lzo
,asset_value_transfer NUMERIC(38,6) ENCODE az64
,asset_transfer_date DATE ENCODE az64
,final_valuation_before_impairment_m_n_g DOUBLE PRECISION ENCODE RAW
,original_valuation_without_written_off DOUBLE PRECISION   ENCODE RAW
,snapshot_timestamp TIMESTAMP WITH TIME ZONE ENCODE az64
)
DISTSTYLE AUTO;

insert into ods_spv_historical.luxco_reporting_{{ params.tbl_suffix }}_snapshot
select
reporting_date,asset_id,serial_number,brand,asset_name,product_sku,warehouse,capital_source_name,category,subcategory,
purchased_date,purchase_price,condition_on_purchase,"condition",asset_added_to_portfolio,asset_added_to_portfolio_m_and_g,
days_in_stock,last_allocation_dpd,dpd_bucket_r,asset_position,asset_classification,asset_classification_m_and_g,
asset_status_original,allocation_status_original,m_since_last_valuation_price,valuation_method,currency,
discounted_purchase_price,original_valuation,impairment_rate,final_valuation,previous_original_valuation,previous_final_valuation,
committed_subscription_value,commited_sub_revenue_future,depreciation,depreciation_buckets,subscription_id,status,
subscription_plan,start_date,year_r,month_r,day_r,replacement_date,maturity_date,committed_duration,current_duration_calculated,
effective_duration,outstanding_duration_calcuated,current_subscription_amount,avg_subscription_amount,max_subscription_amount,
total_subscriptions_per_asset,customer_id,customer_type,country,city,postal_code,customer_risk_category,
customer_active_subscriptions,customer_net_revenue_paid,customer_acquisition_month,cust_acquisition_channel,
subscription_revenue_paid_reporting_month,other_revenue_paid_reporting_month,total_asset_inflow_reporting_month,
refunds_chb_subscription_paid_reporting_month,refunds_chb_others_paid_reporting_month_r,total_refunds_chb_reporting_month,total_net_asset_inflow_reporting_month,subscription_revenue_due_reporting_month,other_revenue_due_reporting_month,total_revenue_due_reporting_month,subscription_revenue_overdue_reporting_month,other_revenue_overdue_reporting_month,total_revenue_overdue_reporting_month,subscription_revenue_paid_lifetime,other_charges_paid_lifetime_r,total_asset_inflow_lifetime,refunds_chb_subscription_paid_lifetime,refunds_chb_others_paid_lifetime_r,total_refunds_chb_lifetime,total_net_inflow_lifetime,subscription_revenue_due_lifetime,other_revenue_due_lifetime,total_revenue_due_lifetime,subscription_revenue_overdue_lifetime,other_revenue_overdue_lifetime,overdue_balance_lifetime,is_sold,is_sold_reporting_month,sold_price,sold_date,asset_status_detailed,sold_asset_status_classification,last_market_valuation,total_profit,return_on_asset,active_subscriptions,active_subscriptions_bom,acquired_subscriptions,rollover_subscriptions,cancelled_subscriptions,active_subscriptions_eom,active_subscription_value,acquired_subscription_value,rollover_subscription_value,cancelled_subscription_value,active_subscription_value_eom,months_rqd_to_own,date_to_own,months_left_required_to_own,shifted_creation_cohort,customer_score,credit_buro,is_risky_customer,asset_purchase_discount_percentage,market_price_at_purchase_date,rentalco,asset_transfer,asset_value_type,asset_value_transfer,
asset_transfer_date,final_valuation_before_impairment_m_n_g,original_valuation_without_written_off,
current_timestamp as snapshot_timestamp
from ods_spv_historical.luxco_reporting_{{ params.tbl_suffix }};

DELETE FROM ods_spv_historical.luxco_reporting_snapshot
WHERE reporting_date < ('{{ params.tbl_suffix }}'||'01')::Date ;   -- delete previous month data

INSERT INTO ods_spv_historical.luxco_reporting_snapshot
SELECT * FROM ods_spv_historical.luxco_reporting_{{ params.tbl_suffix }};