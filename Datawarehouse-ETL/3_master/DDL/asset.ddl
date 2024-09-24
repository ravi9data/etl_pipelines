-- master.asset definition

-- Drop table

-- DROP TABLE master.asset;

--DROP TABLE master.asset;
CREATE TABLE IF NOT EXISTS master.asset
(
	asset_id VARCHAR(18) NOT NULL  ENCODE zstd
	,customer_id VARCHAR(255)   ENCODE zstd
	,subscription_id VARCHAR(18)   ENCODE zstd
	,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,asset_allocation_id VARCHAR(50)   ENCODE zstd
	,asset_allocation_sf_id VARCHAR(80)   ENCODE zstd
	,warehouse VARCHAR(255)   ENCODE zstd
	,capital_source_name VARCHAR(80)   ENCODE zstd
	,supplier VARCHAR(65535)   ENCODE zstd
	,first_allocation_store VARCHAR(60)   ENCODE zstd
	,first_allocation_store_name VARCHAR(60)   ENCODE zstd
	,first_allocation_customer_type VARCHAR(60)   ENCODE zstd
	,serial_number VARCHAR(80)   ENCODE zstd
	,ean VARCHAR(65535)   ENCODE zstd
	,product_sku VARCHAR(255)   ENCODE zstd
	,asset_name VARCHAR(255)   ENCODE zstd
	,asset_condition VARCHAR(1300)   ENCODE zstd
	,asset_condition_spv VARCHAR(4)   ENCODE zstd
	,variant_sku VARCHAR(65535)   ENCODE zstd
	,product_name VARCHAR(510)   ENCODE zstd
	,category_name VARCHAR(65535)   ENCODE zstd
	,subcategory_name VARCHAR(65535)   ENCODE zstd
	,brand VARCHAR(65535)   ENCODE zstd
	,invoice_url VARCHAR(255)   ENCODE zstd
	,total_allocations_per_asset BIGINT   ENCODE zstd
	,asset_order_number VARCHAR(255)   ENCODE zstd
	,purchase_request_item_sfid VARCHAR(255)   ENCODE zstd
	,purchase_request_item_id VARCHAR(255)   ENCODE zstd
	,request_id VARCHAR(255)   ENCODE zstd
	,purchased_date DATE   ENCODE az64
	,months_since_purchase INTEGER   ENCODE az64
	,days_since_purchase INTEGER   ENCODE az64
	,days_on_book INTEGER   ENCODE az64
	,months_on_book INTEGER   ENCODE az64
	,amount_rrp DOUBLE PRECISION   ENCODE zstd
	,initial_price DOUBLE PRECISION   ENCODE zstd
	,residual_value_market_price DOUBLE PRECISION   ENCODE zstd
	,last_month_residual_value_market_price DOUBLE PRECISION   ENCODE zstd
	,average_of_sources_on_condition_this_month DOUBLE PRECISION   ENCODE zstd
	,average_of_sources_on_condition_last_available_price NUMERIC(38,4)   ENCODE zstd
	,sold_price DOUBLE PRECISION   ENCODE zstd
	,sold_date DATE   ENCODE az64
	,currency VARCHAR(255)   ENCODE lzo
	,asset_status_original VARCHAR(255)   ENCODE zstd
	,asset_status_new VARCHAR(30)   ENCODE zstd
	,asset_status_detailed VARCHAR(255)   ENCODE zstd
	,lost_reason VARCHAR(255)   ENCODE zstd
	,lost_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_allocation_days_in_stock DOUBLE PRECISION   ENCODE RAW
	,last_allocation_dpd INTEGER   ENCODE az64
	,dpd_bucket VARCHAR(15)   ENCODE zstd
	,subscription_revenue DOUBLE PRECISION   ENCODE zstd
	,amount_refund DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_due DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_last_31day DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_last_month DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_current_month DOUBLE PRECISION   ENCODE zstd
	,avg_subscription_amount DOUBLE PRECISION   ENCODE zstd
	,max_subscription_amount DOUBLE PRECISION   ENCODE zstd
	,payments_due BIGINT   ENCODE zstd
	,last_payment_amount_due BIGINT   ENCODE zstd
	,last_payment_amount_paid BIGINT   ENCODE az64
	,payments_paid BIGINT   ENCODE zstd
	,shipment_cost_paid DOUBLE PRECISION   ENCODE zstd
	,repair_cost_paid DOUBLE PRECISION   ENCODE zstd
	,customer_bought_paid DOUBLE PRECISION   ENCODE zstd
	,additional_charge_paid DOUBLE PRECISION   ENCODE zstd
	,delivered_allocations BIGINT   ENCODE zstd
	,returned_allocations BIGINT   ENCODE zstd
	,max_paid_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,office_or_sponsorships VARCHAR(22)   ENCODE zstd
	,last_market_valuation DOUBLE PRECISION   ENCODE zstd
	,last_valuation_report_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,asset_value_linear_depr DOUBLE PRECISION   ENCODE zstd
	,asset_value_linear_depr_book DOUBLE PRECISION   ENCODE zstd
	,market_price_at_purchase_date DOUBLE PRECISION   ENCODE zstd
	,active_subscription_id VARCHAR(65535)   ENCODE zstd
	,active_subscriptions_bom BIGINT   ENCODE zstd
	,active_subscriptions BIGINT   ENCODE zstd
	,acquired_subscriptions BIGINT   ENCODE zstd
	,cancelled_subscriptions BIGINT   ENCODE zstd
	,active_subscription_value DOUBLE PRECISION   ENCODE bytedict
	,acquired_subscription_value DOUBLE PRECISION   ENCODE zstd
	,rollover_subscription_value DOUBLE PRECISION   ENCODE bytedict
	,cancelled_subscription_value DOUBLE PRECISION   ENCODE zstd
	,shipping_country VARCHAR(80)   ENCODE zstd
	,asset_sold_invoice VARCHAR(65535)   ENCODE zstd
	,invoice_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,invoice_number VARCHAR(255)   ENCODE zstd
	,invoice_total DOUBLE PRECISION   ENCODE RAW
	,revenue_share BOOLEAN   ENCODE RAW
	,first_order_id VARCHAR(255)   ENCODE zstd
	,country VARCHAR(80)   ENCODE zstd
	,city VARCHAR(40)   ENCODE zstd
	,postal_code VARCHAR(20)   ENCODE zstd
	,asset_cashflow_from_old_subscriptions DOUBLE PRECISION   ENCODE zstd
	,last_active_subscription_id character varying(25) ENCODE lzo
	,PRIMARY KEY (asset_id)
)
DISTSTYLE KEY
 DISTKEY (asset_id)
;
ALTER TABLE master.asset owner to matillion;
ALTER TABLE master.asset ADD purchase_price_commercial double precision
