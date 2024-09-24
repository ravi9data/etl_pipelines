-- master.subscription definition

-- Drop table

-- DROP TABLE master.subscription;

--DROP TABLE master.subscription;
CREATE TABLE IF NOT EXISTS master.subscription
(
	subscription_id VARCHAR(18) NOT NULL  ENCODE zstd
	,subscription_sf_id VARCHAR(80)   ENCODE zstd
	,order_created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,rank_subscriptions BIGINT   ENCODE zstd
	,first_subscription_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,subscriptions_per_customer BIGINT   ENCODE zstd
	,customer_id INTEGER   ENCODE az64
	,customer_type VARCHAR(65535)   ENCODE zstd
	,customer_acquisition_cohort TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,subscription_limit INTEGER   ENCODE zstd
	,order_id VARCHAR(255)   ENCODE zstd
	,store_id VARCHAR(24)   ENCODE zstd
	,store_name VARCHAR(510)   ENCODE zstd
	,store_label VARCHAR(30)   ENCODE zstd
	,store_type VARCHAR(7)   ENCODE zstd
	,store_number VARCHAR(2000)   ENCODE zstd
	,account_name VARCHAR(100)   ENCODE zstd
	,status VARCHAR(9)   ENCODE zstd
	,variant_sku VARCHAR(1300)   ENCODE zstd
	,allocation_status VARCHAR(255)   ENCODE zstd
	,allocation_tries DOUBLE PRECISION   ENCODE zstd
	,cross_sale_attempts INTEGER   ENCODE zstd
	,replacement_attempts INTEGER   ENCODE zstd
	,allocated_assets BIGINT   ENCODE zstd
	,delivered_assets BIGINT   ENCODE zstd
	,returned_packages BIGINT   ENCODE zstd
	,returned_assets BIGINT   ENCODE zstd
	,outstanding_assets BIGINT   ENCODE zstd
	,outstanding_asset_value DOUBLE PRECISION   ENCODE zstd
	,outstanding_residual_asset_value DOUBLE PRECISION   ENCODE zstd
	,outstanding_rrp DOUBLE PRECISION   ENCODE zstd
	,first_asset_delivery_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,last_return_shipment_at TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,subscription_plan VARCHAR(31)   ENCODE zstd
	,rental_period DOUBLE PRECISION   ENCODE zstd
	,subscription_value DOUBLE PRECISION   ENCODE zstd
	,subscription_value_euro DOUBLE PRECISION   ENCODE zstd
	,reporting_subscription_value_euro DOUBLE PRECISION   ENCODE zstd
	,committed_sub_value DOUBLE PRECISION   ENCODE zstd
	,next_due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,commited_sub_revenue_future DOUBLE PRECISION   ENCODE zstd
	,currency VARCHAR(255)   ENCODE zstd
	,subscription_duration INTEGER   ENCODE zstd
	,effective_duration DOUBLE PRECISION   ENCODE zstd
	,outstanding_duration DOUBLE PRECISION   ENCODE zstd
	,months_required_to_own VARCHAR(65535)   ENCODE zstd
	,max_payment_number INTEGER   ENCODE zstd
	,payment_count BIGINT   ENCODE zstd
	,paid_subscriptions BIGINT   ENCODE zstd
	,last_valid_payment_category VARCHAR(25)   ENCODE bytedict
	,dpd INTEGER   ENCODE zstd
	,subscription_revenue_due DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_paid DOUBLE PRECISION   ENCODE zstd
	,outstanding_subscription_revenue DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_refunded DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_chargeback DOUBLE PRECISION   ENCODE zstd
	,net_subscription_revenue_paid DOUBLE PRECISION   ENCODE zstd
	,cancellation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,cancellation_note VARCHAR(2048)   ENCODE zstd
	,cancellation_reason VARCHAR(382)   ENCODE zstd
	,cancellation_reason_new VARCHAR(56)   ENCODE zstd
	,cancellation_reason_churn VARCHAR(16)   ENCODE zstd
	,is_widerruf VARCHAR(24)   ENCODE zstd
	,payment_method VARCHAR(255)   ENCODE zstd
	,debt_collection_handover_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,dc_status VARCHAR(21)   ENCODE zstd
	,result_debt_collection_contact CHAR(255)   ENCODE zstd
	,avg_asset_purchase_price DOUBLE PRECISION   ENCODE zstd
	,product_sku VARCHAR(255)   ENCODE zstd
	,product_name VARCHAR(510)   ENCODE zstd
	,category_name VARCHAR(65535)   ENCODE zstd
	,subcategory_name VARCHAR(65535)   ENCODE zstd
	,brand VARCHAR(65535)   ENCODE zstd
	,new_recurring VARCHAR(9)   ENCODE zstd
	,retention_group VARCHAR(24)   ENCODE bytedict
	,minimum_cancellation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,minimum_term_months DOUBLE PRECISION   ENCODE zstd
	,asset_cashflow_from_old_subscriptions DOUBLE PRECISION   ENCODE zstd
	,exposure_to_default DOUBLE PRECISION   ENCODE zstd
	,mietkauf_amount_overpayment DOUBLE PRECISION   ENCODE RAW
	,is_eligible_for_mietkauf BOOLEAN   ENCODE RAW
	,trial_days INTEGER   ENCODE az64
	,trial_variant BOOLEAN   ENCODE RAW
	,is_not_triggered_payments INTEGER   ENCODE zstd
	,asset_recirculation_status VARCHAR(65535)   ENCODE zstd
	,store_short VARCHAR(20)   ENCODE zstd
	,country_name VARCHAR(510)   ENCODE zstd
	,store_commercial VARCHAR(523)   ENCODE zstd
	,subscription_bo_id VARCHAR(128)   ENCODE lzo
	,buyout_disabled BOOLEAN   ENCODE RAW
	,buyout_disabled_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,buyout_disabled_reason VARCHAR(65535)   ENCODE lzo
	,original_subscription_value DOUBLE PRECISION   ENCODE RAW
	,original_subscription_value_euro NUMERIC(10,2)   ENCODE az64
	,free_year_offer_start_date DATE   ENCODE az64
	,reactivated_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,additional_committed_sub_value VARCHAR(1)   ENCODE lzo
)
DISTSTYLE KEY
	,PRIMARY KEY (subscription_id)
 DISTKEY (subscription_id)
;
ALTER TABLE master.subscription owner to matillion;