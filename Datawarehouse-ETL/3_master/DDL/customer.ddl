-- master.customer definition

-- Drop table

-- DROP TABLE master.customer;

--DROP TABLE master.customer;
CREATE TABLE IF NOT EXISTS master.customer
(
	customer_id INTEGER NOT NULL  ENCODE az64
	,company_id INTEGER   ENCODE az64
	,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,subscription_limit INTEGER   ENCODE zstd
	,subscription_limit_change_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,customer_type VARCHAR(65535)   ENCODE zstd
	,company_name VARCHAR(255)   ENCODE zstd
	,company_status VARCHAR(65535)   ENCODE zstd
	,company_type_name VARCHAR(65535)   ENCODE zstd
	,company_created_at TIMESTAMP WITHOUT TIME ZONE ENCODE zstd
	,billing_country VARCHAR(510)   ENCODE zstd
	,shipping_country VARCHAR(510)   ENCODE zstd
	,billing_city VARCHAR(765)   ENCODE zstd
	,billing_zip VARCHAR(510)   ENCODE zstd
	,shipping_city VARCHAR(128)   ENCODE zstd
	,shipping_zip VARCHAR(16)   ENCODE zstd
	,signup_language VARCHAR(510)   ENCODE zstd
	,default_locale VARCHAR(510)   ENCODE zstd
	,bundesland VARCHAR(2000)   ENCODE zstd
	,referral_code VARCHAR(65535)   ENCODE zstd
	,email_subscribe VARCHAR(12)   ENCODE zstd
	,initial_subscription_limit INTEGER   ENCODE zstd
	,subscription_limit_defined_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,customer_scoring_result VARCHAR(512)   ENCODE zstd
	,burgel_score NUMERIC(7,2)   ENCODE zstd
	,burgel_score_details VARCHAR(512)   ENCODE zstd
	,burgel_person_known BOOLEAN   ENCODE RAW
	,burgel_address_details BOOLEAN   ENCODE RAW
	,verita_score INTEGER   ENCODE zstd
	,verita_person_known_at_address BOOLEAN   ENCODE RAW
	,fraud_type VARCHAR(65535)   ENCODE zstd
	,trust_type VARCHAR(65535)   ENCODE zstd
	,min_fraud_detected TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,max_fraud_detected TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,schufa_class VARCHAR(65535)   ENCODE zstd
	,orders BIGINT   ENCODE zstd
	,completed_orders BIGINT   ENCODE zstd
	,paid_orders BIGINT   ENCODE zstd
	,declined_orders BIGINT   ENCODE zstd
	,last_order_created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_cart_product_names VARCHAR(65535)   ENCODE zstd
	,max_submitted_order_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,delivered_allocations BIGINT   ENCODE zstd
	,returned_allocations BIGINT   ENCODE zstd
	,outstanding_purchase_price DOUBLE PRECISION   ENCODE zstd
	,active_subscription_product_names VARCHAR(65535)   ENCODE zstd
	,active_subscription_subcategory VARCHAR(65535)   ENCODE zstd
	,active_subscription_category VARCHAR(65535)   ENCODE zstd
	,active_subscription_brand VARCHAR(65535)   ENCODE zstd
	,ever_rented_asset_purchase_price DOUBLE PRECISION   ENCODE zstd
	,active_subscription_value DOUBLE PRECISION   ENCODE zstd
	,committed_subscription_value DOUBLE PRECISION   ENCODE zstd
	,active_subscriptions BIGINT   ENCODE zstd
	,subscription_revenue_due DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_paid DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_refunded DOUBLE PRECISION   ENCODE zstd
	,subscription_revenue_chargeback DOUBLE PRECISION   ENCODE zstd
	,payment_count BIGINT   ENCODE zstd
	,paid_subscriptions BIGINT   ENCODE zstd
	,refunded_subscriptions BIGINT   ENCODE zstd
	,failed_subscriptions BIGINT   ENCODE zstd
	,chargeback_subscriptions BIGINT   ENCODE zstd
	,burgel_risk_category VARCHAR(15)   ENCODE zstd
	,subscriptions BIGINT   ENCODE zstd
	,start_date_of_first_subscription TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,first_subscription_store VARCHAR(65535)   ENCODE zstd
	,first_subscription_acquisition_channel VARCHAR(65535)   ENCODE zstd
	,second_subscription_store VARCHAR(65535)   ENCODE zstd
	,subscription_durations VARCHAR(65535)   ENCODE zstd
	,first_subscription_duration VARCHAR(65535)   ENCODE zstd
	,second_subscription_duration VARCHAR(65535)   ENCODE zstd
	,subs_wearables BIGINT   ENCODE zstd
	,subs_drones BIGINT   ENCODE zstd
	,subs_cameras BIGINT   ENCODE zstd
	,subs_phones_and_tablets BIGINT   ENCODE zstd
	,subs_computers BIGINT   ENCODE zstd
	,subs_gaming BIGINT   ENCODE zstd
	,subs_audio BIGINT   ENCODE zstd
	,subs_other BIGINT   ENCODE zstd
	,first_subscription_product_category VARCHAR(65535)   ENCODE zstd
	,second_subscription_product_category VARCHAR(65535)   ENCODE zstd
	,ever_rented_products VARCHAR(65535)   ENCODE zstd
	,ever_rented_sku VARCHAR(65535)   ENCODE zstd
	,ever_rented_brands VARCHAR(65535)   ENCODE zstd
	,ever_rented_subcategories VARCHAR(65535)   ENCODE zstd
	,ever_rented_categories VARCHAR(65535)   ENCODE zstd
	,clv DOUBLE PRECISION   ENCODE zstd
	,voucher_usage VARCHAR(65535)   ENCODE zstd
	,minimum_cancellation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,minimum_cancellation_product VARCHAR(65535)   ENCODE zstd
	,subs_pag BIGINT   ENCODE zstd
	,subs_1m BIGINT   ENCODE zstd
	,subs_3m BIGINT   ENCODE zstd
	,subs_6m BIGINT   ENCODE zstd
	,subs_12m BIGINT   ENCODE zstd
	,subs_24m BIGINT   ENCODE zstd
	,is_bad_customer BOOLEAN   ENCODE RAW
	,customer_acquisition_cohort TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,customer_acquisition_subscription_id VARCHAR(18)   ENCODE zstd
	,customer_acquisition_rental_plan VARCHAR(31)   ENCODE zstd
	,customer_acquisition_category_name VARCHAR(65535)   ENCODE zstd
	,customer_acquisition_subcategory_name VARCHAR(65535)   ENCODE zstd
	,customer_acquisition_product_brand VARCHAR(65535)   ENCODE zstd
	,rfm_score INTEGER   ENCODE zstd
	,rfm_segment VARCHAR(20)   ENCODE zstd
	,signup_country VARCHAR(65535)   ENCODE zstd
	,max_cancellation_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,max_asset_delivered_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,crm_label VARCHAR(10)   ENCODE zstd
	,profile_status VARCHAR(10)   ENCODE zstd
	,age VARCHAR(65535) ENCODE zstd
	,crm_label_braze VARCHAR(200)   ENCODE zstd
 	,ever_rented_variant_sku VARCHAR(65535)   ENCODE zstd
	,PRIMARY KEY (customer_id)
)
DISTSTYLE KEY
 DISTKEY (customer_id)
;
ALTER TABLE master.customer owner to matillion;