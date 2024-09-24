-- master."order" definition

-- Drop table

-- DROP TABLE master."order";

--DROP TABLE master."order";
CREATE TABLE IF NOT EXISTS master."order"
(
	order_id VARCHAR(64) NOT NULL  ENCODE zstd
	,paid_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,customer_id INTEGER   ENCODE az64
	,store_id VARCHAR(24)   ENCODE zstd
	,created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,submitted_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,updated_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,approved_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,canceled_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,acquisition_date TIMESTAMP WITHOUT TIME ZONE   ENCODE zstd
	,status VARCHAR(765)   ENCODE zstd
	,order_rank BIGINT   ENCODE zstd
	,order_journey VARCHAR(55)   ENCODE zstd
	,total_orders BIGINT   ENCODE zstd
	,cancellation_reason VARCHAR(255)   ENCODE zstd
	,declined_reason VARCHAR(255)   ENCODE zstd
	,order_value NUMERIC(12,2)   ENCODE zstd
	,voucher_code VARCHAR(510)   ENCODE zstd
	,voucher_type VARCHAR(510)   ENCODE zstd
	,voucher_value VARCHAR(510)   ENCODE zstd
	,voucher_discount DOUBLE PRECISION   ENCODE zstd
	,is_in_salesforce BOOLEAN   ENCODE RAW
	,store_type VARCHAR(7)   ENCODE zstd
	,new_recurring VARCHAR(9)   ENCODE zstd
	,retention_group VARCHAR(24)   ENCODE zstd
	,marketing_channel VARCHAR(2000)   ENCODE zstd
	,marketing_campaign VARCHAR(2000)   ENCODE zstd
	,device VARCHAR(2000)   ENCODE zstd
	,store_name VARCHAR(510)   ENCODE zstd
	,store_label VARCHAR(30)   ENCODE zstd
	,store_short VARCHAR(20)   ENCODE zstd
	,store_commercial VARCHAR(523)   ENCODE zstd
	,store_country VARCHAR(510)   ENCODE zstd
	,store_number VARCHAR(2000)   ENCODE zstd
	,order_item_count BIGINT   ENCODE zstd
	,basket_size NUMERIC(38,2)   ENCODE zstd
	,is_trial_order INTEGER   ENCODE zstd
	,cart_orders INTEGER   ENCODE zstd
	,cart_page_orders INTEGER   ENCODE zstd
	,cart_logged_in_orders INTEGER   ENCODE zstd
	,address_orders INTEGER   ENCODE zstd
	,payment_orders INTEGER   ENCODE zstd
	,summary_orders INTEGER   ENCODE zstd
	,completed_orders INTEGER   ENCODE zstd
	,declined_orders INTEGER   ENCODE zstd
	,failed_first_payment_orders INTEGER   ENCODE zstd
	,cancelled_orders INTEGER   ENCODE zstd
	,paid_orders INTEGER   ENCODE zstd
	,avg_plan_duration BIGINT   ENCODE zstd
	,current_subscription_limit INTEGER   ENCODE zstd
	,burgel_risk_category VARCHAR(15)   ENCODE zstd
	,schufa_class VARCHAR(65535)   ENCODE zstd
	,scoring_decision VARCHAR(2048)   ENCODE zstd
	,scoring_reason VARCHAR(512)   ENCODE zstd
	,customer_type VARCHAR(65535)   ENCODE zstd
	,initial_scoring_decision VARCHAR(255)   ENCODE zstd
	,payment_method VARCHAR(255)   ENCODE zstd
	,PRIMARY KEY (order_id)
)
DISTSTYLE KEY
 DISTKEY (order_id)
;
ALTER TABLE master."order" owner to matillion;