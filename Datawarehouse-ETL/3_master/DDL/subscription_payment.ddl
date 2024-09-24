-- master.subscription_payment definition

-- Drop table

-- DROP TABLE master.subscription_payment;

--DROP TABLE master.subscription_payment;
CREATE TABLE IF NOT EXISTS master.subscription_payment
(
	payment_group_id VARCHAR(255)   ENCODE lzo
	,payment_id VARCHAR(255)   ENCODE lzo
	,payment_sfid VARCHAR(80)   ENCODE lzo
	,transaction_id VARCHAR(255)   ENCODE lzo
	,resource_id VARCHAR(255)   ENCODE lzo
	,movement_id VARCHAR(255)   ENCODE lzo
	,psp_reference VARCHAR(255)   ENCODE lzo
	,customer_id BIGINT   ENCODE AZ64
	,customer_type VARCHAR(255)   ENCODE lzo
	,order_id VARCHAR(255)   ENCODE lzo
	,asset_id VARCHAR(255)   ENCODE LZO
	,brand VARCHAR(255)   ENCODE lzo
	,allocation_id VARCHAR(255)   ENCODE LZO
	,category_name VARCHAR(255)   ENCODE LZO
	,subcategory_name VARCHAR(255)   ENCODE LZO
	,subscription_id VARCHAR(255)   ENCODE LZO
	,asset_was_delivered BOOLEAN   ENCODE RAW
	,asset_was_returned BOOLEAN   ENCODE RAW
	,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE AZ64
	,updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE AZ64
	,subscription_start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE AZ64
	,billing_period_start TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,billing_period_end TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,payment_number VARCHAR(24)   ENCODE lzo
	,payment_type VARCHAR(255)   ENCODE lzo
	,due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE AZ64
	,paid_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,money_received_at TIMESTAMP WITHOUT TIME ZONE   ENCODE AZ64
	,failed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attempts_to_pay VARCHAR(24)   ENCODE LZO
	,subscription_payment_category VARCHAR(25)   ENCODE lzo
	,default_new VARCHAR(14)   ENCODE lzo
	,status VARCHAR(384)   ENCODE lzo
	,payment_processor_message VARCHAR(5000)   ENCODE lzo
	,payment_method_details VARCHAR(255)   ENCODE lzo
	,paid_status VARCHAR(255)   ENCODE lzo
	,next_due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,dpd INTEGER   ENCODE az64
	,payment_method_detailed VARCHAR(256)   ENCODE lzo
	,currency VARCHAR(255)   ENCODE lzo	
	,amount_due DOUBLE PRECISION   ENCODE RAW
	,amount_paid DOUBLE PRECISION   ENCODE RAW	
	,amount_subscription DOUBLE PRECISION   ENCODE RAW	
	,amount_shipment DOUBLE PRECISION   ENCODE RAW
	,amount_voucher DOUBLE PRECISION   ENCODE RAW
	,amount_discount DOUBLE PRECISION   ENCODE RAW
	,amount_tax DOUBLE PRECISION   ENCODE RAW
	,tax_rate DOUBLE PRECISION   ENCODE RAW
	,amount_overdue_fee DOUBLE PRECISION   ENCODE RAW
	,amount_refund DOUBLE PRECISION   ENCODE RAW
	,amount_chargeback DOUBLE PRECISION   ENCODE RAW
	,debt_collection_handover_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,result_debt_collection_contact CHAR(255)   ENCODE lzo
	,is_dc_collections BOOLEAN   ENCODE RAW
	,is_eligible_for_refund VARCHAR(19)   ENCODE LZO
	,subscription_plan VARCHAR(31)   ENCODE LZO
	,country_name VARCHAR(510)   ENCODE lzo
	,store_label VARCHAR(30)   ENCODE lzo
	,store_name VARCHAR(2510)   ENCODE lzo
	,store_short VARCHAR(20)   ENCODE lzo
	,file_path VARCHAR(512)   ENCODE LZO
	,burgel_risk_category VARCHAR(15)   ENCODE lzo
	,covenant_exclusion_criteria VARCHAR(21)   ENCODE lzo
	,invoice_url VARCHAR(16383)   ENCODE lzo
	,invoice_number VARCHAR(16383)   ENCODE lzo
	,invoice_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,invoice_sent_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,capital_source VARCHAR(255)   ENCODE lzo
	,amount_overdue_pbi_adjusted DOUBLE PRECISION   ENCODE RAW
	,payment_status_pbi_adjusted VARCHAR(256)   ENCODE LZO
	,payment_method VARCHAR(255)   ENCODE lzo
	,src_tbl VARCHAR(15)   ENCODE lzo
	,PRIMARY KEY (payment_id)	
)
DISTSTYLE KEY
 DISTKEY (payment_id)
 SORTKEY (
	subscription_id
	, payment_number
	)
;
ALTER TABLE master.subscription_payment owner to matillion;