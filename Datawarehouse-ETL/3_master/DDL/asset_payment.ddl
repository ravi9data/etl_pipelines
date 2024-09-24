-- master.asset_payment definition

-- Drop table

-- DROP TABLE master.asset_payment;

--DROP TABLE master.asset_payment;
CREATE TABLE IF NOT EXISTS master.asset_payment
(
	payment_group_id VARCHAR(255)   ENCODE lzo
	,asset_payment_id VARCHAR(255)   ENCODE lzo
	,asset_payment_sfid VARCHAR(80)   ENCODE lzo
	,transaction_id VARCHAR(255)   ENCODE lzo
	,resource_id VARCHAR(255)   ENCODE lzo
	,movement_id VARCHAR(255)   ENCODE lzo
	,psp_reference_id VARCHAR(255)   ENCODE lzo
	,customer_id INTEGER   ENCODE az64
	,order_id VARCHAR(255)   ENCODE lzo
	,asset_id VARCHAR(255)   ENCODE lzo
	,allocation_id VARCHAR(255)   ENCODE lzo
	,subscription_id VARCHAR(255)   ENCODE lzo
	,related_subscription_payment_id VARCHAR(255)   ENCODE lzo
	,payment_type VARCHAR(255)   ENCODE lzo
	,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status VARCHAR(255)   ENCODE lzo
	,due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,paid_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,money_received_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,sold_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,paid_status VARCHAR(255)   ENCODE lzo
	,currency VARCHAR(255)   ENCODE lzo
	,payment_method VARCHAR(255)   ENCODE lzo
	,payment_method_funding VARCHAR(255)   ENCODE lzo
	,payment_method_details VARCHAR(255)   ENCODE lzo
	,invoice_url VARCHAR(255)   ENCODE lzo
	,invoice_number VARCHAR(255)   ENCODE lzo
	,invoice_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,invoice_sent_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,amount DOUBLE PRECISION   ENCODE RAW
	,amount_balance DOUBLE PRECISION   ENCODE RAW
	,amount_due DOUBLE PRECISION   ENCODE RAW
	,amount_paid DOUBLE PRECISION   ENCODE RAW
	,amount_tax DOUBLE PRECISION   ENCODE RAW
	,tax_rate DOUBLE PRECISION   ENCODE RAW
	,amount_voucher DOUBLE PRECISION   ENCODE RAW
	,failed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,failed_reason VARCHAR(755)   ENCODE lzo
	,failed_message VARCHAR(5000)   ENCODE lzo
	,refund_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,pending_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,last_try_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,next_try_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refund_amount DOUBLE PRECISION   ENCODE RAW
	,refund_transactions BIGINT   ENCODE az64
	,max_refund_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,min_refund_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,chargeback_amount DOUBLE PRECISION   ENCODE RAW
	,chargeback_transactions BIGINT   ENCODE az64
	,max_chargeback_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,min_chargeback_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,capital_source VARCHAR(255)   ENCODE lzo
	,booking_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,src_tbl VARCHAR(15)   ENCODE lzo
	,PRIMARY KEY (asset_payment_id)
)
DISTSTYLE KEY
DISTKEY (asset_payment_id)
;
ALTER TABLE master.asset_payment owner to matillion;