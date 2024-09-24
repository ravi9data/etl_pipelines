-- master.refund_payment definition

-- Drop table

-- DROP TABLE master.refund_payment;

--DROP TABLE master.refund_payment;
CREATE TABLE IF NOT EXISTS master.refund_payment
(
     payment_group_id VARCHAR(255)   ENCODE lzo
	,refund_payment_id VARCHAR(255)   ENCODE lzo
	,refund_payment_sfid VARCHAR(80)   ENCODE lzo
	,transaction_id VARCHAR(255)   ENCODE lzo
	,resource_id VARCHAR(255)   ENCODE lzo
	,movement_id VARCHAR(255)   ENCODE lzo
	,psp_reference_id VARCHAR(255)   ENCODE lzo
	,refund_type VARCHAR(255)   ENCODE lzo
	,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,created_by VARCHAR(18)   ENCODE lzo
	,updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status VARCHAR(255)   ENCODE lzo
	,subscription_payment_id VARCHAR(80)   ENCODE lzo
	,asset_payment_id VARCHAR(80)   ENCODE lzo
	,asset_id VARCHAR(18)   ENCODE lzo
	,customer_id INTEGER   ENCODE az64
	,order_id VARCHAR(255)   ENCODE lzo
	,subscription_id VARCHAR(18)   ENCODE lzo
	,currency VARCHAR(255)   ENCODE lzo
	,payment_method VARCHAR(255)   ENCODE lzo
	,amount DOUBLE PRECISION   ENCODE RAW
	,amount_due DOUBLE PRECISION   ENCODE RAW
	,amount_tax DOUBLE PRECISION   ENCODE RAW
	,amount_refunded DOUBLE PRECISION   ENCODE RAW
	,amount_refunded_sum DOUBLE PRECISION   ENCODE RAW
	,amount_repaid DOUBLE PRECISION   ENCODE RAW
	,paid_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,money_received_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,repaid_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,failed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,cancelled_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,pending_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,reason VARCHAR(255)   ENCODE lzo
	,capital_source VARCHAR(255)   ENCODE lzo
	,related_payment_type VARCHAR(20)   ENCODE lzo
	,related_payment_type_detailed VARCHAR(255)   ENCODE lzo
	,tax_rate DOUBLE PRECISION   ENCODE RAW
	,store_country VARCHAR(510)   ENCODE lzo
	,src_tbl VARCHAR(17)   ENCODE lzo
	,PRIMARY KEY (refund_payment_id)
)
DISTSTYLE KEY
 DISTKEY (refund_payment_id)
;
ALTER TABLE master.refund_payment owner to matillion;