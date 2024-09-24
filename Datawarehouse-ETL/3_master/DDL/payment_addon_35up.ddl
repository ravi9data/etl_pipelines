-- master.payment_addon_35up definition

-- Drop table

-- DROP TABLE master.payment_addon_35up;

DROP TABLE master.payment_addon_35up;
CREATE TABLE IF NOT EXISTS master.payment_addon_35up
(
	payment_id VARCHAR(255) PRIMARY KEY ENCODE lzo 
	,resource_id VARCHAR(255)   ENCODE lzo
	,movement_id VARCHAR(255)   ENCODE lzo
	,psp_reference_id VARCHAR(255)   ENCODE lzo
	,customer_id INTEGER   ENCODE az64
	,order_id VARCHAR(255)   ENCODE lzo
	,addon_id VARCHAR(255)   ENCODE lzo
	,addon_name VARCHAR(255)   ENCODE lzo
	,payment_type VARCHAR(255)   ENCODE lzo
	,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE lzo
	,updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,status VARCHAR(10)   ENCODE lzo
	,due_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,paid_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,money_received_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,currency VARCHAR(255)   ENCODE lzo
	,payment_method VARCHAR(255)   ENCODE lzo
	,payment_context_reason VARCHAR(255)   ENCODE lzo
	,invoice_url VARCHAR(1023)   ENCODE lzo
	,invoice_number VARCHAR(255)   ENCODE lzo
	,invoice_date VARCHAR(255)   ENCODE lzo
	,amount_due NUMERIC(22,6)   ENCODE az64
	,amount_paid NUMERIC(22,6)   ENCODE az64
	,amount_tax NUMERIC(22,6)   ENCODE az64
	,tax_rate NUMERIC(22,6)   ENCODE az64
	,pending_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,failed_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,attempts_to_pay BIGINT   ENCODE az64
	,failed_reason VARCHAR(16384)   ENCODE lzo
	,refund_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,refund_amount NUMERIC(22,6)   ENCODE az64
	,country_name VARCHAR(50)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE master.payment_addon_35up owner to matillion;