DROP TABLE IF EXISTS master.payment_addon;

CREATE TABLE master.payment_addon (
    payment_id VARCHAR(16383) primary key ENCODE lzo DISTKEY,
    resource_id VARCHAR(16383) ENCODE lzo,
    movement_id VARCHAR(16383) ENCODE lzo,
    psp_reference_id VARCHAR(16383) ENCODE lzo,
    customer_id INTEGER ENCODE az64,
    order_id VARCHAR(16383) ENCODE lzo,
    addon_id VARCHAR(16383) ENCODE lzo,
    addon_name VARCHAR(16383) ENCODE lzo,
    variant_id VARCHAR(16383) ENCODE lzo,
    payment_type VARCHAR(24574) ENCODE lzo,
    created_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    updated_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    status VARCHAR(7) ENCODE lzo,
    due_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    paid_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    money_received_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    currency VARCHAR(16383) ENCODE lzo,
    payment_method VARCHAR(16383) ENCODE lzo,
    payment_context_reason VARCHAR(16383) ENCODE lzo,
    invoice_url VARCHAR(65535) ENCODE lzo,
    invoice_number VARCHAR(256) ENCODE lzo,
    invoice_date VARCHAR(256) ENCODE lzo,
    amount_due NUMERIC(22,6) ENCODE az64,
    amount_paid NUMERIC(22,6) ENCODE az64,
    amount_tax NUMERIC(22,6) ENCODE az64,
    tax_rate NUMERIC(22,6) ENCODE az64,
    pending_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    failed_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    attempts_to_pay BIGINT ENCODE az64,
    failed_reason VARCHAR(16384) ENCODE lzo,
    refund_date TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
    refund_amount NUMERIC(22,6) ENCODE az64,
    country_name VARCHAR(13) ENCODE lzo
)
DISTSTYLE KEY;
