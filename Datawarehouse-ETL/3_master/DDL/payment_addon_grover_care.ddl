
-- Drop table

    payment_id VARCHAR(255) PRIMARY KEY ENCODE LZO,
    resource_id VARCHAR(255) ENCODE LZO,
    movement_id VARCHAR(255) ENCODE LZO,
    payment_group_id VARCHAR(65535) ENCODE LZO,
    psp_reference_id VARCHAR(255) ENCODE LZO,
    customer_id INTEGER ENCODE AZ64,
    order_id VARCHAR(255) ENCODE LZO,
    subscription_id VARCHAR(255) ENCODE LZO,
    payment_type VARCHAR(255) ENCODE LZO,
    payment_number VARCHAR(255) ENCODE LZO,
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    updated_at TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    status VARCHAR(10) ENCODE LZO,
    due_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    paid_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    money_received_at TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    currency VARCHAR(255) ENCODE LZO,
    payment_method VARCHAR(255) ENCODE LZO,
    payment_context_reason VARCHAR(255) ENCODE LZO,
    invoice_url VARCHAR(65535) ENCODE LZO,
    invoice_number VARCHAR(255) ENCODE LZO,
    invoice_date VARCHAR(255) ENCODE LZO,
    amount_due NUMERIC(22,6) ENCODE AZ64,
    amount_paid NUMERIC(22,6) ENCODE AZ64,
    amount_tax NUMERIC(22,6) ENCODE AZ64,
    tax_rate NUMERIC(22,6) ENCODE AZ64,
    pending_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    failed_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    attempts_to_pay BIGINT ENCODE AZ64,
    failed_reason VARCHAR(16384) ENCODE LZO,
    refund_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    refund_amount NUMERIC(22,6) ENCODE AZ64,
    country_name VARCHAR(20) ENCODE LZO
)
DISTSTYLE AUTO
DISTKEY (payment_id);

