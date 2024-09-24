CREATE TABLE IF NOT EXISTS data_science_dev.nethone_signal_customer_history
(
    created_at TIMESTAMP DEFAULT sysdate,
    customer_id INTEGER NOT NULL,
    order_id VARCHAR NOT NULL,
    order_submission TIMESTAMP,
    bad_ratio float4 NULL,
    worst_signal VARCHAR
);
