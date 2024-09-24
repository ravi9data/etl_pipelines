CREATE TABLE IF NOT EXISTS data_science_dev.nethone_signal_risk_categories_order (
    created_at timestamp DEFAULT sysdate,
    customer_id INTEGER NOT NULL,
    order_id VARCHAR NOT NULL,
    signals varchar NULL,
    bad_ratio float4 NULL,
    signal_risk_category varchar NULL
);
