CREATE TABLE IF NOT EXISTS data_science_dev.nethone_signal_risk_categories (
    created_at timestamp DEFAULT sysdate,
    signals varchar NULL,
    bad_ratio float4 NULL,
    signal_risk_category varchar NULL
);
