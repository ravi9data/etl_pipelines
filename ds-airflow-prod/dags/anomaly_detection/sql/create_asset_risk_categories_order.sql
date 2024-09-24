CREATE TABLE IF NOT EXISTS data_science_dev.asset_risk_categories_order (subscription_id BIGINT ,order_id VARCHAR(128),
                                                                         customer_id BIGINT,
                                                                         asset_risk_fraud_rate VARCHAR,
                                                                         asset_risk_info VARCHAR,
                                                                         numeric_fraud_rate float,
                                                                         created_at TIMESTAMP WITHOUT TIME ZONE);
