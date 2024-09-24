CREATE TABLE IF NOT EXISTS data_science_dev.payment_risk_categories (customer_id BIGINT,
                                                                     order_id VARCHAR(128),
                                                                     payment_category_fraud_rate VARCHAR,
                                                                     payment_category_info VARCHAR,
                                                                     created_at TIMESTAMP WITHOUT TIME ZONE);
