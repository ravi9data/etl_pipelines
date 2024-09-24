
    subscription_id VARCHAR(255) NOT NULL ENCODE LZO PRIMARY KEY,
    order_id VARCHAR(255) ENCODE LZO,
    customer_id VARCHAR(255) ENCODE LZO,
    subscription_start_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    order_submitted_date TIMESTAMP WITH TIME ZONE ENCODE AZ64,
    rank_subscriptions BIGINT ENCODE AZ64,
    subscriptions_per_customer BIGINT ENCODE AZ64,
    subscription_status VARCHAR(255) ENCODE LZO,
    subscription_value DOUBLE PRECISION ENCODE RAW,
    country VARCHAR(255) ENCODE LZO,
    customer_type VARCHAR(255) ENCODE LZO,
    variant_sku VARCHAR(255) ENCODE LZO,
    first_asset_delivery_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    subscription_plan VARCHAR(255) ENCODE LZO,
    rental_period DOUBLE PRECISION ENCODE RAW,
    committed_sub_value DOUBLE PRECISION ENCODE RAW,
    new_recurring VARCHAR(10) ENCODE LZO,
    minimum_cancellation_date TIMESTAMP WITHOUT TIME ZONE ENCODE AZ64,
    subscription_bo_id VARCHAR(255) ENCODE LZO
)
DISTSTYLE AUTO;

