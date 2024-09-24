INSERT INTO stg_curated.referral_eu_code_used_v1

SELECT
customer_id,
transaction_id,
campaign_id,
consumed_at::TIMESTAMP as consumed_at,
TIMESTAMP 'epoch' + CAST(published_at AS NUMERIC) / 1000 * INTERVAL '1 second' AS published_at,
code_owner_id,
kafka_received_at::TIMESTAMP as kafka_received_at,
TIMESTAMP 'epoch' + CAST(created_at AS NUMERIC) / 1000 * INTERVAL '1 second' AS created_at,
default_store_code
FROM s3_spectrum_kafka_topics_raw.referral_eu_code_used_v1
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.referral_eu_code_used_v1)
