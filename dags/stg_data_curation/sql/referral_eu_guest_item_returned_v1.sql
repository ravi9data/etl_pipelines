INSERT INTO stg_curated.referral_eu_guest_item_returned_v1

SELECT
code_owner_id,
TIMESTAMP 'epoch' + CAST(published_at AS NUMERIC) / 1000 * INTERVAL '1 second' AS published_at,
default_store_code,
consumed_at::TIMESTAMP as activated_at,
kafka_received_at::TIMESTAMP as kafka_received_at,
TIMESTAMP 'epoch' + CAST(created_at AS NUMERIC) / 1000 * INTERVAL '1 second' AS created_at,
transaction_id,
order_id,
customer_id
FROM s3_spectrum_kafka_topics_raw.referral_eu_guest_item_returned_v1 t1 
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.referral_eu_guest_item_returned_v1)
