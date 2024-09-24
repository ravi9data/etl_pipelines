DROP TABLE IF EXISTS subscriptions_discount_requested;

CREATE TEMP TABLE subscriptions_discount_requested AS
SELECT
	JSON_EXTRACT_PATH_text(payload, 'subscription_id') AS subscription_id,
	JSON_EXTRACT_PATH_text(payload, 'user_id') AS user_id,
	JSON_EXTRACT_PATH_text(payload, 'discount_type') AS discount_type,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'old_price'), 'in_cents') AS old_price_in_cents,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'new_price'), 'in_cents') AS new_price_in_cents,
	JSON_EXTRACT_PATH_text(payload, 'discount_percentage') AS discount_percentage,
	CASE WHEN LENGTH(JSON_EXTRACT_PATH_text(payload, 'started_at'))<1 THEN NULL ELSE JSON_EXTRACT_PATH_text(payload, 'started_at')::timestamp END started_at,
	kafka_received_at::timestamp
FROM s3_spectrum_kafka_topics_raw.subscriptions s
WHERE event_name ='subscription_discount_requested'
AND kafka_received_at::timestamp>(SELECT max(kafka_received_at::timestamp) FROM stg_curated.subscriptions_discount_requested)
QUALIFY ROW_NUMBER() OVER (PARTITION BY subscription_id, payload ORDER BY consumed_at DESC) = 1;

INSERT INTO stg_curated.subscriptions_discount_requested
SELECT *
FROM subscriptions_discount_requested;