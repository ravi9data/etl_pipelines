INSERT INTO stg_curated.internal_loyalty_service_transactions_v1(
SELECT
	payload,
	event_name,
	version,
	consumed_at::TIMESTAMP AS consumed_at,
	kafka_received_at
	FROM s3_spectrum_kafka_topics_raw.internal_loyalty_service_transactions_v1
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.internal_loyalty_service_transactions_v1)
);
