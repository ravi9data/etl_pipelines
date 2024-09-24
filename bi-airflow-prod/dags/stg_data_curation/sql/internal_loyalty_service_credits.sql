INSERT INTO stg_curated.internal_loyalty_service_credits(
SELECT
	payload,
	event_name,
	version,
	consumed_at::TIMESTAMP AS consumed_at
	FROM s3_spectrum_kafka_topics_raw.internal_loyalty_service_credits
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.internal_loyalty_service_credits)
);
