INSERT INTO stg_curated.grover_card_seizures(
SELECT
	payload,
	event_name,
	version,
	consumed_at::TIMESTAMP AS consumed_at,
	kafka_received_at
	FROM s3_spectrum_kafka_topics_raw.grover_card_seizures
WHERE consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.grover_card_seizures)
);
