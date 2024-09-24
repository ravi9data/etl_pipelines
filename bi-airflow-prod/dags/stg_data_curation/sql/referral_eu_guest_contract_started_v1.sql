INSERT INTO stg_curated.referral_eu_guest_contract_started_v1(
SELECT
	customer_id,
	default_store_code,
	order_id,
	transaction_id,
	consumed_at::TIMESTAMP AS consumed_at,
	cash_amount_in_cents,
	published_at,
	kafka_received_at,
	created_at
	FROM s3_spectrum_kafka_topics_raw.referral_eu_guest_contract_started_v1
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.referral_eu_guest_contract_started_v1)
);
