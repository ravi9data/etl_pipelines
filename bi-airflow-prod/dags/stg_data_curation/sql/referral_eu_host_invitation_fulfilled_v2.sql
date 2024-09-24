INSERT INTO stg_curated.referral_eu_host_invitation_fulfilled_v2(
SELECT
	customer_id,
	default_store_code,
	order_id,
	guest_id,
	transaction_id,
	consumed_at::TIMESTAMP AS consumed_at,
	cash_amount_in_cents,
	published_at,
	kafka_received_at,
	created_at,
	email
	FROM s3_spectrum_kafka_topics_raw.referral_eu_host_invitation_fulfilled_v2
WHERE consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.referral_eu_host_invitation_fulfilled_v2)
);
