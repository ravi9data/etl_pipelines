DELETE FROM stg_curated.checkout_eu_addons_paid_v1
WHERE event_timestamp >= CURRENT_DATE-2;

INSERT INTO stg_curated.checkout_eu_addons_paid_v1
SELECT
    DISTINCT
	customer_id
	,order_id
	,tracing_id
	,store_code
	,created_at AS event_timestamp
FROM
	s3_spectrum_kafka_topics_raw.checkout_eu_addons_paid_v1 p
WHERE CAST((p.year||'-'||p."month"||'-'||p."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND created_at >= CURRENT_DATE -2;
