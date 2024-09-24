INSERT INTO stg_curated.b2b_eu_dashboard_access_activated_v2

SELECT
updated_at::TIMESTAMP as updated_at,
activated_by,
activated_at::TIMESTAMP as activated_at,
company_id,
kafka_received_at::TIMESTAMP as kafka_received_at,
consumed_at::TIMESTAMP as consumed_at,
uuid
FROM s3_spectrum_kafka_topics_raw.b2b_eu_dashboard_access_activated_v2 t1
WHERE consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.b2b_eu_dashboard_access_activated_v2)
