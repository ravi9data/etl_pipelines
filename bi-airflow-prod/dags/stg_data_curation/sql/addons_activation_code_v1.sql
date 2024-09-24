DELETE FROM stg_curated.addons_addons_activation_code_v1
WHERE event_timestamp >= CURRENT_DATE-2;

INSERT INTO stg_curated.addons_addons_activation_code_v1
SELECT
    DISTINCT
	addon_name
	,user_id AS customer_id
	,activation_code
	,created_at AS event_timestamp
FROM
	s3_spectrum_kafka_topics_raw.addons_addons_activation_code_v1 a
WHERE CAST((a.year||'-'||a."month"||'-'||a."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND created_at >= CURRENT_DATE -2;
