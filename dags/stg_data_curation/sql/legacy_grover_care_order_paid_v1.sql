INSERT INTO stg_curated.legacy_grover_care_order_paid_v1
SELECT * FROM (
WITH numbers1 AS
	(
  	SELECT
		*
	FROM numbers
  	WHERE ordinal < 20
	)
SELECT
user_id,
order_id,
kafka_received_at::timestamp as kafka_received_at,
consumed_at::timestamp AS created_date,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(subscriptions, numbers.ordinal::INT,TRUE), 'variant_sku') AS variant_sku,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(subscriptions, numbers.ordinal::INT,TRUE), 'rental_plan_length') AS rental_plan_length,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(subscriptions, numbers.ordinal::INT,TRUE), 'salesforce_subscription_id') AS salesforce_subscription_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(subscriptions, numbers.ordinal::INT,TRUE), 'subscription_id') AS subscription_id
FROM s3_spectrum_kafka_topics_raw.legacy_grover_care_order_paid_v1 t1
CROSS JOIN numbers
WHERE numbers.ordinal < json_array_length(t1.subscriptions, TRUE)
AND created_date > (SELECT MAX(created_date) FROM stg_curated.legacy_grover_care_order_paid_v1))
