DROP TABLE IF EXISTS risk_eu_id_verification_order;

CREATE TEMP TABLE risk_eu_id_verification_order AS
SELECT
	customer_id::int8,
	order_id::varchar,
	is_processed::text,
	"trigger"::text,
	verification_state::text,
	created_at::timestamp,
	kafka_received_at::timestamp,
	updated_at::timestamp,
	consumed_at::timestamp
FROM s3_spectrum_kafka_topics_raw.risk_eu_id_verification_order_v1
WHERE kafka_received_at::timestamp > (SELECT max(kafka_received_at) FROM stg_curated.risk_eu_id_verification_order);


INSERT INTO stg_curated.risk_eu_id_verification_order
SELECT *
FROM risk_eu_id_verification_order;


--each order_id can have only one final verification_state
--staging.risk_eu_id_verification_order contains the latest verification_state for each order_id
TRUNCATE TABLE staging.risk_eu_id_verification_order;

INSERT INTO staging.risk_eu_id_verification_order
SELECT *
FROM stg_curated.risk_eu_id_verification_order
WHERE 1=1
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY kafka_received_at DESC) = 1;