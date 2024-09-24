DROP TABLE IF EXISTS checkout_addons_submitted_v1;

CREATE TEMP TABLE checkout_addons_submitted_v1 AS
SELECT
	customer_id
	,tracing_id
	,order_id
	,JSON_EXTRACT_ARRAY_ELEMENT_TEXT(addons,0) AS _addon
	,JSON_EXTRACT_PATH_TEXT(_addon,'addon_id') AS addon_id
	,store_code
	,JSON_EXTRACT_PATH_TEXT(_addon, 'variant_id') AS variant_id
	,JSON_EXTRACT_PATH_TEXT(_addon, 'related_product_sku') AS related_product_sku
	,JSON_EXTRACT_PATH_TEXT(_addon, 'related_variant_sku') AS related_variant_sku
    ,created_at::TIMESTAMP AS event_timestamp
    ,consumed_at::TIMESTAMP as consumed_dt
    ,kafka_received_at::TIMESTAMP
    ,published_at::TIMESTAMP as published_at
FROM s3_spectrum_kafka_topics_raw.checkout_addons_submitted_v1
WHERE kafka_received_at::timestamp > (SELECT max(kafka_received_at) FROM staging.checkout_addons_submitted_v1);


INSERT INTO staging.checkout_addons_submitted_v1
SELECT *
FROM checkout_addons_submitted_v1;


--each order_id can have only one final verification_state
--staging.checkout_addons_submitted_v1 contains the latest verification_state for each order_id
TRUNCATE TABLE stg_curated.checkout_addons_submitted_v1;

INSERT INTO stg_curated.checkout_addons_submitted_v1
SELECT *
FROM staging.checkout_addons_submitted_v1
WHERE 1=1
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY kafka_received_at DESC) = 1;
