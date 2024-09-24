-- PM DELETED

DELETE
FROM
	dm_payments.eu_payment_methods_method_deleted_v1
WHERE
	partition_col >= current_date-1 ;

INSERT
	INTO
	dm_payments.eu_payment_methods_method_deleted_v1
SELECT
	customer_id::integer,
	store_country_code,
	created_at,
	published_at,
	consumed_at,
	"type",
	bankcard_brand,
	bankcard_type,
	bankcard_level,
	bankcard_country_code,
	bankcard_bank_name,
	ixopay_connector_name,
	ixopay_reference_id,
	ixopay_transaction_id,
	id,
	tracing_id,
	added_at,
	'delete' AS event_type,
	CAST(("year" || '-' || "month" || '-' || "day") AS date) AS partition_col
FROM
	s3_spectrum_kafka_topics_raw.payments_eu_payment_methods_method_deleted_v1
WHERE
	CAST(("year" || '-' || "month" || '-' || "day") AS date) > current_date::date-2;


-- PM ADDITION FAILED
DELETE
FROM
	dm_payments.eu_payment_methods_method_addition_failed_v1
WHERE
	partition_col >= current_date-1 ;

INSERT
	INTO
	dm_payments.eu_payment_methods_method_addition_failed_v1
SELECT
	customer_id::integer,
	order_id,
	store_country_code,
	created_at,
	published_at,
	consumed_at,
	"type",
	bankcard_brand,
	bankcard_type,
	bankcard_level,
	bankcard_country_code,
	bankcard_bank_name,
	ixopay_connector_name,
	ixopay_reference_id,
	ixopay_transaction_id,
	processor_error_code,
	processor_error_message,
	gateway_error_code,
	gateway_error_message,
	id,
	tracing_id,
	customer_email_verified,
	preauthorization_amount_in_cents,
	preauthorization_amount_currency,
	with_preauthorization,
	'addition_failed' AS event_type,
	CAST(("year" || '-' || "month" || '-' || "day") AS date) AS partition_col
FROM
	s3_spectrum_kafka_topics_raw.payments_eu_payment_methods_method_addition_failed_v1
WHERE
	CAST(("year" || '-' || "month" || '-' || "day") AS date) > current_date::date-2;


--PM ADDED

DELETE
FROM
	dm_payments.eu_payment_methods_method_added_v1
WHERE
	partition_col >= current_date-1 ;

INSERT
	INTO
	dm_payments.eu_payment_methods_method_added_v1
SELECT
	customer_id::integer,
	order_id,
	store_country_code,
	created_at,
	published_at,
	consumed_at,
	"type",
	bankcard_brand,
	bankcard_type,
	bankcard_level,
	bankcard_country_code,
	ixopay_connector_name,
	ixopay_reference_id,
	ixopay_transaction_id,
	tracing_id,
	added_at,
	preauthorization_amount_in_cents,
	preauthorization_amount_currency,
	with_preauthorization,
	'add' AS event_type,
	CAST(("year" || '-' || "month" || '-' || "day") AS date) AS partition_col
FROM
	s3_spectrum_kafka_topics_raw.payments_eu_payment_methods_method_added_v1
WHERE
	CAST(("year" || '-' || "month" || '-' || "day") AS date) > current_date::date-2;
