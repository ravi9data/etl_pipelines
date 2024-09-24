INSERT INTO stg_curated.legacy_grover_care_order_submitted_v1
(SELECT * FROM (
WITH numbers1 AS
	(
  	SELECT
		*
	FROM numbers
  	WHERE ordinal < 20
	)
SELECT
store_code,
payment_method_id,
user_id,
currency,
order_id,
kafka_received_at::timestamp as kafka_received_at,
consumed_at::timestamp AS created_date,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT,TRUE), 'grover_care_coverage') AS grover_care_coverage,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT,TRUE), 'grover_care_price_in_cents') AS grover_care_price_in_cents,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT,TRUE), 'grover_care_set_by_customer') AS grover_care_set_by_customer,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT,TRUE), 'variant_sku') AS variant_sku,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT,TRUE), 'rental_plan_length') AS rental_plan_length,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT,TRUE), 'product_sku') AS product_sku,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT,TRUE), 'quantity') AS quantity,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(line_items, numbers.ordinal::INT,TRUE), 'deductible_fees') AS deductible_fees
FROM s3_spectrum_kafka_topics_raw.legacy_grover_care_order_submitted_v1 t1
CROSS JOIN numbers
WHERE cast((year||'-'||"month"||'-'||"day"||' 00:00:00') as timestamp) > current_date::date-3
AND numbers.ordinal < json_array_length(t1.line_items, TRUE)
AND created_date > (SELECT max(created_date) FROM stg_curated.legacy_grover_care_order_submitted_v1)));
