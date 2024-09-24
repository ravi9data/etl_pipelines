DELETE FROM stg_curated.checkout_eu_us_cart_orders_v1
WHERE created_date >= CURRENT_DATE-2;

INSERT INTO stg_curated.checkout_eu_us_cart_orders_v1
SELECT * FROM (
WITH numbers AS
	(
  	SELECT
		*
	FROM numbers
  	WHERE ordinal < 20
	)

SELECT
store_code,
order_id,
consumed_at AS created_date,
user_id AS customer_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'name') AS product_name,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'product_sku') AS product_sku,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'quantity') AS quantity,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'category_id') AS category_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'committed_months') AS committed_months,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'price') AS price
FROM s3_spectrum_kafka_topics_raw.checkout_us_cart_created_v1 t1
CROSS JOIN numbers
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND numbers.ordinal < json_array_length(t1.items, TRUE)
AND created_date::DATE >= CURRENT_DATE-2

UNION ALL

SELECT
store_code,
order_id,
consumed_at AS created_date,
user_id AS customer_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'name') AS product_name,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'product_sku') AS product_sku,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'quantity') AS quantity,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'category_id') AS category_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'committed_months') AS committed_months,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'price') AS price
FROM s3_spectrum_kafka_topics_raw.checkout_us_cart_updated_v1 t2
CROSS JOIN numbers
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND numbers.ordinal < json_array_length(t2.items, TRUE)
AND created_date::DATE >= CURRENT_DATE-2

UNION ALL

SELECT
store_code,
order_id,
consumed_at AS created_date,
user_id AS customer_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'name') AS product_name,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'product_sku') AS product_sku,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'quantity') AS quantity,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'category_id') AS category_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'committed_months') AS committed_months,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'price') AS price
FROM s3_spectrum_kafka_topics_raw.checkout_eu_cart_created_v1 t3
CROSS JOIN numbers
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND numbers.ordinal < json_array_length(t3.items, TRUE)
AND created_date::DATE >= CURRENT_DATE-2

UNION ALL

SELECT
store_code,
order_id,
consumed_at AS created_date,
user_id AS customer_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'name') AS product_name,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'product_sku') AS product_sku,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'quantity') AS quantity,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'category_id') AS category_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'committed_months') AS committed_months,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'price') AS price
FROM s3_spectrum_kafka_topics_raw.checkout_eu_cart_updated_v1 t4
CROSS JOIN numbers
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND numbers.ordinal < json_array_length(t4.items, TRUE)
AND created_date::DATE >= CURRENT_DATE-2
)
