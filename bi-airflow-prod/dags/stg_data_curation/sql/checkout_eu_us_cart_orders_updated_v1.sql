-- extracting data for last two days in temp tables by avoid joining specturm tables in query

DROP TABLE IF EXISTS checkout_us_cart_updated_v1;
CREATE TEMP TABLE checkout_us_cart_updated_v1 AS 
SELECT * FROM s3_spectrum_kafka_topics_raw.checkout_us_cart_updated_v1 
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at::DATE  >= current_date-2;

DROP TABLE IF EXISTS checkout_us_cart_created_v1;
CREATE TEMP TABLE checkout_us_cart_created_v1 AS 
SELECT * 
FROM s3_spectrum_kafka_topics_raw.checkout_us_cart_created_v1 
WHERE  CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at::DATE >= CURRENT_DATE-2;

DROP TABLE IF EXISTS checkout_eu_cart_updated_v1;
CREATE TEMP TABLE checkout_eu_cart_updated_v1 AS 
SELECT * 
FROM s3_spectrum_kafka_topics_raw.checkout_eu_cart_updated_v1
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at::DATE  >= current_date-2;

DROP TABLE IF EXISTS checkout_eu_cart_created_v1;
CREATE TEMP TABLE checkout_eu_cart_created_v1 AS 
SELECT * 
FROM  s3_spectrum_kafka_topics_raw.checkout_eu_cart_created_v1 cr
where CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at::DATE  >= current_date-2;


-- INCREMENTAL
DROP TABLE IF EXISTS tmp_cart_orders_incremental;
CREATE TEMP TABLE tmp_cart_orders_incremental
SORTKEY(order_id)
DISTKEY(order_id)
AS
WITH order_info_eu_and_us AS (
	SELECT DISTINCT
		COALESCE(up.order_id, cr.order_id) AS order_id,
		COALESCE(FIRST_VALUE(CASE WHEN up.customer_ip_address IN ('null','undefined') THEN NULL
					ELSE up.customer_ip_address END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.customer_ip_address IN ('null','undefined') THEN NULL
				 	ELSE cr.customer_ip_address END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS customer_ip_address,
		COALESCE(FIRST_VALUE(CASE WHEN up.user_id = 'null' THEN NULL ELSE up.user_id END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.customer_id = 'null' THEN NULL ELSE cr.customer_id END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS customer_id,
		COALESCE(FIRST_VALUE(CASE WHEN up.store_code = 'null' THEN NULL ELSE up.store_code END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.store_code = 'null' THEN NULL ELSE cr.store_code END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS store_code,
		COALESCE(FIRST_VALUE(cr.created_at) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.created_at ASC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(up.consumed_at) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at ASC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS created_date,
		COALESCE(FIRST_VALUE(up.consumed_at) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(cr.consumed_at) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS updated_date,
		COALESCE(FIRST_VALUE(CASE WHEN up.items IN ('null', '[]') THEN NULL ELSE up.items END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.items IN ('null', '[]') THEN NULL ELSE cr.items END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS items,
		COALESCE(FIRST_VALUE(CASE WHEN up.total = 'null' THEN NULL ELSE up.total END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.total = 'null' THEN NULL ELSE cr.total END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS order_value,
		COALESCE(FIRST_VALUE(CASE WHEN up.net_total = 'null' THEN NULL ELSE up.net_total END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.total = 'null' THEN NULL ELSE cr.total END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS basket_size,
		COALESCE(FIRST_VALUE(up.state) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 'cart')
				 AS status,
		FIRST_VALUE(CASE WHEN up.shipping_address_id = 'null' THEN NULL ELSE up.shipping_address_id END)
				IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				 AS shipping_address_id,
		FIRST_VALUE(CASE WHEN up.billing_address_id = 'null' THEN NULL ELSE up.billing_address_id END)
				IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				 AS billing_address_id,
		FIRST_VALUE(CASE WHEN up.payment_method_id = 'null' THEN NULL ELSE up.payment_method_id END)
				IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				 AS payment_method_id
		FROM checkout_us_cart_updated_v1 up
		FULL JOIN checkout_us_cart_created_v1 cr
			ON up.order_id = cr.order_id
		--
	UNION
		--
	SELECT DISTINCT
		COALESCE(up.order_id, cr.order_id) AS order_id,
		COALESCE(FIRST_VALUE(CASE WHEN up.customer_ip_address IN ('null','undefined') THEN NULL
					ELSE up.customer_ip_address END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.customer_ip_address IN ('null','undefined') THEN NULL
				 	ELSE cr.customer_ip_address END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS customer_ip_address,
		COALESCE(FIRST_VALUE(CASE WHEN up.user_id = 'null' THEN NULL ELSE up.user_id END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.customer_id = 'null' THEN NULL ELSE cr.customer_id END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS customer_id,
		COALESCE(FIRST_VALUE(CASE WHEN up.store_code = 'null' THEN NULL ELSE up.store_code END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.store_code = 'null' THEN NULL ELSE cr.store_code END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS store_code,
		COALESCE(FIRST_VALUE(cr.created_at) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.created_at ASC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(up.consumed_at) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at ASC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS created_date,
		COALESCE(FIRST_VALUE(up.consumed_at) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(cr.consumed_at) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS updated_date,
		COALESCE(FIRST_VALUE(CASE WHEN up.items IN ('null', '[]') THEN NULL ELSE up.items END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.items IN ('null', '[]') THEN NULL ELSE cr.items END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS items,
		COALESCE(FIRST_VALUE(CASE WHEN up.total = 'null' THEN NULL ELSE up.total END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.total = 'null' THEN NULL ELSE cr.total END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS order_value,
		COALESCE(FIRST_VALUE(CASE WHEN up.net_total = 'null' THEN NULL ELSE up.net_total END) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 FIRST_VALUE(CASE WHEN cr.total = 'null' THEN NULL ELSE cr.total END) IGNORE NULLS OVER
					(PARTITION BY cr.order_id ORDER BY cr.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
				 AS basket_size,
		COALESCE(FIRST_VALUE(up.state) IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
				 'cart')
				 AS status,
		FIRST_VALUE(CASE WHEN up.shipping_address_id = 'null' THEN NULL ELSE up.shipping_address_id END)
				IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				 AS shipping_address_id,
		FIRST_VALUE(CASE WHEN up.billing_address_id = 'null' THEN NULL ELSE up.billing_address_id END)
				IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				 AS billing_address_id,
		FIRST_VALUE(CASE WHEN up.payment_method_id = 'null' THEN NULL ELSE up.payment_method_id END)
				IGNORE NULLS OVER
					(PARTITION BY up.order_id ORDER BY up.consumed_at DESC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
				 AS payment_method_id
		FROM checkout_eu_cart_updated_v1 up
		FULL JOIN checkout_eu_cart_created_v1 cr
			ON up.order_id = cr.order_id
)
, one_row_per_order_incremental AS (
	SELECT DISTINCT
		order_id,
		customer_ip_address,
		customer_id,
		store_code,
		FIRST_VALUE(created_date) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY created_date ASC
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS created_date,
		updated_date,
		items,
		order_value,
		basket_size,
		status,
		shipping_address_id,
		billing_address_id,
		payment_method_id,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_date DESC) AS rowno
	FROM order_info_eu_and_us
)
, one_row_per_order_with_old_info AS (
	SELECT DISTINCT
		oo.order_id,
		CASE WHEN stg.updated_date IS NULL THEN oo.customer_ip_address
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.customer_ip_address, stg.customer_ip_address)
			 ELSE stg.customer_ip_address END AS customer_ip_address,
		CASE WHEN stg.updated_date IS NULL THEN oo.customer_id
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.customer_id, stg.customer_id)
			 ELSE stg.customer_id END AS customer_id,
		CASE WHEN stg.updated_date IS NULL THEN oo.store_code
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.store_code, stg.store_code)
			 ELSE stg.store_code END AS store_code,
		CASE WHEN oo.created_date > stg.created_date THEN stg.created_date
			 ELSE oo.created_date END AS created_date,
		CASE WHEN stg.updated_date IS NULL THEN oo.updated_date
			 WHEN oo.updated_date > stg.updated_date THEN oo.updated_date
			 ELSE stg.updated_date END AS updated_date,
		CASE WHEN stg.updated_date IS NULL THEN oo.items
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.items, stg.items)
			 ELSE stg.items END AS items,
		CASE WHEN stg.updated_date IS NULL THEN oo.order_value
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.order_value, stg.order_value)
			 ELSE stg.order_value END AS order_value,
		CASE WHEN stg.updated_date IS NULL THEN oo.basket_size
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.basket_size, stg.basket_size)
			 ELSE stg.basket_size END AS basket_size,
		CASE WHEN stg.updated_date IS NULL THEN oo.status
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.status, stg.status)
			 ELSE stg.status END AS status,
		CASE WHEN stg.updated_date IS NULL THEN oo.shipping_address_id
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.shipping_address_id, stg.shipping_address_id)
			 ELSE stg.shipping_address_id END AS shipping_address_id,
		CASE WHEN stg.updated_date IS NULL THEN oo.billing_address_id
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.billing_address_id, stg.billing_address_id)
			 ELSE stg.billing_address_id END AS billing_address_id,
		CASE WHEN stg.updated_date IS NULL THEN oo.payment_method_id
			 WHEN oo.updated_date > stg.updated_date THEN COALESCE(oo.payment_method_id, stg.payment_method_id)
			 ELSE stg.payment_method_id END AS payment_method_id
	FROM one_row_per_order_incremental oo
	LEFT JOIN stg_curated.checkout_eu_us_cart_orders_updated_v1 stg
		ON oo.order_id = stg.order_id
	WHERE oo.rowno = 1
)
, numbers AS (
  	SELECT
		*
	FROM numbers
  	WHERE ordinal < 20
)
SELECT
	o.order_id,
	o.customer_ip_address,
	split_part(o.customer_id , '.', 1) AS customer_id,
	o.store_code,
	o.created_date,
	o.updated_date,
	o.items,
	o.order_value,
	o.basket_size,
	o.status,
	o.shipping_address_id,
	o.billing_address_id,
	o.payment_method_id,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'product_sku') AS product_sku,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'variant_id') AS variant_id,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'variant_sku') AS variant_sku,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'name') AS product_name,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'quantity') AS quantity,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'price')::float/100 AS price,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'discount')::float/100 AS discount,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'total_amount')::float/100 AS total_amount,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'currency') AS currency,
	JSON_EXTRACT_PATH_text(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(items, numbers.ordinal::INT,TRUE), 'committed_months') AS committed_months
FROM one_row_per_order_with_old_info o
CROSS JOIN numbers
WHERE numbers.ordinal < json_array_length(o.items, TRUE)
;


BEGIN TRANSACTION;

DELETE FROM stg_curated.checkout_eu_us_cart_orders_updated_v1
USING tmp_cart_orders_incremental tmp
WHERE checkout_eu_us_cart_orders_updated_v1.order_id = tmp.order_id;

INSERT INTO stg_curated.checkout_eu_us_cart_orders_updated_v1
SELECT *
FROM tmp_cart_orders_incremental;

END TRANSACTION;

