INSERT INTO staging.kafka_order_placed_v2
SELECT * FROM (
WITH numbers as
	(
  	SELECT
		*
	FROM public.numbers
  	WHERE ordinal < 20)
SELECT
DISTINCT
kafka_received_at::timestamp event_timestamp
,event_name
,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'total'),'in_cents') as total_in_cents
,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'total'),'currency') as total_currency
,JSON_EXTRACT_PATH_text(payload,'published_at') as published_at
,JSON_EXTRACT_PATH_text(payload,'order_number') as order_number
,JSON_EXTRACT_PATH_text(payload,'order_mode') as order_mode
,JSON_EXTRACT_PATH_text(payload,'order_state') as order_state
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'store'),'store_code') as store_store_code
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'store'),'store_id') as store_store_id
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'store'),'store_type') as store_store_type
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'store'),'country_id') as store_country_id
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'store'),'country_code') as store_country_code
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'user'),'user_id') as user_user_id
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'user'),'firstname') as user_firstname
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'user'),'lastname') as user_lastname
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'user'),'birthdate') as user_birthdate
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'user'),'user_type') as user_type
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'user'),'email') as user_email
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'user'),'phone') as user_phone
,JSON_EXTRACT_PATH_text (JSON_EXTRACT_PATH_text(payload,'user'),'language') as user_language
,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'address1') billing_address_address1
,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'address2') billing_address_address2
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'city') billing_address_city
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'country') billing_address_country
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'country_iso') billing_address_country_iso
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'zipcode') billing_address_zipcode
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'billing_address'), 'additional_info') billing_address_additional_info
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'address1') shipping_address_address1
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'address2') shipping_address_address2
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'city') shipping_address_city
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'country') shipping_address_country
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'country_iso') shipping_address_country_iso
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'zipcode') shipping_address_zipcode
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_address'), 'additional_info') shipping_address_additional_info
	,NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'code'),'') as adjustment_voucher_code
	,NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount'),'type'),'') as adjustment_voucher_discount_type
	,NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount'),'effect'), '') as adjustment_voucher_discount_effect
	,NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount'),'percent_off'),'') as adjustment_voucher_discount_percent_off
	,NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount_amount'),'in_cents'),'') as adjustment_voucher_discount_amount_in_cents
	,NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'discount_amount'),'currency'),'') as adjustment_voucher_discount_amount_currency
	,NULLIF(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'adjustment'),'voucher'), 'metadata'),'recurring'),'') as adjustment_voucher_metadata_recurring
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'payment_method'), 'billing_account_id') payment_billing_account_id
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'payment_method'), 'source_id') payment_source_id
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'payment_method'), 'source_type') payment_source_type
	,JSON_EXTRACT_PATH_text(payload, 'line_items') AS line_items
	,json_extract_array_element_text(line_items,numbers.ordinal::int, true) as line_items_json
	,JSON_EXTRACT_PATH_text(line_items_json,'line_item_id') order_item_id
	,case when order_mode  = 'MIX' and order_item_id = 0  then 1 else JSON_ARRAY_LENGTH(line_items,true) end as total_order_items
	,JSON_EXTRACT_PATH_text(line_items_json,'rental_mode') order_item_rental_mode
	,JSON_EXTRACT_PATH_text(line_items_json,'quantity') order_item_quantity
	,JSON_EXTRACT_PATH_text(line_items_json,'committed_months') order_item_committed_months
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'item_discount'), 'in_cents') item_discount_in_cents
    ,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'item_discount'), 'currency') item_discount_currency
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'item_price'), 'in_cents') item_price_in_cents
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'item_price'), 'currency') item_price_currency
    ,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'item_total'), 'in_cents') item_total_in_cents
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'item_total'), 'currency') item_total_currency
	,JSON_EXTRACT_PATH_text(line_items_json,'purchase_option_allowed') purchase_option_allowed
	,JSON_EXTRACT_PATH_text(line_items_json,'plan_upgrade_allowed') plan_upgrade_allowed
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'variant'),'variant_id')  variant_variant_id
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'variant'),'variant_sku') variant_variant_sku
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'variant'),'name')  variant_name
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(line_items_json,'variant'),'product_sku') variant_product_sku
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_cost'),'in_cents')  shipping_cost_in_cents
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'shipping_cost'),'currency') shipping_cost_currency
	,JSON_EXTRACT_PATH_text(payload,'created_at') created_at
	,JSON_EXTRACT_PATH_text(payload,'completed_at') completed_at
	,JSON_EXTRACT_PATH_text(payload,'fraud_check_consent') fraud_check_consent
	,JSON_EXTRACT_PATH_text(line_items_json,'grover_care') AS grover_care
	,JSON_EXTRACT_PATH_text(grover_care,'coverage') AS grover_care_coverage
	,JSON_EXTRACT_PATH_text(grover_care,'set_by_customer') AS grover_care_set_by_customer
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(grover_care,'price'),'in_cents') AS grover_care_price
	,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(grover_care,'price'),'currency') AS grover_care_currency
	,JSON_EXTRACT_PATH_text(grover_care, 'deductible_fees') AS deductible_fees
FROM s3_spectrum_kafka_topics_raw.internal_order_placed
CROSS JOIN numbers
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND numbers.ordinal < total_order_items
AND event_timestamp > (SELECT MAX(event_timestamp) FROM staging.kafka_order_placed_v2)
AND is_Valid_json(payload))
