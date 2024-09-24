delete from staging.fulfillment_eu_shipment_update_v1
where published_at >= current_date -2;

insert into staging.fulfillment_eu_shipment_update_v1
with numbers_ as(
	select * from public.numbers
	where ordinal < 50)
select
uid,
published_at,
shipping_profile,
carrier,
tracking_number,
JSON_EXTRACT_PATH_text(latest_lifecycle_event, 'name', true) as latest_lifecycle_event_name,
JSON_EXTRACT_PATH_text(latest_lifecycle_event, 'location', true) as latest_lifecycle_event_location,
JSON_EXTRACT_PATH_text(latest_lifecycle_event, 'description', true) as latest_lifecycle_event_description,
JSON_EXTRACT_PATH_text(latest_lifecycle_event, 'timestamp', true) as latest_lifecycle_event_timestamp,
JSON_EXTRACT_PATH_text(latest_lifecycle_event, 'carrier', true) as latest_lifecycle_event_carrier,
JSON_EXTRACT_PATH_text(customer, 'user_id', true) as customer_user_id,
JSON_EXTRACT_PATH_text(customer, 'firstname', true) as customer_firstname,
JSON_EXTRACT_PATH_text(customer, 'lastname', true) as customer_lastname,
JSON_EXTRACT_PATH_text(customer, 'company', true) as customer_company,
JSON_EXTRACT_PATH_text(customer, 'birthdate', true) as customer_birthdate,
JSON_EXTRACT_PATH_text(customer, 'type', true) as customer_type,
JSON_EXTRACT_PATH_text(customer, 'email', true) as customer_email,
JSON_EXTRACT_PATH_text(customer, 'phone', true) as customer_phone,
JSON_EXTRACT_PATH_text(customer, 'language', true) as customer_language,
JSON_EXTRACT_PATH_text(sender_address, 'country_iso', true) as sender_address_country_iso,
JSON_EXTRACT_PATH_text(sender_address, 'country', true) as sender_address_country,
JSON_EXTRACT_PATH_text(sender_address, 'city', true) as sender_address_city,
JSON_EXTRACT_PATH_text(sender_address, 'state', true) as sender_address_state,
JSON_EXTRACT_PATH_text(sender_address, 'zipcode', true) as sender_address_zipcode,
JSON_EXTRACT_PATH_text(sender_address, 'address1', true) as sender_address_address1,
JSON_EXTRACT_PATH_text(sender_address, 'address2', true) as sender_address_address2,
JSON_EXTRACT_PATH_text(sender_address, 'additional_info', true) as sender_address_additional_info,
JSON_EXTRACT_PATH_text(sender_address, 'company', true) as sender_address_company,
JSON_EXTRACT_PATH_text(receiver_address, 'country_iso', true) as reciever_address_country_iso,
JSON_EXTRACT_PATH_text(receiver_address, 'country', true) as reciever_address_country,
JSON_EXTRACT_PATH_text(receiver_address, 'city', true) as reciever_address_city,
JSON_EXTRACT_PATH_text(receiver_address, 'state', true) as reciever_address_state,
JSON_EXTRACT_PATH_text(receiver_address, 'zipcode', true) as reciever_address_zipcode,
JSON_EXTRACT_PATH_text(receiver_address, 'address1', true) as reciever_address_address1,
JSON_EXTRACT_PATH_text(receiver_address, 'address2', true) as reciever_address_address2,
JSON_EXTRACT_PATH_text(receiver_address, 'additional_info', true) as reciever_address_additional_info,
JSON_EXTRACT_PATH_text(receiver_address, 'company', true) as reciever_address_company,
weight,
height,
tracking_url,
width,
warehouse_code,
status,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items,  numbers_.ordinal::int,true),'variant_sku') as variant_sku,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items,  numbers_.ordinal::int,true),'asset_uid') as asset_uid,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items,  numbers_.ordinal::int,true),'allocation_uid') as allocation_uid,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items,  numbers_.ordinal::int,true),'order_number') as order_number,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items,  numbers_.ordinal::int,true),'order_mode') as order_mode,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items,  numbers_.ordinal::int,true),'serial_number') as serial_number,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items,  numbers_.ordinal::int,true),'contract_id') as contract_id,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items,  numbers_.ordinal::int,true),'current_flow') as current_flow,
metadata,
warehouse_external_id,
length,
shipping_label_url,
consumed_at,
type,
pickup_address
from s3_spectrum_kafka_topics_raw_sensitive.fulfillment_eu_shipment_update_v1 s
cross join numbers_
where numbers_.ordinal < json_array_length(package_items, true)
and cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
AND published_at >= current_date -2;
