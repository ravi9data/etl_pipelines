--materialised for staging.stream_shipping_shipment_inbound
delete from staging.stream_shipping_shipment_inbound
where event_timestamp::date >= current_date -2;

insert into staging.stream_shipping_shipment_inbound
select * from (  with
		 numbers_ as(
				select * from public.numbers
				   where ordinal < 100)
select distinct
event_timestamp,
event_name,
JSON_EXTRACT_PATH_text(payload ,'uid') as uid,
JSON_EXTRACT_PATH_text(payload ,'order_mode') as order_mode,
json_extract_path_text(payload, 'order_number') as order_number,
JSON_EXTRACT_PATH_text(payload ,'tracking_url') as tracking_url,
JSON_EXTRACT_PATH_text(payload ,'tracking_number') as tracking_number,
JSON_EXTRACT_PATH_text(payload ,'shipping_label_url') as shipping_label_url,
JSON_EXTRACT_PATH_text(payload ,'shipping_profile') as shipping_profile,
json_extract_path_text(payload, 'service') as service,
JSON_EXTRACT_PATH_text(payload ,'customer') as customer_info,
JSON_EXTRACT_PATH_text (customer_info, 'user_type') as user_type,
payload,
version,
JSON_EXTRACT_PATH_text(payload ,'carrier') as carrier,
JSON_EXTRACT_PATH_text(payload ,'type') as shipment_type,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'email') as customer_email,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'phone') as customer_phone,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'user_id') as user_id,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'language') as customer_language,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'lastname') as customer_lastname,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'birthdate') as customer_birthdate,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'firstname') as customer_firstname,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'country_iso') as sender_customer_iso,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'address1') as sender_address1,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'address2') as sender_address2,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'country') as sender_country,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'city') as sender_city,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'zipcode') as sender_zipcode,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'additional_info') as sender_additional_info,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'country_iso') as receiver_country_iso,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'address1') as receiver_address1,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'address2') as receiver_address2,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'country') as receiver_country,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'city') as receiver_city,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'zipcode') as receiver_zipcode,
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'additional_info') as receiver_additional_info,
(case when JSON_EXTRACT_PATH_text(payload ,'package_items') = '[]' then null else JSON_EXTRACT_PATH_text(payload ,'package_items') end) as package_items,
 JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'variant_sku') as package_items_variant_sku,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'order_number') as package_items_order_number,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'order_mode') as package_items_order_mode,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'allocation_uid') as package_items_allocation_uid,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'asset_uid') as package_items_asset_uid,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'salesforce_asset_id') as package_items_salesforce_asset_id,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'serial_number') as package_items_serial_number,
JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'contract_id') as package_items_contract_id
from stg_kafka_events_full.stream_shipping_shipment_inbound
cross join numbers_
where numbers_.ordinal < json_array_length(package_items, true)
   and event_timestamp::date >= current_date -2
);
