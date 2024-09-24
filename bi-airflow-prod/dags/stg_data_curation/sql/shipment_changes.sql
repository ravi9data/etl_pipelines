
delete from staging.stream_shipping_shipment_change_v3
where event_timestamp::date >= current_date::date-1;


insert into staging.stream_shipping_shipment_change_v3
with numbers_ as(
		select * from public.numbers
where ordinal < 100)
select
    "timestamp" as event_timestamp ,
    event_name ,
    JSON_EXTRACT_PATH_text(payload ,'uid') as uid,
    JSON_EXTRACT_PATH_text(payload ,'order_number') as order_number,
    JSON_EXTRACT_PATH_text(payload ,'order_mode') as order_mode,
    JSON_EXTRACT_PATH_text(payload ,'tracking_url') as tracking_url,
    JSON_EXTRACT_PATH_text(payload ,'tracking_number') as tracking_number,
    JSON_EXTRACT_PATH_text(payload ,'shipping_label_url') as shipping_label_url,
    JSON_EXTRACT_PATH_text(payload ,'shipping_profile') as shipping_profile,
    JSON_EXTRACT_PATH_text(payload ,'carrier') as carrier,
    JSON_EXTRACT_PATH_text(payload ,'service') as service,
    JSON_EXTRACT_PATH_text(payload ,'customer') as customer,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'email') as customer_email,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'phone') as customer_phone,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'company') as customer_company,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'user_id') as user_id,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'language') as customer_language,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'lastname') as customer_lastname,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'birthdate') as customer_birthdate,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'firstname') as customer_firstname,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'customer'),'user_type') as user_type,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'country_iso') as sender_customer_iso,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'address1') as sender_address1,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'address2') as sender_address2,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'country') as sender_country,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'city') as sender_city,
     JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'state') as sender_state,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'zipcode') as sender_zipcode,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'sender_address'),'additional_info') as sender_additional_info,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'country_iso') as receiver_country_iso,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'address1') as receiver_address1,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'address2') as receiver_address2,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'country') as receiver_country,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'city') as receiver_city,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'state') as receiver_state,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'zipcode') as receiver_zipcode,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload ,'receiver_address'),'additional_info') as receiver_additional_info,
    (case when JSON_EXTRACT_PATH_text(payload ,'package_items') = '[]' then null else JSON_EXTRACT_PATH_text(payload ,'package_items') end) as package_items,
    JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'variant_sku') as variant_sku,
    JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'allocation_uid') as allocation_uid,
    JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'asset_uid') as asset_uid,
    JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'salesforce_asset_id') as salesforce_asset_id,
    JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'serial_number') as serial_number,
    JSON_EXTRACT_PATH_text(json_extract_array_element_text(package_items, numbers_.ordinal::int,true), 'contract_id') as contract_id,
    nullif(json_extract_path_text(payload, 'transition_timestamp'),'string') as transition_timestamp,
    nullif (JSON_EXTRACT_PATH_text(payload ,'location'),'') as location,
    nullif(JSON_EXTRACT_PATH_text(payload ,'warehouse_code'),'') as warehouse_code,
    payload,
    "version"::int
FROM s3_spectrum_kafka_topics_raw.shipping_shipment_change
CROSS JOIN numbers_
WHERE numbers_.ordinal < json_array_length(package_items, TRUE)
AND CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-1
AND event_timestamp >= current_date-2
and is_valid_json(payload);
