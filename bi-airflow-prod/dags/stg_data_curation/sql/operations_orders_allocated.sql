--materalised for operations_order_allocated
delete from staging.spectrum_operations_order_allocated
where consumed_at > current_date-2;

delete from staging.spectrum_operations_order_allocated where allocation_uid  in (
select distinct
JSON_EXTRACT_PATH_text(allocation_item, 'uid') as allocation_uid
from  s3_spectrum_kafka_topics_curated.operations_order_allocated s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
and consumed_at::date >= current_date::date-2
and allocation_item<>'nan');

insert into staging.spectrum_operations_order_allocated
select distinct
    JSON_EXTRACT_PATH_text(allocation_item, 'created_at') as allocated_at,
    JSON_EXTRACT_PATH_text(allocation_item, 'uid') as allocation_uid,
    JSON_EXTRACT_PATH_text(allocation_item, 'asset_uid') as asset_uid,
    JSON_EXTRACT_PATH_text(allocation_item, 'reservation_uid') as reservation_uid,
    JSON_EXTRACT_PATH_text(allocation_item, 'salesforce_asset_id') as asset_id,
    JSON_EXTRACT_PATH_text(allocation_item, 'salesforce_allocation_id') as salesforce_allocation_id,
    JSON_EXTRACT_PATH_text(allocation_item, 'sku_variant_code') as variant_sku,
    JSON_EXTRACT_PATH_text(allocation_item, 'serial_number') as serial_number,
    JSON_EXTRACT_PATH_text(allocation_item, 'store_id') as store_id,
    JSON_EXTRACT_PATH_text(allocation_item, 'salesforce_subscription_id') as salesforce_subscription_id,
    JSON_EXTRACT_PATH_text(allocation_item,'updated_at') as updated_at,
    nullif(JSON_EXTRACT_PATH_text(allocation_item, 'warehouse_code'), '') as warehouse,
    order_number as order_id,
    user_id as customer_id,
    event_name,
    consumed_at,
    processed_at,
    allocation_item
from s3_spectrum_kafka_topics_curated.operations_order_allocated s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
and consumed_at::date >= current_date::date-2
and allocation_item<>'nan'
QUALIFY row_number () over (partition by allocation_uid order by consumed_at desc) = 1 ;
