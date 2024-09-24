
delete from staging.spectrum_operations_replacement_allocated
where consumed_at::date >= current_date::date-2;

delete from staging.spectrum_operations_replacement_allocated where allocation_uid  in (
select distinct
JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'uid') as allocation_uid
from  s3_spectrum_kafka_topics_raw.operations_replacement_allocated s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
and consumed_at::date >= current_date::date-2);

insert into staging.spectrum_operations_replacement_allocated
select distinct
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'created_at') as allocated_at,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'uid') as allocation_uid,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'asset_uid') as asset_uid,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'reservation_uid') as reservation_uid,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'salesforce_asset_id') as asset_id,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'salesforce_allocation_id') as salesforce_allocation_id,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'sku_variant_code') as variant_sku,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'serial_number') as serial_number,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'store_id') as store_id,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'warehouse_code') as warehouse,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'uid') as replaced_allocation_uid,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'asset_uid') as replaced_asset_uid,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'reservation_uid') as replaced_reservation_uid,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'serial_number') as replaced_serial_number,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'salesforce_asset_id') as repalced_asset_id,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'created_at') as replacement_date,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'updated_at') as allocation_updated_at,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'uid') as replaced_by,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'allocation'), 'salesforce_subscription_id') as salesforce_subscription_id,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'salesforce_subscription_id') as replaced_salesforce_subscription_id,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'sku_variant_code') as replaced_variant_sku,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'salesforce_allocation_id') as replaced_salesforce_allocation_id,
    JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload, 'replaced_allocation'), 'updated_at') as replaced_allocation_updated_at,
    JSON_EXTRACT_PATH_text(payload, 'order_number') as order_id,
    JSON_EXTRACT_PATH_text(payload, 'user_id') as customer_id,
    JSON_EXTRACT_PATH_text(payload, 'replacement_uid') as replacement_uid,
    payload,
    consumed_at
from s3_spectrum_kafka_topics_raw.operations_replacement_allocated s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
and consumed_at::date >= current_date::date-2
QUALIFY row_number () over (partition by allocation_uid order by consumed_at desc) = 1 ;