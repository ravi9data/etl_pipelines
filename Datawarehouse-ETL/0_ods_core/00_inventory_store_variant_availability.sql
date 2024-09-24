
/*
 *Incremental load script for inventory_store_variant_availability
 *
 * */

drop table if exists tmp_inventory_store_variant_availability;

create temp table tmp_inventory_store_variant_availability
as (
with sva_events as (
select
	json_extract_path_text(payload, '_id') as id,
	json_extract_path_text(payload, 'date') as date,
	json_extract_path_text(payload, 'op') as op,
	json_extract_path_text(payload, 'type') as type,
	json_extract_path_text(payload, 'uid') as uid,
	json_extract_path_text(payload, 'next') as next,
	json_extract_path_text(payload, 'prev') as prev,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'storeUid') as store_uid,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'storeId') as store_id,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'skuUid') as sku_uid,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'skuVariantCode') as sku_variant_code,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'availabilityMode') as availability_mode ,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'orderMode') as order_mode,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'mixVariantUid') as mix_variant_uid,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'enabledModeThreshold') as enabled_mode_threshold ,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'availableCount') as available_count,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'inStockCount') as in_stock_count,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'reservedCount') as reserved_count,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'meta') as meta,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'createdAt') as created_at,
	json_extract_path_text(json_extract_path_text(payload, 'state'),'updatedAt') as updated_at,
	row_number() over (partition by store_uid,sku_uid order by updated_at desc) rn
from
	s3_spectrum_kafka_topics_raw.inventory_store_variant_availability s
	where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-2
)
select * from sva_events
where rn = 1);


DELETE FROM ods_production.inventory_store_variant_availability
  USING tmp_inventory_store_variant_availability
  WHERE inventory_store_variant_availability.store_uid  = tmp_inventory_store_variant_availability.store_uid
  and inventory_store_variant_availability.sku_uid  = tmp_inventory_store_variant_availability.sku_uid;


insert into	ods_production.inventory_store_variant_availability(
	uid,
	store_uid,
	store_id,
	sku_uid,
	sku_variant_code,
	availability_mode,
	order_mode,
	mix_variant_uid,
	enabled_mode_threshold,
	available_count,
	in_stock_count,
	reserved_count,
	meta,
	created_at,
	updated_at)
select
	uid,
	store_uid,
	nullif(store_id,'')::int,
	sku_uid,
	sku_variant_code,
	availability_mode,
	nullif(order_mode,''),
	nullif(mix_variant_uid,''),
	nullif(enabled_mode_threshold,''),
	nullif(available_count,'')::int,
	nullif(in_stock_count,'')::int,
	nullif(reserved_count,'')::int,
	meta,
	nullif(created_at,'')::timestamp,
	nullif(updated_at,'')::timestamp
from
	tmp_inventory_store_variant_availability
where
	op in ('update', 'create');

update ods_production.inventory_store_variant_availability a
set product_sku = b.product_sku
from (select distinct v.variant_sku  ,p.product_sku
from ods_production.product p
join ods_production.variant v on p.product_id = v.product_id ) b
where a.sku_variant_code = b.variant_sku ;

/*assign sku from sku_variant*/
update ods_production.inventory_store_variant_availability
set product_sku = split_part(sku_variant_code , 'V', 1)
where product_sku is null;
