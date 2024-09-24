drop table if exists dm_operations.shipments_unique;
create table dm_operations.shipments_unique
as
select 
	'outbound' as shipment_type,
	ob_shipment_unique as shipment_unique,
	carrier,
	shipment_service,
    shipping_country,
    region,
	customer_type,
	warehouse,
	warehouse_detail,
	count(asset_id) as assets_shipped,
	min(allocated_at) as allocated_at,
	min(shipment_label_created_at) as shipment_label_created_at ,
	min(shipment_at) as shipment_at,
	min(delivered_at) as delivered_at
from ods_operations.allocation_shipment 
where 
	outbound_bucket not in ('Partners Offline', 'Cancelled Before Shipment', 'Ready to Ship')
group by 1, 2, 3, 4, 5, 6, 7, 8, 9
union all 
select 
	'return' as shipment_type,
	ib_shipment_unique as shipment_unique,
	return_carrier as carrer,
	null as shipment_service,
    shipping_country,
    region,
	customer_type,
	return_warehouse as warehouse,
	null as warehouse_detail,
	count(asset_id) as assets_shipped,
	min(allocated_at) as allocated_at,
	min(return_shipment_label_created_at) as shipment_label_created_at,
	min(return_shipment_at) as shipment_at ,
	min(return_delivery_date) as delivered_at
from ods_operations.allocation_shipment 
where 
	return_shipment_uid is not null or 
	return_tracking_number is not null or 
	return_shipment_label_created_at is not null or 
	return_shipment_at is not null or 
	return_delivery_date is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9;


drop table if exists dm_operations.shipments_daily;
create table dm_operations.shipments_daily
as
select 
	shipment_type,
	date_trunc ('day', coalesce (shipment_at, delivered_at)) as fact_date, 
	carrier,
	shipment_service,
    shipping_country,
    region,
	customer_type,
	warehouse,
	warehouse_detail,
	count(distinct shipment_unique) as total_shipments,
	sum(assets_shipped) as total_assets_shipped 
from dm_operations.shipments_unique
group by 1,2,3,4,5,6,7,8,9;


drop table if exists dm_operations.shipments_variant_sku;
create table dm_operations.shipments_variant_sku
as
with cat_stg as (
	select 
		'outbound' as shipment_type,
		t.category_name ,
		t.subcategory_name ,
		t.product_sku,
		t.variant_sku ,
		t.asset_name ,
		s.ob_shipment_unique as shipment_unique ,
        s.customer_type,
        s.shipping_country,
        s.region,
		s.carrier,
		s.warehouse,
		d.weight_bucket,
		min(shipment_label_created_at) as shipment_label_created_at ,
		min(shipment_at) as shipment_at,
		min(delivered_at) as delivered_at,
		count(s.asset_id) as assets_shipped
	from ods_operations.allocation_shipment s
	left join master.asset t on s.asset_id = t.asset_id
	left join dm_operations.dimensions d ON d.variant_sku = t.variant_sku AND d.product_sku = t.product_sku 
	where 
		outbound_bucket not in ('Partners Offline', 'Cancelled Before Shipment', 'Ready to Ship')
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13
	union all
	select 
		'return' as shipment_type,
		t.category_name ,
		t.subcategory_name ,
		t.product_sku ,
		t.variant_sku ,
		t.asset_name ,
		s.ib_shipment_unique as shipment_unique ,
        s.customer_type,
        s.shipping_country,
        s.region,
		s.carrier,
		s.warehouse,
		d.weight_bucket,
		min(return_shipment_label_created_at) as shipment_label_created_at ,
		min(return_shipment_at) as shipment_at,
		min(return_delivery_date) as delivered_at,
		count(s.asset_id) as assets_shipped
	from ods_operations.allocation_shipment s
	left join master.asset t on s.asset_id = t.asset_id
	left join dm_operations.dimensions d ON d.variant_sku = t.variant_sku AND d.product_sku = t.product_sku 
	where 
		return_shipment_uid is not null or 
		return_tracking_number is not null or 
		return_shipment_label_created_at is not null or 
		return_shipment_at is not null or 
		return_delivery_date is not null
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13
	)
select 
	shipment_type,
	category_name ,
	subcategory_name ,
	product_sku,
	variant_sku ,
	asset_name,
    customer_type,
    shipping_country,
    region,
	carrier,
	warehouse,
	weight_bucket,
	date_trunc('day', coalesce (shipment_at,  delivered_at)) as fact_date, 
	count(distinct shipment_unique) as total_shipments,
	sum(assets_shipped) as total_assets_shipped 
from cat_stg 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13;

GRANT SELECT ON dm_operations.shipments_variant_sku TO tableau;
GRANT SELECT ON dm_operations.shipments_daily TO tableau;
GRANT SELECT ON dm_operations.shipments_daily TO GROUP recommerce_data;
