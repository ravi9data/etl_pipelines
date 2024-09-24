create or replace view dm_operations.v_bf_shipments AS
select 
    convert_timezone('CET',as2.ready_to_ship_at) as ready_to_ship_at,
    convert_timezone('CET',as2.shipment_label_created_at) as shipment_label_created_at,
    convert_timezone('CET',as2.shipment_at) as shipment_at,
    convert_timezone('CET',as2.delivered_at) as delivered_at,
    convert_timezone('CET',as2.failed_delivery_at) as failed_delivery_at,
    as2.variant_sku,
    v.variant_name,
    as2.warehouse,
    as2.shipping_country,
    as2.customer_type,
    as2.infra,
    as2.carrier,
    as2.allocation_status_original,
    as2.allocation_id,
    as2.region,
    as2.order_id
from ods_operations.allocation_shipment as2
left join ods_production.variant v 
on as2.variant_sku = v.variant_sku
where is_last_allocation_per_asset
and allocated_at>=current_date - 365
and as2.store <> 'Partners Offline'
with no schema binding;
