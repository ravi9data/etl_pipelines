DROP TABLE if exists monitoring.or1;

Create TABLE monitoring.or1 as

With incomplete_return as 
(
select a.customer_id,c.customer_type,count(allocation_id) as total_count_per_customer, TRUE as incomplete_check
from ods_production.allocation a
left join ods_production.customer c on a.customer_id = c.customer_id 
where delivered_at is not null
and	cancellation_returned_at is not null
and return_shipment_at is null
group by 1,2
order by 3 desc
)
select a.allocation_id,
a.allocation_sf_id, 
a.customer_id,
a.is_package_lost,
a.order_id,
a.return_shipment_at,
a.return_shipment_tracking_number,
a.return_delivery_date,
a.is_last_allocation_per_asset,
a.rank_allocations_per_subscription,
s.status,
ast.asset_name, 
ast.asset_id,
ast.asset_status_original,
ast.serial_number,
ast.initial_price,
ah.allocated_assets as allocated_assets_per_customer,
case when ir.incomplete_check is true 
	then true else false
		end as incomplete_check,
c.customer_type,
ir.total_count_per_customer
from
ods_production.allocation a
left join ods_production.subscription s on a.subscription_id = s.subscription_id
left join ods_production.asset ast on ast.asset_id = a.asset_id
left join ods_production.customer_allocation_history ah on ah.customer_id = a.customer_id
left join incomplete_return ir on ir.customer_id = a.customer_id
left join ods_production.customer c on c.customer_id = a.customer_id
where allocation_status_original = 'IN TRANSIT';