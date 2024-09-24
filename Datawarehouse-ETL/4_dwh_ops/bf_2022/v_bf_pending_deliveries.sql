create or replace view dm_operations.v_bf_pending_deliveries AS
with hours as (
	select ordinal as hours 
	from public.numbers 
	where ordinal < 24
),
days as (
	select datum, hours 
	from public.dim_dates 
	cross join hours 
	where datum >=  '2022-11-07' 
	and datum <= current_date
),
pending_deliveries as (
	select 
		convert_timezone('CET', shipment_at) as shipment_at,
		convert_timezone('CET', delivered_at) as delivered_at,
		convert_timezone('CET', failed_delivery_at) as failed_delivery_at,
		allocation_id,
		region
	from ods_operations.allocation_shipment as2
	where allocated_at::date>='2022-06-01'
	and as2.store <> 'Partners Offline'
)
select 
	datum, 
	hours,
	region, 
	count(distinct allocation_id)::int as pending_assets
from pending_deliveries, days
where shipment_at < dateadd('hour', hours, datum)
and (delivered_at 		is null or delivered_at			> dateadd('hour', hours, datum))
and (failed_delivery_at 	is null or failed_delivery_at	> dateadd('hour', hours, datum))
and dateadd('hour', hours, datum) <= current_timestamp at time zone 'CET'
group by 1,2,3
with no schema binding;