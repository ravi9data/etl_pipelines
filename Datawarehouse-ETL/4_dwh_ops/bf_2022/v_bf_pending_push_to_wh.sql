create or replace view dm_operations.v_bf_pending_push_to_wh AS
with hours as (
	select ordinal as hours 
	from public.numbers 
	where ordinal < 24
),
days as (
	select datum, hours 
	from public.dim_dates 
	cross join hours 
	where datum >=  date_add('day',-30,current_date)
	and datum <= current_date
),
pending_shipments as (
	select 
		convert_timezone('CET', allocated_at) as allocated_at,
		convert_timezone('CET', ready_to_ship_at) as ready_to_ship_at,
		allocation_id,
		as2.serial_number,
		as2.order_id
	from ods_production.allocation as2
	left join ods_production.subscription s on as2.subscription_id = s.subscription_id
	where allocated_at::date>='2023-07-01'
	and as2.allocation_status_original='PENDING SHIPMENT'
	and s.status ='ACTIVE'
	and allocated_at is not null
	and s.country_name<>'United States'
	and as2.asset_status='RESERVED'
)

select 
	datum, 
	hours,
	'EU' AS region, 
	count(distinct allocation_id)::int as pending_shipments
from pending_shipments, days
where allocated_at < dateadd('hour', hours, datum)
and (ready_to_ship_at is null or ready_to_ship_at	> dateadd('hour', hours, datum))
and dateadd('hour', hours, datum) <= current_timestamp at time zone 'CET'
GROUP BY 1,2,3
with no schema binding;