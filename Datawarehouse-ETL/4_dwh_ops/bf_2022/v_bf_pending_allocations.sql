create or replace view dm_operations.v_bf_pending_allocations AS
with hours as (
	select ordinal as hours 
	from public.numbers 
	where ordinal < 24
),
days as (
	select datum, hours 
	from public.dim_dates 
	cross join hours 
	where datum >= '2022-11-07' and datum <= current_date
)
select 
	datum, 
	hours,
	case when store_id in (14, 629, 621) then 'US' else 'EU' end as region,
	sum(case 
	    when dateadd('hour', hours, datum) = date_trunc('hour', current_timestamp at time zone 'CET')
	    then quantity
	    else initial_quantity end) pending_allocations
from ods_production.inventory_reservation, days
where convert_timezone('CET', paid_at) < dateadd('hour', hours, datum) 
	and (fulfilled_at 	is null or convert_timezone('CET', fulfilled_at)	> dateadd('hour', hours, datum))
	and (cancelled_at 	is null or convert_timezone('CET', cancelled_at)	> dateadd('hour', hours, datum))
	and (deleted_at 	is null or convert_timezone('CET', deleted_at) 	    > dateadd('hour', hours, datum))
	and (expired_at 	is null or convert_timezone('CET', expired_at) 	    > dateadd('hour', hours, datum))
	and (declined_at 	is null or convert_timezone('CET', declined_at) 	> dateadd('hour', hours, datum))
	and dateadd('hour', hours, datum) <= current_timestamp at time zone 'CET'
group by 1, 2, 3
with no schema binding;