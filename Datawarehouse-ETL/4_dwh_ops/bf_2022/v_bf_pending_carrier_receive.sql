create or replace view dm_operations.v_bf_pending_carrier_receive AS
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
processed_assets as(
	select 
		convert_timezone('CET', shipment_label_created_at) as shipment_label_created_at,
		convert_timezone('CET', shipment_at) as shipment_at,
		convert_timezone('CET', delivered_at) as delivered_at,
		convert_timezone('CET', refurbishment_start_at) as refurbishment_start_at,
		convert_timezone('CET', return_shipment_label_created_at) as return_shipment_label_created_at,
		allocation_id,
		region
	from ods_operations.allocation_shipment
        where allocated_at::date>='2022-06-01'
	      and store <> 'Partners Offline'
	)
select 
	datum, 
	hours,
	region,
	count(distinct allocation_id)::int as processed_assets 
	from processed_assets, days
	where  shipment_label_created_at < dateadd('hour', hours, datum)
	  and (shipment_at 	is null or shipment_at	> dateadd('hour', hours, datum))
      and (delivered_at	is null or delivered_at	> dateadd('hour', hours, datum))
	  and (refurbishment_start_at 			is null or refurbishment_start_at > 			dateadd('hour', hours, datum))
      and (return_shipment_label_created_at is null or return_shipment_label_created_at > 	dateadd('hour', hours, datum))
      and dateadd('hour', hours, datum) <= current_timestamp at time zone 'CET'
      group by 1,2,3
with no schema binding;