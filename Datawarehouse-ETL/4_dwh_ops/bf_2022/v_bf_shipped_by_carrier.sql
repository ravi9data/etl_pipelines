create or replace view dm_operations.v_bf_shipped_by_carrier AS
with shipped_assets as (
	select 
		date_trunc('hour',
					convert_timezone('CET', shipment_at)) as fact_date,
		region,
		count(distinct allocation_id) shipped_assets
	from ods_operations.allocation_shipment
	where shipment_at > '2022-11-07'
	and is_last_allocation_per_asset 
	and store <> 'Partners Offline'
	group by 1,2
	)
select 
	date_trunc('day', fact_date) datum, 
	date_part('hours', fact_date) hours,
	region,
	shipped_assets
from shipped_assets
with no schema binding;