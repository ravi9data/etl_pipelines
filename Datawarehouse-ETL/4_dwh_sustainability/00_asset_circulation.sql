drop table if exists dm_sustainability.asset_circulation
create table dm_sustainability.asset_circulation as
select 
	date_part('year', created_at) purchase_year, 
	category_name,
	case when total_allocations_per_asset >= 4 then '4+' 
		else total_allocations_per_asset::text end total_circulation,
	case when asset_status_original in (
		'LOST',
		'SOLD',
		'WRITTEN OFF DC',
		'WRITTEN OFF OPS',
		'REPORTED AS STOLEN'
		)
		then false else true end as still_own_asset,
	count(1)
from master.asset 
group by 1,2,3,4;