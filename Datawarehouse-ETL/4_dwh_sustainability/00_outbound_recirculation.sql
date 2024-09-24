drop table if exists dm_sustainability.outbound_recirculation
create table dm_sustainability.outbound_recirculation as
with allocations as (
	select 
		allocation_id ,
		allocated_at ,
		store_short ,
		asset_id ,
		is_recirculated ,
		subscription_id 
	from master.allocation
	where store_short <> 'Partners Offline'
	--and allocated_at > current_date - 365
)
select 
	date_trunc('week', a.allocated_at) fact_date,
	case when a.store_short = 'Grover International' then s.country_name 
		 when a.store_short = 'Partners Online' then a.store_short || '-' || s.country_name 
		 else a.store_short end as store_short ,
	t.category_name ,
	t.subcategory_name ,
	t.brand ,
	t.product_sku ,
	t.product_name ,
	date_part('year', t.created_at) purchase_year, 
	count(allocation_id) assets_allocated,
	count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated
from allocations a 
left join master.asset t on a.asset_id = t.asset_id  
left join master.subscription s on a.subscription_id = s.subscription_id 
where t.asset_id is not null
group by 1,2,3,4,5,6,7,8;