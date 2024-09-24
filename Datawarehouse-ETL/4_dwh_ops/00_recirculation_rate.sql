drop table if exists dm_operations.recirculated_assets;
create table dm_operations.recirculated_assets as
with allocations as (
select 
	a.allocation_id,
	case when a.store_short = 'Grover International' then s.country_name 
		 when a.store_short = 'Partners Online' then a.store_short || '-' || s.country_name 
		 else a.store_short end as store_short ,
	case when s.country_name = 'United States' then 'US' else 'EU' end as store_region,
	is_recirculated,
	s.category_name,
	a.subcategory_name,
	a.product_sku,
	s.brand,
	s.product_name,
	allocated_at
	from master.allocation a
	left join master.subscription s on a.subscription_id = s.subscription_id  
	    where rank_allocations_per_subscription = 1 
	    and a.store_short != 'Partners Offline'
	)
	, allocated_detailed as (
	select 
		DATE_TRUNC('day',allocated_at) as fact_date,
		store_short,
		is_recirculated,
		category_name,
		subcategory_name,
		product_sku,
		brand,
		product_name,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated
		from allocations
		group by 1,2,3,4,5,6,7,8
		)
	, allocated_store as (
	select 
		DATE_TRUNC('day',allocated_at) as fact_date,
		store_short,
		'Total' as is_recirculated,
		category_name,
		subcategory_name,
		product_sku,
		brand,
		product_name,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated
		from allocations
		group by 1,2,3,4,5,6,7,8
		)
	, allocated_region as (
	select 
		DATE_TRUNC('day',allocated_at) as fact_date,
		store_region,
		'Total' as is_recirculated,
		category_name,
		subcategory_name,
		product_sku,
		brand,
		product_name,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated
		from allocations
		group by 1,2,3,4,5,6,7,8
		)
	, allocated_total as (
	select 
		DATE_TRUNC('day',allocated_at) as fact_date,
		'Total' AS store_short,
		'Total' as is_recirculated,
		category_name,
		subcategory_name,
		product_sku,
		brand,
		product_name,
		count(allocation_id) as assets_allocated,
		count(case when is_recirculated = 'Re-circulated' then allocation_id end) as assets_recirculated
		from allocations
		group by 1,2,3,4,5,6,7,8
		)	
	, allocated as (
	select * from allocated_detailed 
	union all 
    select * from allocated_store
	union all 
    select * from allocated_region
    UNION ALL 
	select * from allocated_total     
	)
select 
	a.fact_date, 
	a.store_short,
	a.is_recirculated,
	category_name,
	subcategory_name,
	product_sku,
	brand,
	product_name,
	assets_allocated,
	assets_recirculated
	from allocated a 
order by 1 desc;