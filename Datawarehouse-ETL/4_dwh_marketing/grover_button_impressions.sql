with fact_days as(
	SELECT
		DISTINCT DATUM AS reporting_date, 
		id as store_id, 
		store_name,
		store_label
	FROM public.dim_dates, (select id, store_name, store_label, created_date::date from ods_production.store where  store_short = 'Partners Online' or id = 1)
		 where DATUM <= CURRENT_DATE and datum >=created_date::date)
, page_view as(select  
		pv.page_view_date::date as reporting_date,
		pv.store_id,  
		count(distinct page_view_id) as page_views
	from traffic.page_views pv 
	group by 1,2
	order by 2 desc)
	,impressions as (
	select 
		i.reporting_date::date as reporting_date,
		i.store_id,
		s.store_name,
		s.store_label,
		s.store_code,
		i.button_state, 
    	sum(impressions) as impressions,
     	sum(unique_impressions) as unique_impressions 
	join ods_production.store s 
	on s.id =i.store_id 
	group by 1,2,3,4,5,6
)
,b as (
	select DISTINCT 
		date_trunc('day',created_date)::date as reporting_date,
		store_id,
		store_label,
		store_name,
        store_number,
		sum(COALESCE(cart_orders, 0::bigint)) AS carts,
   		sum(COALESCE(completed_orders, 0::bigint)) AS completed_orders,
    	sum(COALESCE(declined_orders, 0::bigint)) AS declined_orders,
    	sum(COALESCE(paid_orders, 0::bigint)) AS paid_orders
	from master."order"  
	group by 1,2,3,4,5
	order by 2 desc
), final as (
select 
	f.reporting_date,
	f.store_id,
	f.store_label,
	f.store_name,
	coalesce(b.carts,0) as carts,
	coalesce(b.completed_orders,0) as completed_orders,
	coalesce(b.declined_orders,0) as declined_orders,
	coalesce(b.paid_orders,0) as paid_orders,
	coalesce(a.page_views,0) as page_views,
	sum(coalesce(impressions,0)) as impressions,
	sum(case when button_state = 'available' then i.impressions else 0 end) as available_impressions,
	sum(case when button_state = 'widget' then i.impressions else 0 end) as widget_impressions,
	sum(case when button_state = 'unavailable' then i.impressions else 0 end) as unavailable_impressions
from fact_days f
left join page_view a on a.reporting_date = f.reporting_date and a.store_id = f.store_id
left join b on f.store_id=b.store_id and f.reporting_date=b.reporting_date
left join impressions i ON f.reporting_date = i.reporting_date AND f.store_id = i.store_id
group by 1,2,3,4,5,6,7,8,9
order by 1 desc)
select *,
		--are all rows zero?
		case when (impressions = 0
		and page_views = 0 
		and carts = 0 
		and paid_orders = 0 
		and declined_orders = 0 ) then 'True'
		else 'False' end as is_all_zero
		from final
		where is_all_zero ='False';

