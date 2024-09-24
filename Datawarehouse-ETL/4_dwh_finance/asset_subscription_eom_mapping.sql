
drop table if exists dm_finance.asset_subscription_eom_mapping;
create table dm_finance.asset_subscription_eom_mapping as 
with a as (
	select 
	distinct 
	datum, 
	a.asset_id, 
	last_value(s.subscription_id) over (partition by asset_id, dd.datum order by coalesce(a.cancellation_returned_at,a.refurbishment_start_at, a.refurbishment_end_at,a.return_delivery_date,s.cancellation_date,CURRENT_DATE)
										ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_subscription_id,
	s.subscription_id, 
	s.subscription_value,
	coalesce(a.shipment_at,a.allocated_at,s.start_date) as start_date,  
	coalesce(a.cancellation_returned_at,a.refurbishment_start_at, a.refurbishment_end_at,a.return_delivery_date,s.cancellation_date) as cancellation_date
	from 
		public.dim_dates dd, 
		master.subscription s, 
		master.allocation a 
	where 
	s.subscription_id=a.subscription_id
	and (day_is_last_of_month =1 or datum= current_date-1)
	and datum<=CURRENT_DATE
	and coalesce(a.shipment_at,a.allocated_at,s.start_date)<=dd.datum 
	and CURRENT_DATE >= dd.datum
	--and a.allocation_status_original not in ('CANCELLED','TO BE FIXED')
	ORDER BY 2,1 DESC
)
select 
datum,
asset_id,
subscription_id,
subscription_value,
start_date,
cancellation_date
from a 
where subscription_id=last_subscription_id
order by 2,1 desc 
;
