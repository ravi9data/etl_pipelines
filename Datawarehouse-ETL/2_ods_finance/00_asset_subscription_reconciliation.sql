drop table if exists ods_production.asset_subscription_reconciliation;
create table ods_production.asset_subscription_reconciliation as 
with sub_recon as (
select 
a.asset_id, 
count(distinct 
	case when
		start_date::date<date_trunc('month',CURRENT_DATE)
		and coalesce(cancellation_date,'2030-12-31')::date >= date_trunc('month',CURRENT_DATE)
	   then s.subscription_id 
	end) as active_subscriptions_bom,
count(distinct 
	case 
		when status='ACTIVE' 
	   then s.subscription_id 
	end) as active_subscriptions,
count(distinct 
	case 
		when start_date::Date<date_trunc('month',CURRENT_DATE) 
		and coalesce(cancellation_date,'2030-12-31')::date >= date_trunc('month',CURRENT_DATE)
		then s.subscription_id 
	end) as rollover_subscriptions,
count(distinct 
	case 
		when date_trunc('month',start_date)::Date=date_trunc('month',CURRENT_DATE)
		then s.subscription_id 
	end) as acquired_subscriptions,	
count(distinct 
 	case 
 		when date_trunc('month',cancellation_date)::Date=date_trunc('month',CURRENT_DATE)
 		 then s.subscription_id 
 	end) as cancelled_subscriptions
from ods_production.subscription s 
left join ods_production.allocation a 
 on s.subscription_id=a.subscription_id
where a.allocation_status_original not in ('CANCELLED')
group by 1)
------------------------------------------------------------------------------------------------
,active_value as (
select distinct 
 s.subscription_id,
 a.asset_id,
 s.subscription_value,
 s.start_date,
 max(case when date_trunc('month',s.start_date)=date_trunc('month',current_date) then 1 else 0 end) as is_acquired,
 row_number() over(partition by a.asset_id order by s.start_date desc) rn
from ods_production.subscription s 
left join ods_production.allocation a 
 on s.subscription_id=a.subscription_id
where a.allocation_status_original not in ('CANCELLED')
and status='ACTIVE'
group by 1,2,3,4)
------------------------------------------------------------------------------------------------
,active_value_final as (
select 
asset_id,
listagg(subscription_id, ', ') as active_subscription_id,
sum(subscription_value) as active_subscription_value,
sum(case when is_acquired=1 then subscription_value end) as acquired_subscription_value,
sum(case when is_acquired=0 then subscription_value end) as rollover_subscription_value
from active_value 
group by 1
    )
------------------------------------------------------------------------------------------------
,active_value_final_subs as (
select 
asset_id,
subscription_id
from active_value 
where rn = 1
)    
-------------------------------------------------------------------------------
,cancelled_value as (
select distinct 
 s.subscription_id,
 a.asset_id,
 s.subscription_value,
 max(case when date_trunc('month',s.cancellation_date)=date_trunc('month',current_date) then 1 else 0 end) as is_cancelled
from ods_production.subscription s 
left join ods_production.allocation a 
 on s.subscription_id=a.subscription_id 
where a.allocation_status_original not in ('CANCELLED')
and date_trunc('month',s.cancellation_date)=date_trunc('month',current_date)
group by 1,2,3
)
-------------------------------------------------------------------------------
,cancelled_value_final as (
select 
 asset_id,
 listagg(subscription_id, ', ') as cancelled_subscription_id,
 sum(subscription_value) as cancelled_subscription_value
from cancelled_value 
group by 1
    )
select 
 s.asset_id,
 active_subscription_id,
 cancelled_subscription_id,
 s.active_subscriptions_bom,
 active_subscriptions,
 rollover_subscriptions,
 acquired_subscriptions,
 cancelled_subscriptions,
 coalesce(active_subscription_value,0) as active_subscription_value,
 coalesce(rollover_subscription_value,0) as rollover_subscription_value,
 coalesce(acquired_subscription_value,0) as acquired_subscription_value,
 coalesce(cancelled_subscription_value,0) as cancelled_subscription_value,
 avfs.subscription_id last_active_subscription_id
from sub_recon s 
left join active_value_final avf 
 on avf.asset_id=s.asset_id
left join active_value_final_subs avfs 
 on avfs.asset_id=s.asset_id
left join cancelled_value_final cvf 
 on cvf.asset_id=s.asset_id
;

GRANT SELECT ON ods_production.asset_subscription_reconciliation TO tableau;
