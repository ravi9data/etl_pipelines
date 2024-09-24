drop table if exists ods_production.subscription_retention;
create table ods_production.subscription_retention as
with journey as (
select distinct 
 s.subscription_id, 
 max(case when s2.start_date::DATE<s.start_date::DATE then s2.start_date end) as start_of_previous_sub,
 min(case when s2.start_date::DATE>s.start_date::DATE then s2.start_date end) as start_of_next_sub,
 max(case when s2.cancellation_date::DATE<s.start_date::DATE then s2.cancellation_date end) as cancell_previous_sub
from ods_production.subscription s
left join ods_production.subscription s2 on s.customer_id=s2.customer_id
group by 1)
,next_ as (
select 
 s.customer_id, 
 subscription_id, 
 lead(retention_group) over (partition by customer_id order by start_date) as retention_group_of_next_sub,
  max(cancellation_date) over (partition by customer_id) as max_cancellatioN_date,
 coalesce(count(case when cancellation_date is null then subscription_id end) over (partition by customer_id),0) as active_subs_per_customer
from ods_production.subscription s
left join ods_production.order_retention_group r 
 on r.order_id=s.order_id
)
select 
 s.customer_id, 
 s.subscription_id, 
 s.order_id,
 rank_subscriptions, 
 start_date, 
 cancellation_date, 
 r.retention_group,
 n.retention_group_of_next_sub,
 j.start_of_previous_sub,
 j.start_of_next_sub,
 j.cancell_previous_sub,
 start_date::date - (case 
 					  when retention_group = 'RECURRING, REACTIVATION' 
 					   then cancell_previous_sub 
  					  when retention_group = 'RECURRING, UPSELL' 
  					   then start_of_previous_sub 
  					  end)::date as days_to_return,
  start_date::date - (case when retention_group = 'RECURRING, REACTIVATION' then cancell_previous_sub end)::date as days_to_reactivation,
  start_date::date - (case when retention_group = 'RECURRING, UPSELL' then start_of_previous_sub end)::date as days_to_upsell,
  CASE 
     when cancellation_date is null 
    then 'ACTIVE'
    WHEN max_cancellatioN_date::date = cancellation_date::date
     and active_subs_per_customer = 0
    THEN 'CHURN'
    when n.retention_group_of_next_sub='RECURRING, REACTIVATION' 
    THEN coalesce(case when ((j.start_of_next_sub::DATE-cancellation_date::DATE)/30)::TEXT <0 
          then 'Cancelled but still renting something' else least(((j.start_of_next_sub::DATE-cancellation_date::DATE)/30),48)::TEXT end,'Cancelled but still renting something')
   WHEN N.retention_group_of_next_sub='RECURRING, UPSELL' 
    or (N.retention_group_of_next_sub IS NULL
     and max_cancellatioN_date > cancellation_date)
    THEN 'Cancelled but still renting something'
    else 'Cancelled but still renting something'
   END AS DAYS_TO_REACTIVATE_CHURN_SUB
from ods_production.subscription s 
left join ods_production.order_retention_group r 
 on r.order_id=s.order_id
left join journey j 
 on j.subscription_id=s.subscription_id
left join next_ n  
 on n.subscription_id=s.subscription_id
--where s.customer_Id ='125170'
order by rank_subscriptions asc;

GRANT SELECT ON ods_production.subscription_retention TO tableau;
