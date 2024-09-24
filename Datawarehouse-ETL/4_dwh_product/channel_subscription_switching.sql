drop table if exists dwh.channel_subscription_switching;
create table dwh.channel_subscription_switching as 
with asv as (
select 
 fact_day,
 st.store_short,
 sum(active_subscriptions) as active_subscriptions
from dwh.product_subscription_value s 
inner join ods_production.store st 
 on st.id=s.store_id
left join public.dim_dates d 
 on d.datum=s.fact_day
where d.day_is_first_of_month
group by 1,2
order by 1 desc)
,switch as (
select 
date_trunc('month',sw.date)::date as fact_day,
s.store_short,
coalesce(count(distinct s.subscription_id),0) as subscription_switching
from ods_production.subscription_plan_switching sw 
left join ods_production.subscription s on 
  s.subscription_id=sw.subscription_id 
group by 1,2)
select asv.*, s.subscription_switching
from asv 
left join switch s on asv.fact_day=s.fact_day 
and asv.store_short=s.store_short
and s.fact_day=asv.fact_day;

GRANT SELECT ON dwh.channel_subscription_switching TO tableau;
