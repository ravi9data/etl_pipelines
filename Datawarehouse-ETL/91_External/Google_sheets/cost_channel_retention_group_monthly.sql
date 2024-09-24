-- Step 1: Staging table from google sheet import

drop table if exists trans_dev.monthly_marketing_costs;

create table trans_dev.monthly_marketing_costs as
with dim_dates as (
select last_day(datum) as month_date,
       substring(to_date(datum,'YYYY-MM'),1,7) as month_number,
       year_number
from public.dim_dates
where datum > '2015-12-31' and datum < '2022-01-01'
), 
month_dates as(
select
  *
from dim_dates
group by 1,2,3)
,mktg_cost as (
select channel||'-'||country as channel_country,
       *
from stg_external_apis.monthly_mktg_import
where monthnumber not like ('%Week%')
and monthnumber not like ('%Q%')
and monthnumber not like ('%-20%')
)
select
md.month_date,
mc.monthnumber as month_number,
mc.channel_country,
mc.country,
mc.channel,
mc.channeltype as channel_type,
mc.value as mktg_costs
from mktg_cost mc
left join month_dates md
on mc.monthnumber = md.month_number;


-- Step 2: Intermediate marketing channel table

drop table if exists trans_dev.monthly_marketing_channel_retention_traffic;

create table trans_dev.monthly_marketing_channel_retention_traffic as 
with sessions_prep_new
as (
select
 last_day(s.session_start::date) as month_date,
 case
  when st.country_name is null
   and s.geo_country='DE'
    then 'Germany'
  when st.country_name is null
   and s.geo_country = 'AT'
    then 'Austria'
  when st.country_name is null
   and s.geo_country = 'NL'
    then 'Netherlands'
  when st.country_name is null
   and s.geo_country = 'ES'
    then 'Spain'   
  else coalesce(st.country_name,'Germany')
  end as country,
  marketing_channel as channel,
 'NEW' as retention_group,
 count(distinct s.anonymous_id)*0.8 as traffic
from traffic.sessions s
left join ods_production.store st
 on st.id=s.store_id
left join master.customer_historical c
 on c.customer_id=s.customer_Id
 and c."date"=s.session_start::date-1
group by 1,2,3,4
),
 sessions_prep_rec
as (
select
 last_day(s.session_start::date) as month_date,
 case
  when st.country_name is null
   and s.geo_country='DE'
    then 'Germany'
  when st.country_name is null
   and s.geo_country = 'AT'
    then 'Austria'
  when st.country_name is null
   and s.geo_country = 'NL'
    then 'Netherlands'
  when st.country_name is null
   and s.geo_country = 'ES'
    then 'Spain'   
  else coalesce(st.country_name,'Germany')
  end as country,
  marketing_channel as channel,
 'RECURRING' as retention_group,
 count(distinct s.anonymous_id)*0.2 as traffic
from traffic.sessions s
left join ods_production.store st
 on st.id=s.store_id
left join master.customer_historical c
 on c.customer_id=s.customer_Id
 and c."date"=s.session_start::date-1
group by 1,2,3,4
)
,sessions_prep as (
select * from sessions_prep_new
union all 
select * from sessions_prep_rec
)
select
 month_date,
 coalesce(country,'n/a') as country,
 coalesce(channel,'n/a') as channel,
 coalesce(retention_group,'n/a') as retention_group,
 sum(traffic) as traffic
from sessions_prep
group by 1,2,3,4
;

-- Step 3: DWH target table

drop table if exists dwh.marketing_cost_monthly_reporting;

create table dwh.marketing_cost_monthly_reporting as
with monthly_cost as (
select
 month_date,
 month_number,
 coalesce(country,'n/a') as country,
case
when CHANNEL in  ('Affiliates','Affliates')
then 'Affiliates'
when CHANNEL in ('Waiting list offers','Email','CRM')
then 'CRM'
when CHANNEL in ('Others','only Direct','Offline','n/a','Internal','Direct','(Other)')
then 'Direct & Other'
when CHANNEL in ('Retargeting','Display')
then 'Display'
when CHANNEL in ('Paid Social','Paid Content')
then 'Paid Social'
when CHANNEL in ('ReferralRock','Refer Friend')
then 'Refer Friend'
when CHANNEL in ('Retail','Direct Partnerships')
then 'Retail'
when CHANNEL in('Partnerships')
then 'Partnerships'
when CHANNEL in ('Social Media','Social')
then 'Social Media'
else coalesce(CHANNEL,'n/a')
end as channel,
 sum(mktg_costs) as cost
from trans_dev.monthly_marketing_costs
where country <> 'Total'
and (channel_type ='Spend (Channel Grouping)' or channel in ('Vouchers','Other (agency, production, etc)'))
group by 1,2,3,4
),
cost_split as (
select distinct
 coalesce(m.month_date,c.month_date) as month_date,
 coalesce(m.country,c.country) as country,
 coalesce(m.channel,c.channel) as  channel,
 coalesce(m.retention_group,'NEW') as retention_group,
 coalesce(sum(m.traffic) over (partition by
    m.month_date,
    m.country,
    m.channel),0) as total_traffic,
 coalesce(m.traffic,0) as traffic,
 coalesce(m.traffic::decimal/
 (sum(m.traffic) over (partition by
    m.month_date,
    m.country,
    m.channel))::decimal,1) as retention_weight,
coalesce(c.cost,0) as total_cost,
c.cost*(
 coalesce(m.traffic::decimal/
 (sum(m.traffic) over (partition by
    m.month_date,
    m.country,
    m.channel))::decimal,1)
) as cost
from monthly_cost c
full outer join trans_dev.monthly_marketing_channel_retention_traffic  m
on c.month_date=m.month_date
and m.country=c.country
and m.channel=c.channel
),
subs as (
select
 last_day(s.created_date::date) as month_date,
 coalesce(o.store_country,'n/a') as country,
 o.marketing_channel as channel,
 o.new_recurring as retention_group,
 count(distinct s.subscription_id) as subscriptions,
 count(distinct s.customer_id) as customers,
 sum(subscription_value) as subscription_value,
 sum(committed_sub_value + s.additional_committed_sub_value) as committed_subscription_value
from master."order" o
left join master.subscription s
 on o.order_id=s.order_id
group by 1,2,3,4
),
orders as (
select
 last_day(o.created_date::date) as month_date,
 coalesce(o.store_country,'n/a') as country,
 o.marketing_channel as channel,
 o.new_recurring as retention_group,
 count(distinct case when o.cart_orders>=1 then o.order_id end) as carts,
 count(distinct case when o.completed_orders>=1 then o.order_id end) as submitted_orders,
 count(distinct case when o.paid_orders>=1 then o.order_id end) as paid_orders
from master."order" o
group by 1,2,3,4
)
,ftp_subs as (
select 
 last_day(s.created_date::date) as month_date,
 coalesce(o.store_country,'n/a') as country,  
 oc.first_touchpoint as channel,
 o.new_recurring as retention_group,
 count(distinct s.subscription_id) as first_touchpoint_subscriptions,
 count(distinct s.customer_id) as first_touchpoint_customers,
 sum(subscription_value) as first_touchpoint_subscription_value,
 sum(committed_sub_value + s.additional_committed_sub_value) as first_touchpoint_committed_subscription_value
from master."order" o
left join traffic.order_conversions oc on oc.order_id=o.order_id
left join master.subscription s 
 on o.order_id=s.order_id
group by 1,2,3,4
),
ftp_orders as (
select 
  last_day(o.created_date::date) as month_date,
 coalesce(o.store_country,'n/a') as country,  
 oc.first_touchpoint as channel,
 o.new_recurring as retention_group,
 count(distinct case when o.cart_orders>=1 then o.order_id end) as first_touchpoint_carts,
 count(distinct case when o.completed_orders>=1 then o.order_id end) as first_touchpoint_submitted_orders,
 count(distinct case when o.paid_orders>=1 then o.order_id end) as first_touchpoint_paid_orders
from master."order" o
left join traffic.order_conversions oc on oc.order_id=o.order_id
group by 1,2,3,4
)
,atp_subs as (
select 
last_day(s.created_date::date) as month_date,
 coalesce(o.store_country,'n/a') as country,  
 m.marketing_Channel as channel,
 o.new_recurring as retention_group,
 count(distinct s.subscription_id) as any_touchpoint_subscriptions,
 count(distinct s.customer_id) as any_touchpoint_customers,
 sum(subscription_value) as any_touchpoint_subscription_value,
 sum(committed_sub_value + s.additional_committed_sub_value) as any_touchpoint_committed_subscription_value
from master."order" o
left join traffic.session_order_mapping m on m.order_id=o.order_id
left join master.subscription s 
 on o.order_id=s.order_id
group by 1,2,3,4
),
atp_orders as (
select 
last_day(o.created_date::date) as month_date,
 coalesce(o.store_country,'n/a') as country,  
 m.marketing_channel as channel,
 o.new_recurring as retention_group,
 count(distinct case when o.cart_orders>=1 then o.order_id end) as any_touchpoint_carts,
 count(distinct case when o.completed_orders>=1 then o.order_id end) as any_touchpoint_submitted_orders,
 count(distinct case when o.paid_orders>=1 then o.order_id end) as any_touchpoint_paid_orders
from master."order" o
left join traffic.session_order_mapping m on m.order_id=o.order_id
group by 1,2,3,4
)
select distinct
coalesce(o.country,c.country) as country,
--COALESCE(O.CHANNEL,C.CHANNEL,T.CHANNEL) AS CHANNEL,
case
when coalesce(o.channel,c.channel) in ('Retail','Partnerships')
then 'Retail'
else coalesce(o.channel,c.channel)
end as channel,
coalesce(O.retention_group,C.retention_group) AS retention_group,
coalesce(O.month_date,C.month_date) AS month_date,
coalesce(sum(C.TRAFFIC),0) as traffic,
coalesce(sum(O.carts ),0) as order_in_cart,
coalesce(sum(ftpo.first_touchpoint_carts ),0) as first_touchpoint_carts,
coalesce(sum(atpo.any_touchpoint_carts ),0) as any_touchpoint_carts,
coalesce(sum(O.submitted_orders ),0) as submitted_orders,
coalesce(sum(ftpo.first_touchpoint_submitted_orders ),0) as first_touchpoint_submitted_orders,
coalesce(sum(atpo.any_touchpoint_submitted_orders ),0) as any_touchpoint_submitted_orders,
coalesce(sum(s.subscriptions ),0) as subscriptions,
coalesce(sum(s.subscription_value ),0) as subscription_value,
coalesce(sum(s.committed_subscription_value ),0) as committed_subscription_value,
coalesce(sum(ftps.first_touchpoint_subscription_value ),0) as first_touchpoint_subscription_value,
coalesce(sum(ftps.first_touchpoint_committed_subscription_value ),0) as first_touchpoint_committed_subscription_value,
coalesce(sum(atps.any_touchpoint_subscription_value ),0) as any_touchpoint_subscription_value,
coalesce(sum(atps.any_touchpoint_committed_subscription_value ),0) as any_touchpoint_committed_subscription_value,
coalesce(sum(ftps.first_touchpoint_subscriptions ),0) as first_touchpoint_subscriptions,
coalesce(sum(atps.any_touchpoint_subscriptions ),0) as any_touchpoint_subscriptions,
coalesce(sum(s.customers),0) as customers,
coalesce(sum(ftps.first_touchpoint_customers),0) as first_touchpoint_customers,
coalesce(sum(atps.any_touchpoint_customers),0) as any_touchpoint_customers,
coalesce(sum(C.COST),0) as cost
from orders o
FULL OUTER JOIN subs s
on s.month_date = O.month_date
AND O.CHANNEL=s.CHANNEL
 AND O.COUNTRY=s.COUNTRY
 and o.retention_group=s.retention_group
FULL OUTER JOIN ftp_subs ftps
on ftps.month_date = O.month_date
 AND O.COUNTRY=ftps.COUNTRY 
 and o.retention_group=ftps.retention_group
 and ftps.channel = o.channel
 FULL OUTER JOIN ftp_orders ftpo
 on ftpo.month_date = O.month_date
 AND O.COUNTRY=ftpo.COUNTRY 
 and o.retention_group=ftpo.retention_group
 and o.channel = ftpo.channel
 FULL OUTER JOIN atp_subs atps
on atps.month_date = O.month_date
 AND O.COUNTRY=atps.COUNTRY 
 and o.retention_group=atps.retention_group
 and atps.channel = o.channel
 FULL OUTER JOIN atp_orders atpo
 on atpo.month_date = O.month_date
 AND O.COUNTRY=atpo.COUNTRY 
 and o.retention_group=atpo.retention_group
 and o.channel = atpo.channel 
 FULL OUTER JOIN cost_split C
 ON O.month_date=C.month_date
 AND O.CHANNEL=C.CHANNEL
 AND O.COUNTRY=C.COUNTRY
 and o.retention_group=c.retention_group
where coalesce(O.month_date,C.month_date)::DATE>='2019-07-01'
 group by 1,2,3,4
;