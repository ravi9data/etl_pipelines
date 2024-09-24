
drop table if exists trans_dev.marketing_channel_retention_traffic;

create table trans_dev.marketing_channel_retention_traffic as 
with sessions_prep_new
as (
select 
 date_trunc('week',session_start)::DATE as week_date,
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
 date_trunc('week',session_start)::DATE as week_date,
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
),
prep as (
select * from sessions_prep_new
union all 
select * from sessions_prep_rec)
select 
 week_date,
 coalesce(country,'n/a') as COUNTRY,
 coalesce(channel,'n/a') as channel,
 coalesce(retention_group,'n/a') as retention_group,
 sum(traffic) as traffic
from prep
group by 1,2,3,4
;

commit;

drop table if exists dwh.marketing_cost_weekly_reporting;

create table dwh.marketing_cost_weekly_reporting as 
with cost as (
select 
 week_date::date as week_date,
 week_number,
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
else COALESCE(CHANNEL,'n/a')
end as channel,
 sum(mktg_costs) as cost 
from dwh.weekly_marketing_costs
where country <> 'Total'
and (channel_type ='Spend (Channel Grouping)' or channel in ('Vouchers','Other (agency, production, etc)'))
--and week_number='2020-13'
group by 1,2,3,4)
,cost_split as (
select distinct
 coalesce(m.week_Date,c.week_date) as week_date, 
 coalesce(m.country,c.country) as country,  
 coalesce(m.channel,c.channel) as  channel,
 coalesce(m.retention_group,'NEW') as retention_group,
 coalesce(sum(m.traffic) over (partition by  
    m.week_Date, 
    m.country,  
    m.channel),0) as total_traffic,
 coalesce(m.traffic,0) as traffic,
 coalesce(m.traffic::decimal/
 (sum(m.traffic) over (partition by  
    m.week_Date, 
    m.country,  
    m.channel))::decimal,1) as retention_weight,
coalesce(c.cost,0) as total_cost,
c.cost*(
 coalesce(m.traffic::decimal/
 (sum(m.traffic) over (partition by  
    m.week_Date, 
    m.country,  
    m.channel))::decimal,1)
) as cost
from cost  c 
full outer join trans_dev.marketing_channel_retention_traffic  m 
on c.week_date=m.week_date
and m.country=c.country
and m.channel=c.channel)
,subs as (
select 
 date_trunc('week',s.created_date)::DATE as week_date,
 coalesce(o.store_country,'n/a') as country,  
 o.marketing_channel as channel,
 o.new_recurring as retention_group,
 count(distinct s.subscription_id) as subscriptions,
 count(distinct s.customer_id) as customers
from master."order" o
left join master.subscription s 
 on o.order_id=s.order_id
group by 1,2,3,4
),
orders as (
select 
 date_trunc('week',o.created_date)::DATE as week_date,
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
 date_trunc('week',s.created_date)::DATE as week_date,
 coalesce(o.store_country,'n/a') as country,  
 oc.first_touchpoint as channel,
 o.new_recurring as retention_group,
 count(distinct s.subscription_id) as first_touchpoint_subscriptions,
 count(distinct s.customer_id) as first_touchpoint_customers
from master."order" o
left join traffic.order_conversions oc on oc.order_id=o.order_id
left join master.subscription s 
 on o.order_id=s.order_id
group by 1,2,3,4
),
ftp_orders as (
select 
 date_trunc('week',o.created_date)::DATE as week_date,
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
 date_trunc('week',s.created_date)::DATE as week_date,
 coalesce(o.store_country,'n/a') as country,  
 m.marketing_Channel as channel,
 o.new_recurring as retention_group,
 count(distinct s.subscription_id) as any_touchpoint_subscriptions,
 count(distinct s.customer_id) as any_touchpoint_customers
from master."order" o
left join traffic.session_order_mapping m on m.order_id=o.order_id
left join master.subscription s 
 on o.order_id=s.order_id
group by 1,2,3,4
),
atp_orders as (
select 
 date_trunc('week',o.created_date)::DATE as week_date,
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
SELECT DISTINCT
COALESCE(O.COUNTRY,C.COUNTRY) AS COUNTRY,
case 
when COALESCE(O.CHANNEL,C.CHANNEL) in ('Retail','Partnerships')
then 'Retail'
else COALESCE(O.CHANNEL,C.CHANNEL)
end as channel,
COALESCE(O.retention_group,C.retention_group) AS retention_group,
COALESCE(O.week_date,C.week_date)::DATE AS week_date,
coalesce(sum(C.TRAFFIC),0) as TRAFFIC,
coalesce(sum(O.carts ),0) as order_in_cart,
coalesce(sum(ftpo.first_touchpoint_carts ),0) as first_touchpoint_carts,
coalesce(sum(atpo.any_touchpoint_carts ),0) as any_touchpoint_carts,
coalesce(sum(O.submitted_orders ),0) as submitted_orders,
coalesce(sum(ftpo.first_touchpoint_submitted_orders ),0) as first_touchpoint_submitted_orders,
coalesce(sum(atpo.any_touchpoint_submitted_orders ),0) as any_touchpoint_submitted_orders,
coalesce(sum(s.subscriptions ),0) as subscriptions,
coalesce(sum(ftps.first_touchpoint_subscriptions ),0) as first_touchpoint_subscriptions,
coalesce(sum(atps.any_touchpoint_subscriptions ),0) as any_touchpoint_subscriptions,
coalesce(sum(s.customers),0) as customers,
coalesce(sum(ftps.first_touchpoint_customers),0) as first_touchpoint_customers,
coalesce(sum(atps.any_touchpoint_customers),0) as any_touchpoint_customers,
coalesce(sum(C.COST),0) as cost
FROM ORDERS O
FULL OUTER JOIN subs s
on s.week_date = O.week_date
AND O.CHANNEL=s.CHANNEL 
 AND O.COUNTRY=s.COUNTRY 
 and o.retention_group=s.retention_group
FULL OUTER JOIN  ftp_subs ftps
on ftps.week_date = O.week_date
 AND O.COUNTRY=ftps.COUNTRY 
 and o.retention_group=ftps.retention_group
 and ftps.channel = o.channel
 FULL OUTER JOIN ftp_orders ftpo
 on ftpo.week_date = O.week_date
 AND O.COUNTRY=ftpo.COUNTRY 
 and o.retention_group=ftpo.retention_group
 and o.channel = ftpo.channel
FULL OUTER JOIN   atp_subs atps
on atps.week_date = O.week_date
 AND O.COUNTRY=atps.COUNTRY 
 and o.retention_group=atps.retention_group
 and atps.channel = o.channel
 FULL OUTER JOIN atp_orders atpo
 on atpo.week_date = O.week_date
 AND O.COUNTRY=atpo.COUNTRY 
 and o.retention_group=atpo.retention_group
 and o.channel = atpo.channel 
FULL OUTER JOIN cost_split C
 ON O.WEEK_DATE=C.WEEK_DATE
 AND O.CHANNEL=C.CHANNEL 
 AND O.COUNTRY=C.COUNTRY 
 and o.retention_group=c.retention_group
where COALESCE(O.week_date,C.week_date)::DATE>='2019-07-01'
 group by 1,2,3,4
;

commit;

drop table trans_dev.marketing_channel_retention_traffic;

commit; 