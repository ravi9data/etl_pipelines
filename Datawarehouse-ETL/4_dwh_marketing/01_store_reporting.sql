drop TABLE if exists dwh.store_reporting;
CREATE TABLE dwh.store_reporting AS 
WITH FACT_DAYS AS (
SELECT
DISTINCT DATUM AS FACT_DAY
FROM public.dim_dates
WHERE DATUM <= CURRENT_DATE
)
,sub_value as (
select 
f.fact_day,
coalesce(s.store_id::text,'n/a') as store_id,
COALESCE(sum(s.subscription_value_eur),0) as active_subscription_value,
COALESCE(count(distinct s.subscription_id),0) as active_subscriptionS
from fact_days f
left join ods_production.subscription_phase_mapping s
   on f.fact_day::date >= s.fact_day::date and
  F.fact_day::date <= coalesce(s.end_date::date, f.fact_day::date+1)
group by 1,2)
,traffic as (
select 
 pv.store_id::CHARACTER VARYING,  
 pv.page_view_start::date as pageview_Date,
 count(distinct pv.page_view_id) as pageviews,
 count(distinct pv.page_view_id) as pageviews_w_url,
 count(distinct pv.session_id) as pageview_unique_sessions,
-- count(distinct coalesce(customer_id_mapped,anonymous_id)) as pageview_unique_customers,
 count(distinct case when s.is_paid then pv.session_id end) paid_traffic_sessions
-- count(distinct case when paid_nonpaid='Paid' then coalesce(customer_id_mapped,anonymous_id) end) paid_traffic_customers
from traffic.page_views pv 
left join traffic.sessions s
  ON pv.session_id = s.session_id 
where page_view_start::date>='2019-08-12'
group by 1,2
union all
select 
 v.store_id::CHARACTER VARYING as store_id,  
 v.creation_time::Date as pageview_Date,
 count(*) as pageviews,
 count(case when page_id is not null then session_id end) as pageviews_w_url,
-- count(distinct session_id) 
  count(case when page_id is not null then session_id end) as pageview_unique_sessions,
 --count(distinct user_id) as pageview_unique_customers,
 null as paid_traffic_sessions
-- null as paid_traffic_customers
from stg_events.product_views v 
where v.creation_time::Date<'2019-08-12'
group by 1,2
order by 3 desc
)
,orders as (
select 
 o.store_id as store_id,
 i.created_at::Date as order_date, 
 count(*) as carts, 
-- count(distinct case when plan_duration <= 1 then o.order_id end) as short_term_carts,
-- count(distinct case when plan_duration >= 12 then o.order_id end) as long_term_carts,
 count(distinct case when o.is_in_salesforce then o.order_id end) as completed_orders,
 count(distinct case when o.status = 'PAID' then o.order_id end) as paid_orders,
 count(distinct case when o.status = 'PAID' and plan_duration <= 1 then o.order_id end) as paid_short_term_orders,
 count(distinct case when o.status = 'PAID' and plan_duration >= 12 then o.order_id end) as paid_long_term_orders,
 count(distinct case when o.is_in_salesforce and rg.new_recurring='NEW' then o.order_id end) as completed_new_orders, 
 count(distinct case when o.status = 'PAID' and rg.new_recurring = 'NEW' then o.order_id end) as paid_new_orders, 
 count(distinct case when returned>0 then o.order_id end) as returned_orders
from ods_production.order_item i
inner join ods_production."order" o on i.order_id=o.order_id
left join ods_production.order_retention_group rg on rg.order_id=o.order_id
left join (
select order_id, sum(case when in_transit_at::date-delivered_at::date <=7 then 1 else 0 end) as returned
from ods_production.allocation
where in_transit_at is not null 
group by 1 ) r on r.order_id=o.order_id
group by 1,2
order by 3 desc
)
,subs as (
select 
start_date::date as report_date,
coalesce(store_id,'N/A') as store_id,
suM(subscription_value) as acquired_subscription_value,
sum(committed_sub_value) as acquired_committed_subscription_value,
avg(subscription_value) as avg_price, 
avg(rental_period) as avg_duration,
count(distinct subscription_Id) as acquired_subscriptions
from ods_production.subscription s
group by 1,2
)
,subs_c as (
select 
cancellation_date::date as report_date,
coalesce(store_id,'N/A') as store_id,
suM(subscription_value) as cancelled_subscription_value,
    count(distinct subscription_Id) as cancelled_subscriptions
from ods_production.subscription s
group by 1,2
)
,conv as (
select distinct 
coalesce(store_name,'N/A') as store_name,
coalesce(store_label,'N/A') as store_label,
s.id as store_id,
coalesce(o.order_date::date,t.pageview_Date::date) as report_date,
coalesce(pageviews_w_url,0) as pageviews,
pageview_unique_sessions,
-- pageview_unique_customers,
 paid_traffic_sessions,
--paid_traffic_customers,
coalesce(carts,0) as carts,
coalesce(completed_orders,0) as completed_orders,
-- short_term_carts,
-- long_term_carts,
 paid_short_term_orders,
 paid_long_term_orders,
coalesce(completed_new_orders,0) as completed_new_orders,
coalesce(paid_orders,0) as paid_orders,
coalesce(paid_new_orders,0) as paid_new_orders,
coalesce(returned_orders,0) as returned_within_7_Days
from traffic t 
full outer join orders o 
 on t.store_id=o.store_id
 and t.pageview_Date=o.order_date
left join ods_production.store s 
 on s.id=coalesce(o.store_id,t.store_id)
order by 3 desc)
  select 
  coalesce(v.fact_DAY,c.report_date,subs.report_date) as FACT_DAY,
  COALESCE(S.ACCOUNT_NAME,'N/A') AS ACCOUNT_NAME,
  COALESCE(S.STORE_NAME,'N/A') AS STORE_NAME,
  COALESCE(S.STORE_LABEL,'N/A') AS STORE_LABEL,
  COALESCE(S.STORE_SHORT,'N/A') AS STORE_SHORT,
  coalesce(active_subscription_value,0) as active_subscription_value,
  coalesce(active_subscriptions,0) as active_subscriptions,
  coalesce(pageviews,0) as pageviews,
 --  short_term_carts,
-- long_term_carts,
 paid_short_term_orders,
 paid_long_term_orders,
 pageview_unique_sessions,
-- pageview_unique_customers,
--  paid_traffic_customers,
  paid_traffic_sessions,
coalesce(carts,0) as carts,
coalesce(completed_orders,0) as completed_orders,
coalesce(paid_orders,0) as paid_orders,
coalesce(completed_new_orders,0) as completed_new_orders,
coalesce(paid_new_orders,0) as paid_new_orders,
coalesce(subs.acquired_subscription_value,0) as acquired_subscription_value,
coalesce(subs_c.cancelled_subscription_value,0) as cancelled_subscription_value,
coalesce(acquired_subscriptions,0) as acquired_subscriptions,
coalesce(cancelled_subscriptions,0) as cancelled_subscriptions,
coalesce(subs.acquired_committed_subscription_value,0) as acquired_committed_subscription_value,
coalesce(returned_within_7_Days,0) as returned_within_7_Days
  from sub_value v 
  full outer join conv c 
   on v.fact_day=c.report_date
   and v.store_id=c.store_id
    full outer join subs
   on subs.report_date=coalesce(c.report_date,v.fact_day)
   and subs.store_id::text=coalesce(c.store_id::text,v.store_id::text)
       full outer join subs_c 
   on subs_c.report_date=coalesce(c.report_date,v.fact_day)
   and subs_c.store_id::text=coalesce(c.store_id::text,v.store_id::text)
   left join ods_production.store s 
   on s.ID::TEXT=coalesce(v.store_id::TEXT,c.store_id::TEXT)
  ;