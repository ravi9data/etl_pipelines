--b2b_marketing_compaigns_report
drop view if exists dm_finance.v_b2b_marketing_compaigns_report;
create view dm_finance.v_b2b_marketing_compaigns_report as
with sessions as(
select distinct 
 session_id, 
 session_start,
 marketing_campaign,
 first_page_title
from traffic.sessions s
where marketing_source = 'medifox')
,orders as (
select 
 o.order_id, 
 som.session_start,
 first_page_title,
s.marketing_campaign,
 max(case when cart_orders>=1 then 1 end) as cart_orders,
 max(case when completed_orders>=1 then 1 end) as submitted_orders,
 max(case when paid_orders>=1 then 1 end) as paid_orders,
 count(distinct subscription_id) as subscriptions,
 sum(subscription_value) as acqired_subscription_value,
 sum(committed_sub_value + sub.additional_committed_sub_value) as committed_subscription_value,
 avg(rental_period) as avg_duration
from traffic.session_order_mapping som  
inner join sessions s 
 on som.session_id=s.session_id
inner join master."order" o 
 on o.order_id=som.order_id
left join master.subscription sub 
 on sub.order_id=o.order_id
where last_touchpoint_before_submitted
 group by 1,2,3,4
 )
,session_summary as (
select 
session_start::date as session_start, 
'affiliate' as medium,
'medifox' as source,
case when marketing_campaign != 'n/a' then marketing_campaign else first_page_title end as campaign,
count(distinct session_id) as sessions
from sessions
group by 1,2,3,4)
,order_summary as (
select 
	order_id,
session_start::date as session_start,
'affiliate' as medium,
'medifox' as source,
first_page_title as campaign,
sum(cart_orders) as cart_orders,
sum(submitted_orders) as submitted_orders,
sum(paid_orders) as paid_orders,
sum(acqired_subscription_value) as acqired_subscription_value,
sum(committed_subscription_value) as committed_subscription_value,
sum(subscriptions) as subscriptions,
avg(avg_duration) as avg_duration
from orders
group by 1,2,3,4,5
)
select 
	order_id,
 s.session_start,
 s.medium,
 s.source,
 s.campaign,
 s.sessions,
 COALESCE(o.cart_orders,0) as cart_orders,
 COALESCE(o.submitted_orders,0) as submitted_orders,
 COALESCE(o.paid_orders,0) as paid_orders,
 COALESCE(o.subscriptions,0) as subscriptions,
 COALESCE(o.acqired_subscription_value,0) as acqired_subscription_value,
 COALESCE(o.committed_subscription_value,0) as committed_subscription_value,
 o.avg_duration
from session_summary s
left join order_summary o 
 on s.session_start=o.session_start
 and s.medium=o.medium
 and s.source=o.source
 and s.campaign=o.campaign
 with no schema binding;