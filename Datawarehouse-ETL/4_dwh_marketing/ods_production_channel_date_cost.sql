drop table if exists ods_production.channel_date_cost;
create table ods_production.channel_date_cost as  
with volume as (
select 
o.created_date::date as created_date,
d.week_number,
store.store_short as store,
coalesce(ch.marketing_channel,'N/A') as marketing_channel,
count(distinct case when is_in_salesforce then o.order_id end) as completed_orders,
count(distinct case when o.status = 'PAID' then o.order_id end) as paid_orders,
count(distinct case when is_in_salesforce and new_recurring='NEW' then o.order_id end) as completed_new_orders,
count(distinct case when is_in_salesforce and new_recurring='RECURRING' then o.order_id end) as completed_recurring_orders,
count(distinct case when o.status = 'PAID' and new_recurring='NEW' then o.order_id end) as paid_new_orders,
count(distinct case when o.status = 'PAID' and new_recurring='RECURRING' then o.order_id end) as paid_Recurring_orders,
count(distinct s.subscription_id) as subscriptions,
count(case when s.subscriptions_per_customer=1 and payment_count=1 and s.status = 'CANCELLED' and customer_type = 'normal_customer' then s.subscription_id end) as churned_subscriptions,
count(distinct case when is_in_salesforce and customer_type = 'business_customer' then o.order_id end) as completed_b2b_orders,
count(distinct case when o.status = 'PAID' and customer_type = 'business_customer' then o.order_id end) as paid_b2b_orders,
count(distinct case when o.status = 'PAID' and new_recurring = 'NEW' and customer_type = 'business_customer' then o.order_id end) as paid_b2b_new_orders
from ods_production.order o 
left join ods_production.customer c on c.customer_id = o.customer_id
left join ods_production.order_marketing_channel ch on o.order_id =ch.order_id
left join ods_production.order_retention_group r on o.order_id =r.order_id
left join ods_production.subscription s on s.order_id=o.order_id
left join ods_production.subscription_cashflow sc on sc.subscription_id=s.subscription_id
left join public.dim_dates d on d.datum=o.created_date::date
left join ods_production.store store on store.id=o.store_id
--where o.store_id=1
group by 1,2,3,4
order by 1 desc )
,cost as (
select 
coalesce(cost.channel,'N/A') as marketing_channel,
'Grover' as store,
cost.week_number,
d.datum,
cost, 
cost_new_customers,
cost_recurring_customers,
cost/7 as daily_cost,
cost_new_customers/7 as daily_cost_new_customers,
cost_recurring_customers/7 as daily_cost_recurring_customers
from stg_external_apis.channel_week_marketing_cost cost
left join public.dim_dates d on d.week_number=cost.week_number
order by 2 desc)
,revenue as (
select 
s.customer_id, sum(c.total_cashflow) as collected_revenue
from ods_production.subscription_cashflow c 
left join ods_production.subscription s on c.subscription_id=s.subscription_id
group by 1
)
, summary as (
select  
 customer_acquisition_cohort::date as customer_acquisition_cohort, 
 coalesce(m.marketing_channel,'N/A') as  marketing_channel,
 store.store_short as store,
 sum(collected_revenue) as collected_revenue, 
 count(distinct a.customer_id) as customers,
   count(distinct case when c.customer_type = 'business_customer' then a.customer_id end) as acquired_b2b_customers,
 count(distinct case when c.customer_type = 'normal_customer' and fast_churn > 0 then a.customer_id end) as fast_churn_b2c_customers
from ods_production.customer_acquisition_cohort a 
left join ods_production.order_marketing_channel m on a.order_id=m.order_id
left join ods_production."order" o on o.order_id=a.order_id
left join ods_production.store store on store.id=o.store_id
left join ods_production.customer c on c.customer_id = a.customer_id
left join ods_production.customer_subscription_details sd on sd.customer_id = a.customer_id
left join revenue r on r.customer_Id=a.customer_id 
group by 1,2,3
)
select 
COALESCE(v.marketing_channel,c.marketing_channel,s.marketing_channel) as marketing_channel,
coalesce(v.week_number,c.week_number,to_Char(s.customer_acquisition_cohort,'YYYY-WW')) as week_number,
coalesce(v.store,c.store,s.store) as store,
coalesce(v.created_date,c.datum,s.customer_acquisition_cohort) as created_date,
coalesce(paid_new_orders,0) as paid_new_orders,
coalesce(paid_recurring_orders,0) as paid_recurring_orders,
coalesce(completed_new_orders,0) as completed_new_orders,
coalesce(completed_recurring_orders,0) as completed_recurring_orders,
coalesce(daily_cost_new_customers,0) as daily_cost_new_customers,
coalesce(daily_cost_recurring_customers,0) as daily_cost_recurring_customers,
coalesce(customers,0) as acquired_customers,
coalesce(collected_revenue,0) as collected_revenue,
coalesce(subscriptions,0) as subscriptions,
coalesce(completed_b2b_orders,0) as completed_b2b_orders,
coalesce(paid_b2b_orders,0) as paid_b2b_orders,
coalesce(paid_b2b_new_orders,0) as paid_b2b_new_orders,
coalesce(churned_subscriptions,0) as churned_subscriptions,
coalesce(acquired_b2b_customers,0) as acquired_b2b_customers,
coalesce(fast_churn_b2c_customers,0) as fast_churn_b2c_customers
from volume v 
full outer join cost c 
 on v.marketing_channel=c.marketing_channel
 and v.created_date=c.datum
 and v.store=c.store
full outer join summary s 
 on s.marketing_channel=COALESCE(v.marketing_channel,c.marketing_channel)
 and s.customer_acquisition_cohort = coalesce(v.created_date,c.datum)
 and s.store=coalesce(v.store,c.store)
 order by 3 desc;
 