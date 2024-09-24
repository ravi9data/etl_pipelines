drop table if exists dwh.crm_country_weekly_report;
create table dwh.crm_country_weekly_report as 
with a as (
select distinct 
"date",
DATE_TRUNC('week',"date") as week_date,
customer_id,
customer_type,
created_at,
customer_acquisition_cohort,
start_date_of_first_subscription,
case when first_subscription_store = 'Grover - USA old online' 
    or (created_at <= '2019-01-01' and shipping_country ='United States')
    then  'US old'
    else shipping_country end as shipping_country,
crm_label,
active_subscriptions,
email_subscribe
from master.customer_historical c 
left join public.dim_dates d 
 on d.datum::date=c."date"::date
where day_name = 'Sunday'
and "date" > CURRENT_DATE - 760 --limited to not increase the run time
)
,dc_churn_customers as
(
select customer_id, 
DATE_TRUNC('week',cancellation_date::date) as cancellation_date,
true as is_dc
from master.subscription
where cancellation_reason_new = 'DEBT COLLECTION'
group by 1,2
order by 2 desc 
)
,prep as (
select 
a.*, coalesce(is_dc,false) as is_dc,
lag(crm_label) over (partition by a.customer_id order by date) as previous_week_status,
lag(active_subscriptions) over (partition by a.customer_id order by date) as previous_week_active_subscriptions
from a
left join dc_churn_customers dc on dc.customer_id = a.customer_id 
     and a.week_date = dc.cancellation_date
)
,agg as (
select distinct 
date_trunc('week',date)::date as bow,
date::date as eow,
shipping_country,
customer_type,
----
count(distinct case when previous_week_status in ('Registered') then customer_id end) as registered_customers_bow,
count(distinct case when previous_week_status in ('Passive') then customer_id end) as Passive_customers_bow,
count(distinct case when previous_week_status in ('Active') then customer_id end) as Active_customers_bow,
count(distinct case when previous_week_status in ('Lapsed') then customer_id end) as Lapsed_customers_bow,
count(distinct case when previous_week_status in ('Inactive') then customer_id end) as Inactive_customers_bow,
count(distinct case when previous_week_status is null then customer_id end) as crm_label_na_bow,
----
count(distinct customer_id) as total_customers_eow,
count(distinct case when email_subscribe in ('Opted In') then customer_id end) as total_customers_subscribed_to_newsletter_eow,
count(distinct case when crm_label in ('Registered') then customer_id end) as registered_customers_eow,
count(distinct case when crm_label in ('Passive') then customer_id end) as Passive_customers_eow,
count(distinct case when crm_label in ('Active') then customer_id end) as Active_customers_eow,
count(distinct case when crm_label in ('Lapsed') then customer_id end) as Lapsed_customers_eow,
count(distinct case when crm_label in ('Inactive') then customer_id end) as Inactive_customers_eow,
count(distinct case when crm_label is null then customer_id end) as crm_label_na,
----
count(distinct case 
				when active_subscriptions>1 
				 then customer_id end
		) as Active_customers_w_multiple_subscriptions_eow,
---
count(distinct case 
				when active_subscriptions >previous_week_active_subscriptions 
				 and previous_week_active_subscriptions>0
				 then customer_id end
		) as upsell_customers,
---
---
count(distinct case 
				when crm_label in ('Active') 
				 and coalesce(previous_week_status,'Registered') in ('Registered') 
				then customer_id end
	) as activated_customers,
count(distinct case 
				when crm_label in ('Active') 
				 and previous_week_status in ('Inactive') 
				then customer_id end
	) as reactivated_customers,
count(distinct case 
				when crm_label in ('Active') 
				 and previous_week_status in ('Lapsed') 
				then customer_id end
	) as Winback_customers,
count(distinct case 
				when crm_label in ('Inactive') 
				 and previous_week_status in ('Active') 
				then customer_id end
	) as gross_churn_customers,
count(distinct case 
				when crm_label in ('Inactive') 
				 and previous_week_status in ('Active') 
				and is_dc is true
	then customer_id end
) as dc_customers
from prep
group by 1,2,3,4
)
--
,registered as (
select 
date_trunc('week',created_at)::date as fact_date,
customer_type,
case when first_subscription_store = 'Grover - USA old online' 
    or (created_at <= '2019-01-01' and shipping_country ='United States')
    then  'US old'
    else shipping_country end as shipping_country,
count(distinct customer_id) as registered_customers,
count(distinct case when email_subscribe = 'Opted In' then customer_id end) as subscribed_to_newsletter_at_signup,
count(distinct case when completed_orders>=1 then customer_id end) as customers_with_submitted_orders,
count(distinct case when start_date_of_first_subscription is not null then customer_id end) as customers_with_paid_orders
from master.customer c
group by 1,2, 3
order by 1 desc )
, switch as (
select 
 date_trunc('week',Date)::Date as fact_date, 
 customer_type,
 case when store_label = 'Grover - USA old online' 
    or (s.order_created_date <= '2019-01-01' and country_name ='United States')
    then  'US old'
    else country_name end as shipping_country,
 count(distinct s.customer_id) as switch_customers
from ods_production.subscription_plan_switching sw 
left join master.subscription s 
 on sw.subscription_id=s.subscription_id
group by 1,2,3)
---
select 
 r.fact_date,
 r.customer_type,
 r.shipping_country,
 r.registered_customers,
 r.subscribed_to_newsletter_at_signup,
 r.customers_with_submitted_orders,
 r.customers_with_paid_orders,
registered_customers_bow,
Passive_customers_bow,
Active_customers_bow,
Lapsed_customers_bow,
Inactive_customers_bow,
crm_label_na_bow,
----
total_customers_eow,
total_customers_subscribed_to_newsletter_eow,
registered_customers_eow,
Passive_customers_eow,
Active_customers_eow,
Lapsed_customers_eow,
Inactive_customers_eow,
crm_label_na,
----
Active_customers_w_multiple_subscriptions_eow,
---
upsell_customers,
---
---
activated_customers,
reactivated_customers,
Winback_customers,
gross_churn_customers,
dc_customers,
(gross_churn_customers - dc_customers) as customers_churn_without_dc,
switch_customers
from registered r 
left join agg 
 on r.fact_date=agg.bow and agg.customer_type = r.customer_type and agg.shipping_country = r.shipping_country 
 left join switch 
 on r.fact_date=switch.fact_Date and switch.customer_type = r.customer_type and switch.shipping_country = r.shipping_country
 order by r.fact_date DESC;