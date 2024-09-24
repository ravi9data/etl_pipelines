drop table if exists tgt_dev.clv;

create table tgt_dev.clv as  
with d as (select distinct date_trunc('month',datum::timestamp) as cohort , s.store_short as store
from public.dim_dates, (select distinct store_short from ods_production.store) s 
where date_trunc('month',datum::timestamp) <= date_trunc('month',CURRENT_TIMESTAMP)
and date_trunc('month',datum::timestamp) >= '2015-01-01'::date
order by 1 asc)
,a as (
select 
 a.customer_acquisition_cohort, --customer_acquisition_cohort
 coalesce(store.store_short,'N/A') as store,
 o.customer_id, --"order"
 pa.paid_date::date as paid_date, --payment_all
 case 
  when datediff('month',a.customer_acquisition_cohort::timestamp,pa.paid_date::timestamp) <= 0  
   then 0 
    else datediff('month',a.customer_acquisition_cohort::timestamp,pa.paid_date::timestamp) 
 end as months_since_acquisition,
 case when c.customer_type = 'business_customer' then pa.amount_paid end as amount_paid_b2b,
 case when c.customer_type = 'normal_customer' and fast_churn > 0  then pa.amount_paid end as amount_paid_1m_churn,
 pa.amount_paid as amount_paid,
 s.fast_churn,
 c.customer_type
from ods_production.payment_all pa 
left join ods_production."order" o 
 on pa.order_id=o.order_id
left join ods_production.store store on store.id=o.store_id
left join ods_production.customer c 
 on c.customer_id=o.customer_id
left join ods_production.customer_acquisition_cohort a
 on a.customer_id=o.customer_id
left join ods_production.order_marketing_channel m 
 on m.order_id=a.order_id
left join ods_production.customer_subscription_details s 
 on s.customer_Id=c.customer_id
where pa.paid_date is not null
 and a.customer_acquisition_cohort is not null
order by a.customer_acquisition_cohort desc
)
,agg as (
SELECT
date_trunc('month',customer_acquisition_cohort) as customer_acquisition_cohort,
a.store,
case 
when a.months_since_acquisition <= 6 then a.months_since_acquisition 
when a.months_since_acquisition >6 and a.months_since_acquisition <=9 then 9
when a.months_since_acquisition >9 and a.months_since_acquisition <=12 then 12
when a.months_since_acquisition >12 and a.months_since_acquisition <=18 then 18
when a.months_since_acquisition >18 and a.months_since_acquisition <=24 then 24
when a.months_since_acquisition >24 then 36 end as months_since_acquisition,
sum(coalesce(a.amount_paid,0)) as collected_revenue,
sum(coalesce(a.amount_paid_1m_churn,0)) as collected_revenue_1m_churn,
sum(coalesce(a.amount_paid_b2b,0)) as collected_revenue_b2b
from a 
group by 1,2,3
order by 1,2
)
,cust as (
select  
 date_trunc('month',customer_acquisition_cohort::date) as customer_acquisition_cohort, 
 coalesce(store_short,'N/A') as store,
 count(distinct a.customer_id) as acquired_customers,
 count(distinct case when c.customer_type = 'business_customer' then a.customer_id end) as acquired_b2b_customers,
 count(distinct case when c.customer_type = 'normal_customer' and fast_churn > 0 then a.customer_id end) as acquired_b2c_churn
from ods_production.customer_acquisition_cohort a 
left join ods_production.order_marketing_channel m on a.order_id=m.order_id
left join ods_production."order" o on o.order_id=a.order_id
left join ods_production.store store on store.id=o.store_id
left join ods_production.customer c on c.customer_id = a.customer_id
left join ods_production.customer_subscription_details sd on sd.customer_id = a.customer_id
group by 1,2
)
select 
d.cohort,
d.store,
a.months_since_acquisition,
a.collected_revenue,
a.collected_revenue_1m_churn,
collected_revenue_b2b,
acquired_customers as  acquired_customers,
acquired_b2b_customers,
acquired_b2c_churn
from d 
left join agg a 
 on d.store=a.store
 and d.cohort=a.customer_acquisition_cohort
left join cust 
 on d.cohort=cust.customer_acquisition_cohort
 and d.store=cust.store
;