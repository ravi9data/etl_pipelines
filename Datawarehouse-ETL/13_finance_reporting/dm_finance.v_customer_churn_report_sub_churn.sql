--customer_churn_report_sub_churn
drop view if exists dm_finance.v_customer_churn_report_sub_churn;
create view dm_finance.v_customer_churn_report_sub_churn as 
with dates as (
select distinct 
 datum as fact_date, 
 date_trunc('month',datum)::DATE as month_bom,
 LEAST(DATE_TRUNC('MONTH',DATEADD('MONTH',1,DATUM))::DATE-1,CURRENT_DATE) AS MONTH_EOM
from public.dim_dates
where datum <= current_date
ORDER BY 1 DESC 
)
,active_subs as (
select 
 fact_date,
 month_bom,
 month_eom,
 s.country_name,
case when s.rental_period > 1 then true else false end as one_month_rental_exclude,
 s.customer_type,
 count(distinct s.subscription_id) as active_subscriptions
  from dates d
left join (select s.subscription_id,s.store_label, s.store_commercial,s.country_name,s.rental_period, s.customer_id,s.customer_type, s.subscription_value, s.start_date, s.cancellation_date, c.cancellation_reason_churn
 from master.subscription s 
 left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
 ) s 
on d.fact_date::date >= s.start_date::date and
  d.fact_date::date < coalesce(s.cancellation_date::date, d.fact_date::date+1)
group by 1,2,3,4,5,6
order by 1 desc
)
,eom_asv as 
(	
select fact_date,
month_bom, 
month_eom, 
country_name,
one_month_rental_exclude,
customer_type, 
lag(active_subscriptions) over (partition by customer_type,country_name,one_month_rental_exclude order by fact_date) as asv_bom,
active_subscriptions as asv_eom
from active_subs 
where fact_date = month_eom 
) 
,bom_asv as 
(
select * from active_subs 
where fact_date = month_bom 
)
,acquisition as (
select 
DATE_TRUNC('month',start_date) as subscription_month,
country_name,
case when s.rental_period > 1 then true else false end as one_month_rental_exclude,
customer_type,
count( s.subscription_id) as new_subscriptions,
count(case when o.retention_group ='RECURRING, UPSELL' then s.subscription_id end) as upsell_subscriptions,
count(case when o.retention_group ='RECURRING, REACTIVATION' then s.subscription_id end) as reactivation_subscriptions
from master.subscription s 
left join ods_production.order_retention_group o 
 on o.order_id=s.order_id
left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
group by 1,2,3,4
)
,churn as (
select 
date_trunc('month',s.cancellation_date) as cancellation_month,
country_name,
case when s.rental_period > 1 then true else false end as one_month_rental_exclude,
s.customer_type,
count(s.subscription_id) as cancelled_subscriptions,
from master.subscription s
left join ods_production.subscription_cancellation_reason c 
on s.subscription_id=c.subscription_id
group by 1,2,3,4
)
select ea.*,a.new_subscriptions,upsell_subscriptions,reactivation_subscriptions,cancelled_subscriptions,cancelled_subscriptions_dc 
	from eom_asv ea 
	left join acquisition a 
 on ea.month_bom = a.subscription_month and ea.customer_type = a.customer_type and ea.one_month_rental_exclude = a.one_month_rental_exclude and ea.country_name = a.country_name
    left join churn c 
 on ea.month_bom = c.cancellation_month and ea.customer_type = c.customer_type and ea.one_month_rental_exclude = c.one_month_rental_exclude and ea.country_name = c.country_name  
order by fact_date desc
with no schema binding;