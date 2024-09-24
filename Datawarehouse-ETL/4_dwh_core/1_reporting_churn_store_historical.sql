drop table if exists dwh.reporting_churn_store_historical;
create table dwh.reporting_churn_store_historical as
with dates as (
select distinct 
 datum as fact_date, 
 date_trunc('month',datum)::DATE as month_bom,
 LEAST(DATE_TRUNC('MONTH',DATEADD('MONTH',1,DATUM))::DATE-1,CURRENT_DATE) AS MONTH_EOM
from public.dim_dates
where datum <= '2020-08-31'
ORDER BY 1 DESC 
)
,active_subs as (
select 
  date,
 fact_date,
 s.store_label,
 month_bom,
 month_eom,
 count(distinct s.subscription_id) as active_subscriptions,
 count(distinct s.customer_id) as active_customers,
 sum(s.subscription_value) as active_subscription_value,
 count(distinct case when s.cancellation_reason_churn = 'failed delivery' then s.subscription_id end) as active_subscriptions_failed_delivery,
 count(distinct case when s.cancellation_reason_churn = 'failed delivery' then s.customer_id end) as active_customers_failed_delivery,
 sum(case when s.cancellation_reason_churn = 'failed delivery' then s.subscription_value end) as active_subscription_value_failed_delivery
/*from dates d
left join (select s.subscription_id, s.customer_id, s.subscription_value, s.start_date, s.cancellation_date, c.cancellation_reason_churn, cf.last_valid_payment_category, cf.default_date+31 as default_date
 from ods_production.subscription s 
 left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
 left join ods_production.subscription_cashflow cf 
 on s.subscription_id=cf.subscription_id
 ) s
on d.fact_date between s.start_date and coalesce(s.cancellation_date, d.fact_date)*/
  from dates d
left join (
    select 
  s.date,
     s.subscription_id,
     s.store_label, 
     s.customer_id, 
     s.subscription_value, 
     s.start_date, 
     s.cancellation_date, 
     s.cancellation_reason_churn, 
     s.last_valid_payment_category
 from master.subscription_historical s
 where date='2020-08-31'
 ) s 
on d.fact_date::date >= s.start_date::date and
  d.fact_date::date < coalesce(s.cancellation_date::date, d.fact_date::date+1)
group by 1,2,3,4,5
order by 1 desc
)
,acquisition as (
select 
s.start_date::date as start_date, 
s.store_label,
count( s.subscription_id) as new_subscriptions,
count(case when retention_group ='RECURRING, UPSELL' then s.subscription_id end) as upsell_subscriptions,
count(case when retention_group ='RECURRING, REACTIVATION' then s.subscription_id end) as reactivation_subscriptions,
sum(case when new_recurring='NEW' then s.subscription_value end) as new_subscription_value,
sum(case when retention_group ='RECURRING, UPSELL' then s.subscription_value end) as upsell_subscription_value,
sum(case when retention_group ='RECURRING, REACTIVATION' then s.subscription_value end) as reactivation_subscription_value,
count(case when s.cancellation_reason_churn = 'failed delivery' and new_recurring='NEW' then s.subscription_id end) as new_subscriptions_failed_delivery,
count(case when s.cancellation_reason_churn = 'failed delivery' and retention_group ='RECURRING, UPSELL' then s.subscription_id end) as upsell_subscriptions_failed_delivery,
count(case when s.cancellation_reason_churn = 'failed delivery' and retention_group ='RECURRING, REACTIVATION' then s.subscription_id end) as reactivation_subscriptions_failed_delivery,
sum(case when s.cancellation_reason_churn = 'failed delivery' and new_recurring='NEW' then s.subscription_value end) as new_subscription_value_failed_delivery,
sum(case when s.cancellation_reason_churn = 'failed delivery' and retention_group ='RECURRING, UPSELL' then s.subscription_value end) as upsell_subscription_value_failed_delivery,
sum(case when s.cancellation_reason_churn = 'failed delivery' and retention_group ='RECURRING, REACTIVATION' then s.subscription_value end) as reactivation_subscription_value_failed_delivery
from master.subscription_historical s 
where date='2020-08-31'
group by 1,2
)
,churn as (
select 
s.cancellation_date::date as cancellation_date,
s.store_label,
count(s.subscription_id) as cancelled_subscriptions,
sum(s.subscription_value) as cancelled_subscription_value,
count(case when s.cancellation_reason_churn = 'failed delivery' then s.subscription_id end) as cancelled_subscriptions_failed_delivery,
sum(case when s.cancellation_reason_churn = 'failed delivery' then s.subscription_value end) as cancelled_subscription_value_failed_delivery,
count(case when s.cancellation_reason_churn = 'customer request' then s.subscription_id end) as cancelled_subscriptions_customer_request,
sum(case when s.cancellation_reason_churn = 'customer request' then s.subscription_value end) as cancelled_subscription_value_customer_request
from master.subscription_historical s
where date='2020-08-31'
group by 1,2)
,targets as (
select fact_date,
case when store_label = 'Grover-DE' or  store_label = 'B2B-Total' then 'Grover - Germany online'
	 else store_label 
	 end as store_label_grouped,
	 sum(active_sub_value_daily_target) as active_sub_value_daily_target
from dwh.reporting_churn_store_commercial
group by 1,2)
select distinct 
 l.date,
 l.fact_date,	
 l.store_label,
 l.month_bom,
 l.month_eom,
 --active customers
 coalesce(active_customers,0) as active_customers,
 coalesce(active_customers_failed_delivery,0) as active_customers_failed_delivery,
 --active subs
 coalesce(active_subscriptions,0) as active_subscriptions,
 coalesce(active_subscriptions_failed_delivery,0) as active_subscriptions_failed_delivery,
 coalesce(active_subscription_value,0) as active_subscription_value,
 coalesce(active_subscription_value_failed_delivery,0) as active_subscription_value_failed_delivery,
 --new subs
 coalesce(new_subscriptions,0) as new_subscriptions,
 coalesce(new_subscriptions_failed_delivery,0) as new_subscriptions_failed_delivery,
 coalesce(new_subscription_value,0) as new_subscription_value,
 coalesce(new_subscription_value_failed_delivery,0) as new_subscription_value_failed_delivery,
 --upsell
 coalesce(upsell_subscriptions,0) as upsell_subscriptions,
 coalesce(upsell_subscriptions_failed_delivery,0) as upsell_subscriptions_failed_delivery,
 coalesce(upsell_subscription_value,0) as upsell_subscription_value,
 coalesce(upsell_subscription_value_failed_delivery,0) as upsell_subscription_value_failed_delivery,
 -- reactivation
 coalesce(reactivation_subscriptions,0) as reactivation_subscriptions,
 coalesce(reactivation_subscriptions_failed_delivery,0) as reactivation_subscriptions_failed_delivery,
 coalesce(reactivation_subscription_value,0) as reactivation_subscription_value,
 coalesce(reactivation_subscription_value_failed_delivery,0) as reactivation_subscription_value_failed_delivery,
 -- cancelled subscriptions
 coalesce(cancelled_subscriptions,0) as cancelled_subscriptions,
 coalesce(cancelled_subscriptions_failed_delivery,0) as cancelled_subscriptions_failed_delivery,
 coalesce(cancelled_subscriptions_customer_request,0) as cancelled_subscriptions_customer_request,
 coalesce(cancelled_subscription_value,0) as cancelled_subscription_value,
 coalesce(cancelled_subscription_value_failed_delivery,0) as cancelled_subscription_value_failed_delivery,
 coalesce(cancelled_subscription_value_customer_request,0) as cancelled_subscription_value_customer_request,
 --sub switching
 t.active_sub_value_daily_target
from active_subs l
left join acquisition a 
 on l.fact_date::date=a.start_date::date and l.store_label=a.store_label
left join churn c 
 on l.fact_date::date=c.cancellation_date::date and l.store_label=c.store_label
left join targets t 
  on t.fact_date::date=l.fact_date::date and  l.store_label=t.store_label_grouped
 ;

GRANT SELECT ON dwh.reporting_churn_store_historical TO tableau;
