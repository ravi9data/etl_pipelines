drop table if exists dwh.reporting_churn_historical;
create table dwh.reporting_churn_historical as
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
 month_bom,
 month_eom,
 s.customer_type,
 count(distinct s.subscription_id) as active_subscriptions,
  count(distinct 
  case 
   when s.cancellation_reason_churn <> 'failed delivery' 
    and last_valid_payment_category like ('%DEFAULT%') 
    and last_valid_payment_category not like ('%RECOVERY%') 
    and default_date < fact_date 
   then subscription_id 
  end) as active_default_subscriptions,
 count(distinct s.customer_id) as active_customers,
   count(distinct 
  case 
    when s.cancellation_reason_churn <> 'failed delivery' 
   and last_valid_payment_category like ('%DEFAULT%') 
    and last_valid_payment_category not like ('%RECOVERY%') 
    and default_date < fact_date 
   then customer_id 
  end) as active_default_customers,
 sum(s.subscription_value) as active_subscription_value,
    sum( 
  case 
    when s.cancellation_reason_churn <> 'failed delivery' 
   and last_valid_payment_category like ('%DEFAULT%') 
    and last_valid_payment_category not like ('%RECOVERY%') 
    and default_date < fact_date 
   then s.subscription_value 
  end) as active_default_subscription_value,
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
s.subscription_id,s.store_label, s.store_commercial, s.customer_id,s.customer_type, s.subscription_value, 
s.start_date, s.cancellation_date, s.cancellation_reason_churn, s.last_valid_payment_category, s.next_due_date+31 as default_date
 from master.subscription_historical s
 where date='2020-08-31'
 ) s 
on d.fact_date::date >= s.start_date::date and
  d.fact_date::date < coalesce(s.cancellation_date::date, d.fact_date::date+1)
group by 1,2,3,4,5
order by 1 desc
)
,payments as (
select 
sp.paid_date::date as paid_date,
sp.customer_type,
sum(sp.amount_paid) as collected_subscription_revenue
from master.subscription_payment_historical sp 
left join master.subscription_historical s 
 on sp.subscription_id=s.subscription_id 
 and s.date=sp.date
where sp.date='2020-08-31'
group by 1,2
order by 1 desc
)
,acquisition as (
select 
s.start_date::date as start_date, 
customer_type,
count( s.subscription_id) as new_subscriptions,
count( distinct s.order_id) as new_orders,
sum(s.subscription_value) as acquired_subscription_value,
sum(s.committed_sub_value + COALESCE(s.additional_committed_sub_value,0)) as acquired_committed_sub_value,
count(case when s.retention_group ='RECURRING, UPSELL' then s.subscription_id end) as upsell_subscriptions,
count(case when s.retention_group ='RECURRING, REACTIVATION' then s.subscription_id end) as reactivation_subscriptions,
sum(case when s.new_recurring='NEW' then s.subscription_value end) as new_subscription_value,
sum(case when s.retention_group ='RECURRING, UPSELL' then s.subscription_value end) as upsell_subscription_value,
sum(case when s.retention_group ='RECURRING, REACTIVATION' then s.subscription_value end) as reactivation_subscription_value,
count(case when s.cancellation_reason_churn = 'failed delivery' and s.new_recurring='NEW' then s.subscription_id end) as new_subscriptions_failed_delivery,
count(case when s.cancellation_reason_churn = 'failed delivery' and s.retention_group ='RECURRING, UPSELL' then s.subscription_id end) as upsell_subscriptions_failed_delivery,
count(case when s.cancellation_reason_churn = 'failed delivery' and s.retention_group ='RECURRING, REACTIVATION' then s.subscription_id end) as reactivation_subscriptions_failed_delivery,
sum(case when s.cancellation_reason_churn = 'failed delivery' and s.new_recurring='NEW' then s.subscription_value end) as new_subscription_value_failed_delivery,
sum(case when s.cancellation_reason_churn = 'failed delivery' and s.retention_group ='RECURRING, UPSELL' then s.subscription_value end) as upsell_subscription_value_failed_delivery,
sum(case when s.cancellation_reason_churn = 'failed delivery' and s.retention_group ='RECURRING, REACTIVATION' then s.subscription_value end) as reactivation_subscription_value_failed_delivery
from master.subscription_historical s
where s.date='2020-08-31' 
group by 1,2
)
,churn as (
select 
s.cancellation_date::date as cancellation_date,
s.customer_type,
count(s.subscription_id) as cancelled_subscriptions,
sum(s.subscription_value) as cancelled_subscription_value,
count(case when s.cancellation_reason_churn = 'failed delivery' then s.subscription_id end) as cancelled_subscriptions_failed_delivery,
sum(case when s.cancellation_reason_churn = 'failed delivery' then s.subscription_value end) as cancelled_subscription_value_failed_delivery,
count(case when s.cancellation_reason_churn = 'customer request' then s.subscription_id end) as cancelled_subscriptions_customer_request,
sum(case when s.cancellation_reason_churn = 'customer request' then s.subscription_value end) as cancelled_subscription_value_customer_request
from master.subscription_historical s
where s.date='2020-08-31' 
group by 1,2
)
,switch as (
select
date::date as report_date,
cu.customer_type,
count(distinct sw.subscription_id) as switch_subscriptions,
sum(delta_in_sub_value * -1) as switch_subscription_value
from ods_production.subscription_plan_switching sw 
left join ods_production.subscription s 
 on sw.subscription_id = s.subscription_id
 left join master.customer cu 
 on s.customer_id = cu.customer_id
 where date::date <= '2020-08-31'
group by 1,2 )
,historical as (
select 
 date as reporting_date, 
 s.customer_type,
sum(case when status='ACTIVE' then (committed_sub_value + s.additional_committed_sub_value) end) as committed_sub_value,
sum(case when status='ACTIVE' then commited_sub_revenue_future end) as commited_sub_revenue_future
from master.subscription_historical s 
group by 1,2
order by 1 desc
),
commercial_targets as 
( select    datum,
			case when store = 'B2B' then 'business_customer' else 'normal_customer' end as customer_type,
			 sum(t.active_sub_value_daily_target) as active_sub_value_daily_target,
			sum(t.incrementalsubsvalue_daily) as incrementalsubsvalue_daily,
			sum(t.cancelled_subvalue_daily) as cancelled_subvalue_daily,
			sum(t.acquired_subvalue_daily) as acquired_subvalue_daily
			from dwh.commercial_targets_daily_store_commercial t
	 group by 1,2)
select distinct
 l.date, 
 l.fact_date,
 l.month_bom,
 l.month_eom,
 l.customer_type,
 coalesce(active_customers,0) as active_customers,
 coalesce(active_default_customers,0) as active_default_customers,
 coalesce(active_customers_failed_delivery,0) as active_customers_failed_delivery,
 --active subs
 coalesce(active_subscriptions,0) as active_subscriptions,
 coalesce(active_default_subscriptions,0) as active_default_subscriptions,
 coalesce(active_subscriptions_failed_delivery,0) as active_subscriptions_failed_delivery,
 coalesce(active_subscription_value,0) as active_subscription_value,
 coalesce(active_default_subscription_value,0) as active_default_subscription_value,
 coalesce(active_subscription_value_failed_delivery,0) as active_subscription_value_failed_delivery,
 --new subs
coalesce(acquired_subscription_value,0) as acquired_subscription_value,
coalesce(acquired_committed_sub_value,0) as acquired_committed_sub_value,
--
coalesce(new_orders,0) as new_orders,
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
 coalesce(switch_subscriptions,0) as switch_subscriptions,
 coalesce(switch_subscription_value,0) as switch_subscription_value,
 --targets
 t.active_sub_value_daily_target,
 t.incrementalsubsvalue_daily,
 t.cancelled_subvalue_daily,
 t.acquired_subvalue_daily,
 d.day_is_first_of_month,
 d.day_is_last_of_month,
 coalesce(h.committed_sub_value,0) as committed_sub_value,
 coalesce(h.commited_sub_revenue_future,0) as commited_sub_revenue_future,
 coalesce(p.collected_subscription_revenue,0) as collected_subscription_revenue
from active_subs l
left join acquisition a 
 on l.fact_date::date=a.start_date::date  and l.customer_type = a.customer_type
left join churn c 
 on l.fact_date::date=c.cancellation_date::date and l.customer_type = c.customer_type
left join commercial_targets t
  on t.datum::date=l.fact_date::date and l.customer_type = t.customer_type
left join public.dim_dates d 
 on d.datum::date=l.fact_date::date 
left join historical h 
 on h.reporting_date::date=l.fact_date::date and  l.customer_type = h.customer_type
left join payments p 
 on p.paid_date::date=l.fact_Date::date and  l.customer_type = p.customer_type
 left join switch s
 on s.report_date::date=l.fact_date::date and  l.customer_type = s.customer_type
order by l.fact_date DESC;

GRANT SELECT ON dwh.reporting_churn_historical TO tableau;
