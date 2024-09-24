drop table if exists dwh.reporting_churn_store_commercial;
create table dwh.reporting_churn_store_commercial as
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
 case 
 when s.store_commercial like ('%B2B%') 
  then 'B2B-Total'
 when s.store_commercial like ('%Partnerships%') 
  then 'Partnerships-Total'
 when s.store_commercial = 'Grover Germany'
  then 'Grover-DE'
 when s.store_label in ('Grover - UK online')
  then 'Grover-UK'
   when s.store_label in ('Grover - Netherlands online')
  then 'Grover-NL'
   when s.store_label in ('Grover - Austria online')
  then 'Grover-AT'
   when s.store_label in ('Grover - Spain online')
  then 'Grover-ES'
 when s.store_label in ('Grover - USA old online')
  then 'Grover-US old'
  when s.store_label in ('Grover - United States online')
  then 'Grover-US'
 else 'Grover-DE'
 end as store_label,
 month_bom,
 month_eom,
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
left join (select s.subscription_id,s.store_label, s.store_commercial, s.customer_id, s.subscription_value, s.start_date, s.cancellation_date, c.cancellation_reason_churn, cf.last_valid_payment_category, cf.default_date+31 as default_date
 from ods_production.subscription s 
 left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
 left join ods_production.subscription_cashflow cf 
 on s.subscription_id=cf.subscription_id
 ) s 
on d.fact_date::date >= s.start_date::date and
  d.fact_date::date < coalesce(s.cancellation_date::date, d.fact_date::date+1)
group by 1,2,3,4
order by 1 desc
)
,payments as (
select 
sp.paid_date::date as paid_date,
 case 
 when s.store_commercial like ('%B2B%') 
  then 'B2B-Total'
 when s.store_commercial like ('%Partnerships%') 
  then 'Partnerships-Total'
 when s.store_commercial = 'Grover Germany'
  then 'Grover-DE'
 when s.store_label in ('Grover - UK online')
  then 'Grover-UK'
   when s.store_label in ('Grover - Netherlands online')
  then 'Grover-NL'
   when s.store_label in ('Grover - Austria online')
  then 'Grover-AT'
   when s.store_label in ('Grover - Spain online')
  then 'Grover-ES'
 when s.store_label in ('Grover - USA old online')
  then 'Grover-US old'
  when s.store_label in ('Grover - United States online')
  then 'Grover-US'
 else 'Grover-DE'
 end as store_label,
sum(sp.amount_paid) as collected_subscription_revenue
from master.subscription_payment sp 
left join master.subscription s 
 on sp.subscription_id=s.subscription_id 
group by 1,2
order by 1 desc
)
,acquisition as (
select 
s.start_date::date as start_date, 
 case 
 when s.store_commercial like ('%B2B%') 
  then 'B2B-Total'
 when s.store_commercial like ('%Partnerships%') 
  then 'Partnerships-Total'
 when s.store_commercial = 'Grover Germany'
  then 'Grover-DE'
 when s.store_label in ('Grover - UK online')
  then 'Grover-UK'
   when s.store_label in ('Grover - Netherlands online')
  then 'Grover-NL'
   when s.store_label in ('Grover - Austria online')
  then 'Grover-AT'
   when s.store_label in ('Grover - Spain online')
  then 'Grover-ES'
 when s.store_label in ('Grover - USA old online')
  then 'Grover-US old'
  when s.store_label in ('Grover - United States online')
  then 'Grover-US'
 else 'Grover-DE'
 end as store_label,
count( s.subscription_id) as new_subscriptions,
count( distinct s.order_id) as new_orders,
sum(s.subscription_value) as acquired_subscription_value,
sum(s.committed_sub_value) as acquired_committed_sub_value,
count(case when retention_group ='RECURRING, UPSELL' then s.subscription_id end) as upsell_subscriptions,
count(case when retention_group ='RECURRING, REACTIVATION' then s.subscription_id end) as reactivation_subscriptions,
sum(case when new_recurring='NEW' then s.subscription_value end) as new_subscription_value,
sum(case when retention_group ='RECURRING, UPSELL' then s.subscription_value end) as upsell_subscription_value,
sum(case when retention_group ='RECURRING, REACTIVATION' then s.subscription_value end) as reactivation_subscription_value,
count(case when c.cancellation_reason_churn = 'failed delivery' and new_recurring='NEW' then s.subscription_id end) as new_subscriptions_failed_delivery,
count(case when c.cancellation_reason_churn = 'failed delivery' and retention_group ='RECURRING, UPSELL' then s.subscription_id end) as upsell_subscriptions_failed_delivery,
count(case when c.cancellation_reason_churn = 'failed delivery' and retention_group ='RECURRING, REACTIVATION' then s.subscription_id end) as reactivation_subscriptions_failed_delivery,
sum(case when c.cancellation_reason_churn = 'failed delivery' and new_recurring='NEW' then s.subscription_value end) as new_subscription_value_failed_delivery,
sum(case when c.cancellation_reason_churn = 'failed delivery' and retention_group ='RECURRING, UPSELL' then s.subscription_value end) as upsell_subscription_value_failed_delivery,
sum(case when c.cancellation_reason_churn = 'failed delivery' and retention_group ='RECURRING, REACTIVATION' then s.subscription_value end) as reactivation_subscription_value_failed_delivery
from ods_production.subscription s 
left join ods_production.order_retention_group o 
 on o.order_id=s.order_id
left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
group by 1,2
)
,churn as (
select 
s.cancellation_date::date as cancellation_date,
 case 
 when s.store_commercial like ('%B2B%') 
  then 'B2B-Total'
 when s.store_commercial like ('%Partnerships%') 
  then 'Partnerships-Total'
 when s.store_commercial = 'Grover Germany'
  then 'Grover-DE'
 when s.store_label in ('Grover - UK online')
  then 'Grover-UK'
   when s.store_label in ('Grover - Netherlands online')
  then 'Grover-NL'
   when s.store_label in ('Grover - Austria online')
  then 'Grover-AT'
   when s.store_label in ('Grover - Spain online')
  then 'Grover-ES'
 when s.store_label in ('Grover - USA old online')
  then 'Grover-US old'
  when s.store_label in ('Grover - United States online')
  then 'Grover-US'
 else 'Grover-DE'
 end as store_label,
count(s.subscription_id) as cancelled_subscriptions,
sum(s.subscription_value) as cancelled_subscription_value,
count(case when c.cancellation_reason_churn = 'failed delivery' then s.subscription_id end) as cancelled_subscriptions_failed_delivery,
sum(case when c.cancellation_reason_churn = 'failed delivery' then s.subscription_value end) as cancelled_subscription_value_failed_delivery,
count(case when c.cancellation_reason_churn = 'customer request' then s.subscription_id end) as cancelled_subscriptions_customer_request,
sum(case when c.cancellation_reason_churn = 'customer request' then s.subscription_value end) as cancelled_subscription_value_customer_request
from ods_production.subscription s
left join ods_production.subscription_cancellation_reason c on s.subscription_id=c.subscription_id
group by 1,2)
,hisotrical as (
select 
 date as reporting_date, 
 case 
 when s.store_commercial like ('%B2B%') 
  then 'B2B-Total'
 when s.store_commercial like ('%Partnerships%') 
  then 'Partnerships-Total'
 when s.store_commercial = 'Grover Germany'
  then 'Grover-DE'
 when s.store_label in ('Grover - UK online')
  then 'Grover-UK'
   when s.store_label in ('Grover - Netherlands online')
  then 'Grover-NL'
   when s.store_label in ('Grover - Austria online')
  then 'Grover-AT'
   when s.store_label in ('Grover - Spain online')
  then 'Grover-ES'
 when s.store_label in ('Grover - USA old online')
  then 'Grover-US old'
  when s.store_label in ('Grover - United States online')
  then 'Grover-US'
 else 'Grover-DE'
 end as store_label,
sum(case when status='ACTIVE' then (committed_sub_value + COALESCE(s.additional_committed_sub_value,0)) end) as committed_sub_value,
sum(case when status='ACTIVE' then commited_sub_revenue_future end) as commited_sub_revenue_future
from master.subscription_historical s 
group by 1,2
order by 1 desc
)
select distinct 
 l.fact_date,
 l.store_label,
 l.month_bom,
 l.month_eom,
 --active customers
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
 on l.fact_date::date=a.start_date::date and l.store_label=a.store_label
left join churn c 
 on l.fact_date::date=c.cancellation_date::date and l.store_label=c.store_label
left join dwh.commercial_targets_daily_store_commercial t 
  on t.datum::date=l.fact_date::date 
  and l.store_label=t.store_country
left join public.dim_dates d 
 on d.datum::date=l.fact_date::date
left join hisotrical h 
 on h.reporting_date::date=l.fact_date::date
 and l.store_label=h.store_label
left join payments p 
 on p.paid_date::date=l.fact_Date::date
 and p.store_label=l.store_label;

GRANT SELECT ON dwh.reporting_churn_store_commercial TO tableau;
