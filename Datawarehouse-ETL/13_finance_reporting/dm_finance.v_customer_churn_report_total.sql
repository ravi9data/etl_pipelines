---customer_churn_report_total
drop view if exists dm_finance.v_customer_churn_report_total;
create view dm_finance.v_customer_churn_report_total as
with dates as (
select distinct 
 datum as fact_date, 
 date_trunc('month',datum)::DATE as month_bom,
 day_is_last_of_month,
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
 s.customer_type,
 customer_id,
 count(*) as active_subscriptions,
 sum(s.subscription_value) as active_subscription_value
  from dates d
left join (select s.subscription_id,s.store_label, s.store_commercial, s.customer_id,s.customer_type, s.subscription_value, s.start_date, s.cancellation_date, c.cancellation_reason_churn,s.rental_period
 from master.subscription s 
 left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
 where rental_period > 1 ---EXCLUDE all one month subscriptions
 ) s 
on d.fact_date::date >= s.start_date::date and
  d.fact_date::date < coalesce(s.cancellation_date::date, d.fact_date::date+1)
  where (day_is_last_of_month or fact_date = DATEADD('day',-1,DATE_TRUNC('day',CURRENT_DATE)))
group by 1,2,3,4,5
order by 1 desc
)
,total_subs as (
select 
 fact_date,
 month_bom,
 month_eom,
 s.customer_type,
 customer_id, 
 count(*) as total_subscriptions,
 sum(s.subscription_value) as total_subscription_value
  from dates d
left join (select s.subscription_id,s.store_label, s.store_commercial, s.customer_id,s.customer_type, s.subscription_value, s.start_date, s.cancellation_date, c.cancellation_reason_churn
 from master.subscription s 
 left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
 where rental_period > 1 ---EXCLUDE all one month subscriptions
 ) s 
on d.fact_date::date >= s.start_date::date
 where (day_is_last_of_month or fact_date = DATEADD('day',-1,DATE_TRUNC('day',CURRENT_DATE)))
group by 1,2,3,4,5
order by 1 desc
)
,prep as (
select 
	ts.fact_date,
	ts.month_bom,
	ts.month_eom,
	ts.customer_type,
	ts.customer_id,
	ts.total_subscriptions,
	ts.total_subscription_value,
	coalesce(a.active_subscriptions,0) as active_subscriptions,
	coalesce(a.active_subscription_value,0) as active_subscription_value
	from total_subs ts 
left join active_subs a on ts.fact_date = a.fact_date and ts.customeR_id = a.customeR_id 
order by 1 desc
	)
	,dc_churn_customers as
(
select customer_id, DATE_TRUNC('month',cancellation_date::date) as cancellation_date,
true as is_dc
from master.subscription
where cancellation_reason_new = 'DEBT COLLECTION'
and rental_period > 1 ---EXCLUDE all one month subscriptions
group by 1,2
order by 2 desc
)
,prep_b as (
  select p.*,case when is_dc is null then false else is_dc end as is_dc,
    lag(active_subscriptions) over (partition by p.customer_id order by fact_date) as previous_month_active_subscriptions,
	lag(total_subscriptions) over (partition by p.customer_id order by fact_date) as previous_month_total_subscriptions
from prep p 
   left join dc_churn_customers dc on dc.cancellation_date = month_bom and dc.customer_id = p.customer_id
)
	, agg as (
select distinct 
 month_bom as bom,
month_eom as eom,
(case when (DATEADD('day',-1,DATE_TRUNC('day',CURRENT_DATE))) = fact_date then 1 else 0 end) as month_to_date,
customer_type,
----
count(distinct case when previous_month_active_subscriptions > 0 then customer_id end) as Active_customers_bom,
----
count(distinct case when active_subscriptions > 0 then customer_id end) as Active_customers_eom,
count(distinct case 
				when active_subscriptions >previous_month_active_subscriptions 
				 and previous_month_active_subscriptions>0
				 then customer_id end
		) as upsell_customers,
count(distinct case 
				when active_subscriptions >0 
				 and  (previous_month_total_subscriptions=0 or previous_month_total_subscriptions  is null)
				 then customer_id end
		) as acquired_customers,
---
count(distinct case 
				when active_subscriptions > 0
				 and previous_month_active_subscriptions = 0 
				 and previous_month_total_subscriptions > 0
				then customer_id end
	) as reactivated_customers,
count(distinct case 
				when active_subscriptions = 0 
				 and previous_month_active_subscriptions > 0
				then customer_id end
	) as gross_churn_customers,
count(distinct case 
				when active_subscriptions = 0 
				 and previous_month_active_subscriptions > 0
				and is_dc is true
	then customer_id end
) as dc_customers	
from prep_b
group by 1,2,3,4
)
,active_subs_all as (
select 
 fact_date,
 month_bom,
 month_eom,
 s.customer_type,
 customer_id,
 count(*) as active_subscriptions,
 sum(s.subscription_value) as active_subscription_value
  from dates d
left join (select s.subscription_id,s.store_label, s.store_commercial, s.customer_id,s.customer_type, s.subscription_value, s.start_date, s.cancellation_date, c.cancellation_reason_churn,s.rental_period
 from master.subscription s 
 left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
 --where rental_period > 1 ---EXCLUDE all one month subscriptions
 ) s 
on d.fact_date::date >= s.start_date::date and
  d.fact_date::date < coalesce(s.cancellation_date::date, d.fact_date::date+1)
  where (day_is_last_of_month or fact_date = DATEADD('day',-1,DATE_TRUNC('day',CURRENT_DATE)))
group by 1,2,3,4,5
order by 1 desc
)
,total_subs_all as (
select 
 fact_date,
 month_bom,
 month_eom,
 s.customer_type,
 customer_id, 
 count(*) as total_subscriptions,
 sum(s.subscription_value) as total_subscription_value
  from dates d
left join (select s.subscription_id,s.store_label, s.store_commercial, s.customer_id,s.customer_type, s.subscription_value, s.start_date, s.cancellation_date, c.cancellation_reason_churn
 from master.subscription s 
 left join ods_production.subscription_cancellation_reason c 
 on s.subscription_id=c.subscription_id
 --where rental_period > 1 ---EXCLUDE all one month subscriptions
 ) s 
on d.fact_date::date >= s.start_date::date
 where (day_is_last_of_month or fact_date = DATEADD('day',-1,DATE_TRUNC('day',CURRENT_DATE)))
group by 1,2,3,4,5
order by 1 desc
)
,prep_all as (
select 
	ts.fact_date,
	ts.month_bom,
	ts.month_eom,
	ts.customer_type,
	ts.customer_id,
	ts.total_subscriptions,
	ts.total_subscription_value,
	coalesce(a.active_subscriptions,0) as active_subscriptions,
	coalesce(a.active_subscription_value,0) as active_subscription_value
	from total_subs_all ts 
left join active_subs_all a on ts.fact_date = a.fact_date and ts.customeR_id = a.customeR_id 
order by 1 desc
	)
	,dc_churn_customers_all as
(
select customer_id, DATE_TRUNC('month',cancellation_date::date) as cancellation_date,
true as is_dc
from master.subscription
where cancellation_reason_new = 'DEBT COLLECTION'
--and rental_period > 1 ---EXCLUDE all one month subscriptions
group by 1,2
order by 2 desc
)
,prep_b_all as (
  select p.*,case when is_dc is null then false else is_dc end as is_dc,
    lag(active_subscriptions) over (partition by p.customer_id order by fact_date) as previous_month_active_subscriptions,
	lag(total_subscriptions) over (partition by p.customer_id order by fact_date) as previous_month_total_subscriptions
from prep_all p 
   left join dc_churn_customers dc on dc.cancellation_date = month_bom and dc.customer_id = p.customer_id
)
	, agg_all as (
select distinct 
 month_bom as bom,
month_eom as eom,
(case when (DATEADD('day',-1,DATE_TRUNC('day',CURRENT_DATE))) = fact_date then 1 else 0 end) as month_to_date,
customer_type,
----
count(distinct case when previous_month_active_subscriptions > 0 then customer_id end) as Active_customers_bom,
----
count(distinct case when active_subscriptions > 0 then customer_id end) as Active_customers_eom,
count(distinct case 
				when active_subscriptions >previous_month_active_subscriptions 
				 and previous_month_active_subscriptions>0
				 then customer_id end
		) as upsell_customers,
count(distinct case 
				when active_subscriptions >0 
				 and (previous_month_total_subscriptions=0 or previous_month_total_subscriptions  is null)
				 then customer_id end
		) as acquired_customers,
---
count(distinct case 
				when active_subscriptions > 0
				 and previous_month_active_subscriptions = 0 
				 and previous_month_total_subscriptions > 0
				then customer_id end
	) as reactivated_customers,
count(distinct case 
				when active_subscriptions = 0 
				 and previous_month_active_subscriptions > 0
				then customer_id end
	) as gross_churn_customers,
count(distinct case 
				when active_subscriptions = 0 
				 and previous_month_active_subscriptions > 0
				and is_dc is true
	then customer_id end
) as dc_customers	
from prep_b_all
group by 1,2,3,4
)
select *, 'EXCLUDE' as one_month_rental from agg
union all 
select *, 'INCLUDE' as one_month_rental from agg_all
with no schema binding;