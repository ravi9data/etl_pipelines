create or replace view dm_b2b.category_kpis as
WITH fact_days AS (
SELECT
DISTINCT datum AS FACT_DAY,
day_is_last_of_month,
CASE	
	 WHEN week_day_number = 7 
	  THEN 1
	 WHEN datum = CURRENT_DATE 
	  THEN 1
	 ELSE 0 
  END AS day_is_end_of_week,
category_name, 
subcategory_name,
coalesce(is_freelancer,0) as is_freelancer
FROM public.dim_dates, 
			(select 
					distinct category_name,  subcategory_name,  min(created_at::date) as created_at
					from
					ods_production.product p
					group by 1,2
					),
			(select 
					distinct fm.is_freelancer
					from dm_risk.b2b_freelancer_mapping fm
					)
WHERE DATUM <= CURRENT_DATE
and datum >= created_at
) 
,sub_value as (
select 
f.datum as fact_day,
s.category_name,
s.subcategory_name,
coalesce(is_freelancer,0) as is_freelancer,
sum(s.subscription_value_eur) as active_subscription_value,
sum(s.subscription_value_eur * s.rental_period) as committed_subscription_value,
count (distinct s.subscription_id) as active_subscriptions
from public.dim_dates f
left join ods_production.subscription_phase_mapping s 
 ON f.datum::date >= s.fact_day::date
		AND F.datum::date < COALESCE(s.end_date::date, f.datum::date + 1)
left join ods_b2b.account u on s.customer_id = u.customer_id 
left join dm_risk.b2b_freelancer_mapping fm on fm.company_type_name = u.account_type 
where s.customer_type ='business_customer' 
and  DATUM <= CURRENT_DATE and datum >= f.datum
group by 1, 2, 3, 4)
,new_subs AS (
select 
start_date::date as fact_date,
coalesce(is_freelancer,0) as is_freelancer,
category_name,
subcategory_name,
count(distinct subscription_Id) as acquired_subscriptions,
sum(s.subscription_value) as acquired_subscriptions_value,
sum(s.committed_sub_value + s.additional_committed_sub_value) AS committed_subscription_revenue
from master.subscription s
left join master.customer c on c.customer_id = s.customer_id 
left join dm_risk.b2b_freelancer_mapping fm on fm.company_type_name = c.company_type_name 
where s.customer_type = 'business_customer'
group by 1,2,3,4)
,cancellation AS (
SELECT 
 s.cancellation_date::date as fact_date,
 coalesce(is_freelancer,0) as is_freelancer,
 category_name,
 subcategory_name,
 COUNT(DISTINCT s.subscription_id) AS cancelled_subscriptions,
 SUM(s.subscription_value) AS cancelled_subscription_value,
 sum(s.committed_sub_value + s.additional_committed_sub_value) AS committed_subscription_revenue_cancelled
FROM master.subscription s
left join master.customer c on c.customer_id = s.customer_id 
left join dm_risk.b2b_freelancer_mapping fm on fm.company_type_name = c.company_type_name 
where s.customer_type = 'business_customer' and s.cancellation_date is not null
GROUP BY 1,2,3,4
)
, final_data as (	
Select distinct
	fd.fact_day,
	fd.day_is_last_of_month,
	fd.day_is_end_of_week,
	fd.category_name,
	fd.subcategory_name,
	fd.is_freelancer,
	coalesce(s.acquired_subscriptions,0) as acquired_subscriptions,
	coalesce(s.acquired_subscriptions_value,0) as acquired_subscriptions_value,
	coalesce(s.committed_subscription_revenue,0) as committed_subscription_revenue,
	coalesce(c.cancelled_subscriptions,0) as cancelled_subscriptions,
	coalesce(c.cancelled_subscription_value,0) as cancelled_subscription_value,
	coalesce(c.committed_subscription_revenue_cancelled,0) as committed_subscription_revenue_cancelled,
	coalesce(sv.active_subscription_value,0) as active_subscription_value,
	coalesce(sv.committed_subscription_value,0) as committed_subscription_value,
	coalesce(sv.active_subscriptions,0) as active_subscriptions
from fact_days fd 
LEFT JOIN new_subs s ON fd.fact_day = s.fact_date
 AND fd.category_name = s.category_name
 AND fd.subcategory_name = s.subcategory_name
 and fd.is_freelancer = s.is_freelancer 
LEFT JOIN cancellation c ON fd.fact_day = c.fact_date
 AND fd.category_name = c.category_name
 AND fd.subcategory_name = c.subcategory_name
 and fd.is_freelancer = c.is_freelancer 
LEFT JOIN sub_value sv ON fd.fact_day = sv.fact_day
 AND fd.category_name = sv.category_name
 AND fd.subcategory_name = sv.subcategory_name
 and fd.is_freelancer = sv.is_freelancer )
select 
fd.*
from final_data fd
order by 1 desc
WITH NO SCHEMA BINDING;