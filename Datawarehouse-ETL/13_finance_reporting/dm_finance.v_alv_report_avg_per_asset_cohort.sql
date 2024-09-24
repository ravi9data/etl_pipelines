--ALV_report_avg_per_asset_cohort
drop view if exists dm_finance.v_alv_report_avg_asv_asset_cohort;
create view dm_finance.v_alv_report_avg_asv_asset_cohort as 
WITH FACT_DAYS AS (
SELECT
DISTINCT DATUM AS FACT_DAY
FROM public.dim_dates
WHERE DATUM <= CURRENT_DATE
)
select 
fact_day,
day_is_last_of_month,
date_trunc('month',ass.purchased_date) as asset_cohort,
COALESCE(sum(s.subscription_value),0) as active_subscription_value,
COALESCE(avg(s.subscription_value),0) as avg_active_subscription_value,
COALESCE(sum(s.avg_asset_purchase_price),0) as active_asset_purchase_price,
coalesce(count(distinct s.subscription_id),0) as active_subscriptions
from fact_days f
left join master.subscription as s
 on f.fact_day::date >= s.start_date::date and
  F.fact_day::date < coalesce(s.cancellation_date::date, f.fact_day::date+1)
left join master.allocation a on a.subscription_id=s.subscription_id
left join master.asset ass on ass.asset_id=a.asset_id
left join public.dim_dates dd
 on dd.DATUM=f.fact_day
group by 1,2,3
with no schema binding;