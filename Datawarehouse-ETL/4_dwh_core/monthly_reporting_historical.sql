--Table created for Monthly Reporting
delete from dwh.monthly_reporting_historical where  day_is_last_of_month = 0 or (date = (CURRENT_DATE -1) and day_is_last_of_month = 0);
insert into dwh.monthly_reporting_historical 

--getting only the eom dpd info by subscription 
with eom as(
select
distinct
date_trunc('month', s.date::date)::date as bom,
date as eom,
s.subscription_id,
lag(dpd) over (partition by subscription_id order by date) as previous_dpd,
lag(returned_packages) over (partition by subscription_id order by date) as previous_returns,
lag(outstanding_asset_value) over (partition by subscription_id order by date) as prev_asset_value
from master.subscription_historical s  
left join public.dim_dates d on s.date=d.datum
where (d.day_is_last_of_month = 1 or date = current_date - 1)
order by 1 desc
)
select
distinct 
date_trunc('month', s.date::date)::date as bom,
s.date::date,
s.customer_id,
s.subscription_id,
s.start_date,
s.customer_type,
s.order_id,
s.status,
s.returned_packages,
s.outstanding_residual_asset_value,
s.rental_period,
s.subscription_value,
s.last_valid_payment_category,
s.dpd,
s.cancellation_date,
s.cancellation_reason_new,
s.cancellation_reason_churn,
s.store_commercial,
d.day_is_last_of_month,
e.previous_dpd as end_of_last_month_dpd,
e.previous_returns as end_of_last_month_returns,
e.prev_asset_value as end_of_last_month_asset_value
from master.subscription_historical s  
left join public.dim_dates d on s.date=d.datum
left join eom e on (e."bom" = DATE_TRUNC('month',s."date")  and s.subscription_id = e.subscription_id)
where (s.date = current_date -1)
order by s.date desc;