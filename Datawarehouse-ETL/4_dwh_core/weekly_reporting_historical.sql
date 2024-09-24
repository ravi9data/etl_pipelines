--Table created for Weekly Reporting
delete from dwh.weekly_reporting_historical WHERE date = current_date -1 or week_day_number != 7;
insert into dwh.weekly_reporting_historical 

with eow as(
select
distinct
date_trunc('week', s.date::date)::date as bow,
date as eow,
s.subscription_id,
lag(dpd) over (partition by subscription_id order by date) as previous_dpd,
lag(returned_packages) over (partition by subscription_id order by date) as previous_returns,
lag(outstanding_asset_value) over (partition by subscription_id order by date) as prev_asset_value
from master.subscription_historical s  
left join public.dim_dates d on s.date=d.datum
where d.week_day_number = 7 
order by 1 desc
)
, countries AS (  
	SELECT 
		subscription_id,
		country_name,
		ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY date DESC) AS row_num
	FROM master.subscription_historical
	WHERE country_name IS NOT NULL
)
select distinct 
date_trunc('week', s.date)::date as bow,
s.date,
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
d.week_day_number,
e.previous_dpd as end_of_last_week_dpd,
e.previous_returns as end_of_last_week_returns,
e.prev_asset_value as end_of_last_week_asset_value,
c.country_name
from master.subscription_historical s  
left join public.dim_dates d on s.date=d.datum
left join eow e on (e."bow" = DATE_TRUNC('week',s."date")  and s.subscription_id = e.subscription_id)
LEFT JOIN countries c
	ON s.subscription_id = c.subscription_id
	AND row_num = 1
where d.week_day_number = 7 and s.date = current_date -1
order by date DESC
;
