truncate dm_recommerce.return_stages_final;
insert into dm_recommerce.return_stages_final

with fact_dates as(
select dd.datum 
from public.dim_dates dd where dd.datum <=current_date 
),

--get last event for the assets are In Process
latest_event_of_in_process_articles as(
select * from(
select *,
case when return_stages_all like '%Processed%' then last_day(dateadd('month',-1,in_process_end_date))
else current_date end as in_process_last_end_of_month,
row_number() over(partition by serial_number, return_stages,article_id order by event_timestamp desc) as rn_return_stages
from dm_recommerce.return_stages)sub_q where sub_q.return_stages='In Process' and sub_q.rn_return_stages=1
),

--since we don't get any event for the assets are in process, this method is applied
in_process_articles as(
select *
from fact_dates fd
left join latest_event_of_in_process_articles rs on fd.datum>=rs.in_process_start_date::date and fd.datum<=rs.in_process_last_end_of_month
),

all_data as(
select 
datum as reporting_date,
article_id,
serial_number,
asset_id,
asset_name,
brand,
category_name,
subcategory_name,
created_at,
has_scanned_position,
in_process_start_date,
in_process_end_date,
processed_date,
initial_price,
product_sku,
variant_sku,
ref_status,
residual_value_market_price,
return_stages,
return_stages_detailed,
subscription_revenue,
_3pl
from in_process_articles

union

select 
event_timestamp as reporting_date,
article_id,
serial_number,
asset_id,
asset_name,
brand,
category_name,
subcategory_name,
created_at,
has_scanned_position,
in_process_start_date,
in_process_end_date,
processed_date,
initial_price,
product_sku,
variant_sku,
ref_status,
residual_value_market_price,
return_stages,
return_stages_detailed,
subscription_revenue,
_3pl
from dm_recommerce.return_stages rs where return_stages<>'In Process')

select 
*,
max(case when return_stages='In Process' then reporting_date end) 
over(partition by serial_number, article_id, date_trunc('month', reporting_date)) as last_day_in_process_per_month,
datediff('day', in_process_start_date::date,last_day_in_process_per_month::date) as days_in_process,
datediff('day', in_process_start_date::date,processed_date::date) as days_till_procesed,
row_number() over(partition by date_trunc('month', reporting_date), serial_number, article_id) as rn_month
from all_data;
