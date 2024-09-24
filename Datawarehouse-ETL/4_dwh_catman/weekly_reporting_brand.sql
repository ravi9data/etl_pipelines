drop table if exists dwh.weekly_reporting_brand;
create table dwh.weekly_reporting_brand as 
WITH FACT_DAYS AS (
SELECT
DISTINCT DATUM AS FACT_DAY,variant_sku
FROM public.dim_dates, 
			(select 
					variant_sku,created_at::date as created_at
					from
					ods_production.variant v 
					left join ods_production.product p on v.product_id = p.product_id)
WHERE DATUM <= CURRENT_DATE
and datum >= created_at
) 
-- updated with old logic
,sub_value as (
select 
f.fact_day,
coalesce(f.variant_sku,'n/a') as variant_sku,
COALESCE(sum(s.subscription_value_eur),0) as active_subscription_value,
coalesce(sum(s.subscription_value_lc),0) as active_subscription_value_original_currency,
COALESCE(count(distinct s.subscription_id),0) as active_subscriptionS
from fact_days f
left join ods_production.subscription_phase_mapping s
   on f.fact_day::date >= s.fact_day::date and
  F.fact_day::date <= coalesce(s.END_DATE::date, f.fact_day::date+1)
and s.variant_sku = f.variant_sku
group by 1,2)
,subs as (
select 
start_date::date as report_date,
coalesce(v.variant_sku,'N/A') as variant_sku,
count(distinct subscription_Id) as acquired_subscriptions
from ods_production.subscription s
left join ods_production.variant v on v.variant_sku=s.variant_sku
group by 1,2
)
,subs_c as (
select 
cancellation_date::date as report_date,
coalesce(v.variant_sku,'N/A') as variant_sku,
    count(distinct subscription_Id) as cancelled_subscriptions
from ods_production.subscription s
left join ods_production.variant v on v.variant_sku=s.variant_sku
group by 1,2
)
, inv as (
select date_trunc('day',created_date) as created_date,
variant_sku,
count(*) as assets_created from ods_production.asset 
group by 1,2
)
-- stock level historical
, soh as (
  select 
    date_trunc('day',date)::date as report_date,
    a.variant_sku,
	count(case when asset_status_new = 'IN STOCK' then asset_id end) as stock_on_hand,
	count(case when Asset_status_new = 'IN STOCK' and asset_condition = 'NEW' then asset_id end) as stock_on_hand_new
from master.asset_historical a
	group by 1,2)
, new_rentals as (
	select allocated_at::date as allocated_date, 
	s.variant_sku,
	count(*) as new_rentals from ods_production.allocation a 
	left join ods_production.subscription s on s.subscription_id = a.subscription_id
	where is_recirculated = 'New'
	group by 1,2 )
-- rental duration
, rental_duration as 
(select date_trunc('day',date)::date as report_date,
   variant_sku,
   avg_rental_duration
   from master.variant_historical vh
   group by 1,2,3)
, final_data as (	
Select 
	fd.fact_day,
	date_part('week', fd.fact_day)::numeric as week_num,
    date_part('year', fd.fact_day)::numeric as year_,
	fd.variant_sku,
	active_subscription_value,
	active_subscription_value_original_currency,
	active_subscriptions,
	coalesce(acquired_subscriptions,0) as acquired_subscriptions,
	coalesce(cancelled_subscriptions,0) as cancelled_subscriptions,
	coalesce(assets_created,0) as asset_investment,
	coalesce(new_rentals,0) as new_rentals,
	coalesce(soh.stock_on_hand,0) as stock_on_hand,
	coalesce(soh.stock_on_hand_new,0) as stock_on_hand_new,
	rd.avg_rental_duration
from FACT_DAYS fd 
left join sub_value sv on fd.fact_day = sv.fact_day and fd.variant_sku = sv.variant_sku
left join subs s on s.report_date = fd.fact_day and fd.variant_sku = s.variant_sku
left join subs_c sc on sc.report_date = fd.fact_day and fd.variant_sku = sc.variant_sku
left join inv i on i.created_date = fd.fact_day and fd.variant_sku = i.variant_sku
left join soh on soh.report_date = fd.fact_day and soh.variant_sku = fd.variant_sku
left join new_rentals n on n.allocated_date = fd.fact_day and n.variant_sku = fd.variant_sku
left join rental_duration rd on rd.report_date = fd.fact_day and rd.variant_sku = fd.variant_sku
)
,eow_active as (
select 
DATE_TRUNC('week',fact_day) as week_date,
variant_sku,
active_subscription_value as eow_asv,
active_subscription_value_original_currency as eow_asv_original,
active_subscriptions as eow_active_subs from final_data 	
where (DATE_part('dayofweek',fact_day) = 0 or (fact_day = CURRENT_DATE))
)
select fd.*,
    s.category_name,
	s.subcategory_name,
	s.brand,
	s.product_name,
	s.ean,
	s.upcs,
	s.variant_color,
	s.article_number,
	ea.eow_active_subs,
	ea.eow_asv,
	ea.eow_asv_original
	from final_data fd 
	left join eow_active ea on DATE_TRUNC('week',fact_day) = ea.week_date and fd.variant_sku = ea.variant_sku
	left join skyvia.product_data_livefeed s on s.variant_sku = fd.variant_sku
    order by 1 desc;

GRANT SELECT ON dwh.weekly_reporting_brand TO tableau;
