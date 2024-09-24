drop table if exists dm_brand_reporting.weekly_reporting_brand_automated;
create table dm_brand_reporting.weekly_reporting_brand_automated as 
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
,sub_value as (
select 
f.fact_day,
coalesce(f.variant_sku,'n/a') as variant_sku,
COALESCE(sum(s.subscription_value_eur),0) as active_subscription_value,
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
count(*) as assets_created 
from ods_production.asset 
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
, sold_assets as (
  select 
    date_trunc('day',sold_date) as sold_date,
    variant_sku,
    count(*) as sold_Assets from ods_production.asset 
    group by 1,2)
, new_rentals as (
	select allocated_at::date as allocated_date, 
	s.variant_sku,
	count(*) as new_rentals 
	from ods_production.allocation a 
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
	fd.variant_sku,
	active_subscription_value,
	active_subscriptions,
	coalesce(acquired_subscriptions,0) as acquired_subscriptions,
	coalesce(cancelled_subscriptions,0) as cancelled_subscriptions,
	coalesce(assets_created,0) as asset_investment,
	coalesce(sold_Assets,0) as sold_assets,
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
left join sold_assets sa on sa.sold_date = fd.fact_day and sa.variant_sku = fd.variant_sku
)
-- aggregating all the KPIs at EOW
,eow_active as (
select 
DATE_TRUNC('week',fact_day) as week_date,
fd.variant_sku,
round(active_subscription_value,0) as eow_asv_cw,
active_subscriptions as eow_active_subs_cw,
stock_on_hand as eow_stock,
stock_on_hand_new as eow_new_stock,
avg_rental_duration
from final_data fd	
where (DATE_part('dayofweek',fact_day) = 0 or (fact_day = CURRENT_DATE))
)
,eow_ as (
select 
DATE_TRUNC('week',fact_day) as week_date,
fd.variant_sku,
sum(acquired_subscriptions) as eow_acquired_sub_cw,
sum(cancelled_subscriptions) as eow_cancelled_sub_cw,
sum(sold_assets) as eow_sold_assets_cw,
sum(new_rentals) as eow_new_rentals_cw
from final_data fd	
group by 1,2
)
,pre_final_ as (select 
    distinct date_trunc('week',fact_day)::date as fact_week,
	ea.*,
	e.*
	from final_data fd 
	left join eow_active ea on DATE_TRUNC('week',fact_day) = ea.week_date and fd.variant_sku = ea.variant_sku
	left join eow_ e on DATE_TRUNC('week',fact_day) = e.week_date and fd.variant_sku = e.variant_sku)
-- final calculations to get KPIs at the end of lw and end of 2 weeks before
,final_ as (select 
    f.fact_week,
    s.category_name,
	s.subcategory_name,
	s.brand,
	s.product_name,
	case when s.product_name like '%(US)%' then 'US' else 'EU' end as eu_us,
	s.ean,
	s.variant_color,
	s.article_number,
    coalesce(round((eow_active_subs_lw::numeric  -  eow_active_subs_2w::numeric) / nullif(eow_active_subs_2w::numeric,0) * 100,0),0) as active_subs_wow_percentage,
    coalesce(round((eow_acquired_sub_lw::numeric  -  eow_acquired_sub_2w::numeric) / nullif(eow_acquired_sub_2w::numeric,0) * 100,0),0) as acquired_wow_percentage,
    coalesce(round((eow_cancelled_sub_lw::numeric  -  eow_cancelled_sub_2w::numeric) / nullif(eow_cancelled_sub_2w::numeric,0) * 100,0),0) as cancelled_wow_percentage,
    coalesce(round((eow_new_rentals_lw::numeric  -  eow_new_rentals_2w::numeric) / nullif(eow_new_rentals_2w::numeric,0) * 100,0),0) as newrental_wow_percentage,
    coalesce(round((eow_sold_assets_lw::numeric  -  eow_sold_assets_2w::numeric) / nullif(eow_sold_assets_2w::numeric,0) * 100,0),0) as sold_assets_wow_percentage
from pre_final_ f
select *
from 
final_ f
order by 1 DESC