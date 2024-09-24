drop table if exists dwh.weekly_reporting_supplier;
create table dwh.weekly_reporting_supplier as 
with days_ as (
select
distinct datum as fact_date, variant_sku,category_name,subcategory_name
from public.dim_dates,
(select 
    v.variant_sku,
    p.category_name,
    p.subcategory_name,
    p.created_at::date as created_at
    from
    ods_production.variant v 
left join ods_production.product p on v.product_id = p.product_id)
where datum <= CURRENT_DATE AND datum >= DATE_TRUNC('MONTH', DATEADD('MONTH', -3, CURRENT_DATE))
)
-- counting the number of cancelled subscriptions day and supplier level
, b as (select date_trunc('day',cancellation_date)::date as cancelled_date, 
count(distinct s.subscription_id) as cancelled_subs, 
coalesce(a2.supplier,'NA') as supplier, 
coalesce(a2.variant_sku,'n/a') as variant_sku
from ods_production.allocation a 
left join ods_production.asset a2 on a.asset_id = a2.asset_id 
left join ods_production.subscription s on a.subscription_id = s.subscription_id 
where s.cancellation_date is not null
group by 1,3,4
order by 1 desc)
-- counting the number of allocated subscriptions day and supplier level
,c as (select date_trunc('day',allocated_at)::date as allocated_date, 
count(distinct s.subscription_id) as allocated_subs, 
avg(s.rental_period) as avg_rental_duration,
coalesce(a2.supplier,'NA') as supplier, 
coalesce(a2.variant_sku,'n/a') as variant_sku
from ods_production.allocation a 
left join ods_production.asset a2 on a.asset_id = a2.asset_id 
left join ods_production.subscription s on a.subscription_id = s.subscription_id 
group by 1,4,5
order by 1 desc)
,sub_value as (
select 
distinct f.fact_date,
coalesce(a2.supplier,'NA') as supplier,
coalesce(f.variant_sku,'n/a') as variant_sku,
COALESCE(sum(s.subscription_value_eur),0) as asv,
coalesce(sum(s.subscription_value_lc),0) as active_subscription_value_original_currency,
COALESCE(count(distinct s.subscription_id),0) as active_subs
from days_ f
left join ods_production.subscription_phase_mapping s
   on f.fact_date::date >= s.fact_day::date and
  F.fact_date::date <= coalesce(s.END_DATE::date, f.fact_date::date+1)
and s.variant_sku = f.variant_sku
left join ods_production.allocation a on s.subscription_id = a.subscription_id
left join ods_production.asset a2 on a.asset_id = a2.asset_id
where a.rank_allocations_per_subscription >= 1
group by 1,2,3
)
-- counting the total assets created on a particular day
,inv as (
select 
    date_trunc('day',created_date)::date as created_date,
     coalesce(supplier,'NA') as supplier, 
    coalesce(variant_sku,'n/a') as variant_sku,
    count(*) as assets_created from ods_production.asset 
    group by 1,2,3)
-- sold assets day wise from a particular supplier
, sold_assets as (
select 
    date_trunc('day',sold_date)::date as sold_date,
    coalesce(supplier,'NA') as supplier, 
    coalesce(variant_sku,'n/a') as variant_sku,
    count(*) as sold_Assets 
    from ods_production.asset 
    where sold_date is not null
    group by 1,2,3)
-- counting the total number of stock (day wise - hence used asset historical table)
,soh as (
select 
    date_trunc('day',date)::date as report_date,
    coalesce(supplier,'NA') as supplier, 
    coalesce(variant_sku,'n/a') as variant_sku,
	count(case when asset_status_new = 'IN STOCK' then asset_id end) as stock_on_hand,
	count(case when asset_status_new = 'IN STOCK' and asset_condition = 'NEW' then asset_id end) as stock_on_hand_new
from master.asset_historical a
	group by 1,2,3)
-- counting the total number of new rentals (day wise, supplier)
,new_rentals as (select 
    date_trunc('day',allocated_at)::date as allocated_date, 
	coalesce(supplier,'NA') as supplier, 
     coalesce(s.variant_sku,'n/a') as variant_sku,
	count(*) as new_rentals from ods_production.allocation a 
left join ods_production.subscription s on s.subscription_id = a.subscription_id
left join ods_production.asset a2 on a2.asset_id = a.asset_id
where is_recirculated = 'New'
group by 1,2,3),
-- eow active subscriptions
eow_active as (
select 
    date_trunc('week',fact_date)::date as week_date,
    coalesce(supplier,'NA') as supplier, 
    coalesce(variant_sku,'n/a') as variant_sku,
    active_subs as eow_active_subs,
    asv as eow_asv 
from sub_value
where (date_part('dayofweek',fact_date) = 0 or (fact_date = current_date))
),
final_data as (select distinct d.fact_date,
d.variant_sku,
d.category_name,
d.subcategory_name,
s.supplier,
coalesce(s.asv,0) as asv,
coalesce(s.active_subs,0) as active_subs,
coalesce(c.allocated_subs,0) as allocated_subs,
coalesce (c.avg_rental_duration,0) as avg_rental_duration,
coalesce(b.cancelled_subs,0) as cancelled_subs,
coalesce (i.assets_created,0) as total_assets,
coalesce (si.stock_on_hand,0) as stock_on_hand,
coalesce (si.stock_on_hand_new,0) as stock_on_hand_new,
coalesce (n.new_rentals,0) as new_rentals,
coalesce(so.sold_Assets,0) as sold_assets,
e.eow_active_subs,
e.eow_asv
from days_ d
left join sub_value s on s.fact_date = d.fact_date 
and s.variant_sku = d.variant_sku
left join c on c.allocated_date = d.fact_date 
and c.variant_sku = d.variant_sku
and c.supplier = s.supplier
left join b on b.cancelled_date = d.fact_date 
and b.variant_sku = d.variant_sku
and b.supplier = s.supplier
left join inv i on i.created_date = d.fact_date
and i.variant_sku = d.variant_sku
and i.supplier = s.supplier
left join soh si on si.report_date = d.fact_date
and si.variant_sku = d.variant_sku
and si.supplier = s.supplier
left join new_rentals n on n.allocated_date = d.fact_date
and n.variant_sku = d.variant_sku
and n.supplier = s.supplier
left join sold_assets so on so.sold_date = d.fact_date
and so.variant_sku = d.variant_sku
and so.supplier = s.supplier
left join eow_active e on date_trunc('week',d.fact_date)::date = e.week_date 
and e.variant_sku = d.variant_sku
and e.supplier = s.supplier
)
select 
    distinct fd.*,
    s.brand,
    s.product_name,
    case when s.product_name like '%(US)%' then 'US' else 'EU' end as eu_us,
    s.variant_color,
    s.ean,
    s.article_number
from final_data fd
left join hightouch_sources.product_data_livefeed s on s.variant_sku = fd.variant_sku;

GRANT SELECT ON dwh.weekly_reporting_supplier TO tableau;
