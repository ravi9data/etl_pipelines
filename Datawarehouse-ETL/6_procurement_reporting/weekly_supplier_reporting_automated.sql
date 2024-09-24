drop table if exists dm_brand_reporting.weekly_reporting_supplier_automated;
create table dm_brand_reporting.weekly_reporting_supplier_automated as 
with days_ as (
select
distinct datum as fact_date, variant_sku, supplier, category_name,subcategory_name
from public.dim_dates,
(select 
    v.variant_sku,
    coalesce(a.supplier, 'NA') as supplier,
    p.category_name,
    p.subcategory_name,
    p.created_at::date as created_at
    from
    ods_production.variant v 
left join ods_production.asset a on a.variant_sku = v.variant_sku
left join ods_production.product p on v.product_id = p.product_id)
where datum <= CURRENT_DATE 
AND datum >= DATE_TRUNC('month', DATEADD('month',-3, CURRENT_DATE))
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
,sub_value as(select f.datum as fact_date,
a2.supplier,
s.variant_sku,
sum(s.subscription_value_eur) as asv
from public.dim_dates f
left join ods_production.subscription_phase_mapping s 
 ON f.datum::date >= s.fact_day::date
		AND F.datum::date <= COALESCE(s.end_date::date, f.datum::date + 1)
left join ods_production.allocation a on a.subscription_id = s.subscription_id 
left join ods_production.asset a2 on a2.asset_id = a.asset_id 
where f.datum <= current_date 
and a.rank_allocations_per_subscription >= 1
group by 1,2,3)
,soh as (
select 
    date_trunc('day',date)::date as report_date,
    coalesce(supplier,'NA') as supplier, 
    coalesce(variant_sku,'n/a') as variant_sku,
    count(CASE WHEN asset_status_original IN ('IN STOCK','INBOUND UNALLOCABLE') THEN asset_id END) AS stock_on_hand,
--	count(case when asset_status_new = 'IN STOCK' then asset_id end) as stock_on_hand,
    count(CASE WHEN asset_status_original IN ('IN STOCK','INBOUND UNALLOCABLE') and asset_condition = 'NEW' THEN asset_id END) AS stock_on_hand_new,
    count(CASE WHEN asset_status_original = 'ON LOAN' THEN asset_id END) AS assets_on_loan,
    count(CASE WHEN asset_status_original NOT IN ('IN STOCK','INBOUND UNALLOCABLE', 'ON LOAN') THEN asset_id END) AS assets_in_transition_status
--	count(case when asset_status_new = 'IN STOCK' and asset_condition = 'NEW' then asset_id end) as stock_on_hand_new
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
final_data as (select distinct d.fact_date as fact_day,
d.variant_sku,
d.supplier,
coalesce(s.asv,0) as active_subscription_value,
-- coalesce(s.active_subs,0) as active_subscriptions,
coalesce(c.allocated_subs,0) as allocated_subs,
coalesce(b.cancelled_subs,0) as cancelled_subs,
--coalesce (i.assets_created,0) as total_assets,
coalesce (si.stock_on_hand,0) as stock_on_hand,
coalesce (si.stock_on_hand_new,0) as stock_on_hand_new,
COALESCE (si.assets_on_loan,0) AS assets_on_loan,
COALESCE (si.assets_in_transition_status,0) AS assets_in_transition_status,
coalesce (n.new_rentals,0) as new_rentals
--coalesce(so.sold_Assets,0) as sold_assets
from days_ d
left join sub_value s on s.fact_date = d.fact_date 
and s.variant_sku = d.variant_sku
and s.supplier = d.supplier
left join c on c.allocated_date = d.fact_date 
and c.variant_sku = d.variant_sku
and c.supplier = d.supplier
left join b on b.cancelled_date = d.fact_date 
and b.variant_sku = d.variant_sku
and b.supplier = d.supplier
left join soh si on si.report_date = d.fact_date
and si.variant_sku = d.variant_sku
AND si.supplier = d.supplier
left join new_rentals n on n.allocated_date = d.fact_date
and n.variant_sku = d.variant_sku
and n.supplier = d.supplier
where d.supplier is not null
)	
,eow_active as (
select 
DATE_TRUNC('week',fact_day) as week_date,
fd.variant_sku,
fd.supplier,
round(active_subscription_value,0) as eow_asv_cw,
-- active_subscriptions as eow_active_subs_cw,
stock_on_hand as eow_stock,
stock_on_hand_new as eow_new_stock,
assets_on_loan AS eow_assets_on_loan,
assets_in_transition_status AS eow_assets_in_transition_status
from final_data fd	
where (DATE_part('dayofweek',fact_day) = 0 or (fact_day = CURRENT_DATE))
)
,eow_ as (
select 
DATE_TRUNC('week',fact_day) as week_date,
fd.variant_sku,
fd.supplier,
sum(allocated_subs) as eow_allocated_sub_cw,
sum(cancelled_subs) as eow_cancelled_sub_cw,
--sum(sold_assets) as eow_sold_assets_cw,
sum(new_rentals) as eow_new_rentals_cw
from final_data fd	
group by 1,2,3
)
,pre_final_ as (select 
    distinct date_trunc('week',fact_day)::date as fact_week,
    fd.supplier,
	ea.eow_asv_cw,
	--ea.eow_active_subs_cw,
	ea.eow_stock,
	ea.eow_new_stock,
	ea.eow_assets_on_loan,
	ea.eow_assets_in_transition_status,
	e.eow_allocated_sub_cw,
	e.eow_cancelled_sub_cw,
	--e.eow_sold_assets_cw,
	e.eow_new_rentals_cw
	from final_data fd 
	left join eow_active ea on DATE_TRUNC('week',fact_day) = ea.week_date and fd.variant_sku = ea.variant_sku and fd.supplier = ea.supplier
	left join eow_ e on DATE_TRUNC('week',fact_day) = e.week_date and fd.variant_sku = e.variant_sku and fd.supplier = e.supplier)
select distinct
    f.fact_week,
 --   s.product_sku,
    f.supplier,
    s.category_name,
	s.subcategory_name,
	s.brand,
	s.product_name,
	case when s.product_name like '%(US)%' then 'US' else 'EU' end as eu_us,
	s.ean,
	s.variant_color,
	s.article_number,
--  coalesce(round((eow_active_subs_lw::numeric  -  eow_active_subs_2w::numeric) / nullif(eow_active_subs_2w::numeric,0) * 100,0),0) as active_subs_wow_percentage,
    coalesce(round((eow_allocated_sub_lw::numeric  -  eow_allocated_sub_2w::numeric) / nullif(eow_allocated_sub_2w::numeric,0) * 100,0),0) as allocated_wow_percentage,
    coalesce(round((eow_cancelled_sub_lw::numeric  -  eow_cancelled_sub_2w::numeric) / nullif(eow_cancelled_sub_2w::numeric,0) * 100,0),0) as cancelled_wow_percentage,
    coalesce(round((eow_new_rentals_lw::numeric  -  eow_new_rentals_2w::numeric) / nullif(eow_new_rentals_2w::numeric,0) * 100,0),0) as newrental_wow_percentage
--  coalesce(round((eow_sold_assets_lw::numeric  -  eow_sold_assets_2w::numeric) / nullif(eow_sold_assets_2w::numeric,0) * 100,0),0) as sold_assets_wow_percentage
from pre_final_ f

GRANT SELECT ON dm_brand_reporting.weekly_reporting_supplier_automated TO radwa_hosny;