drop view if exists dm_brand_reporting.v_soa_report;
create or replace view dm_brand_reporting.v_soa_report
as
with days_ as (
select
distinct datum as fact_date, variant_sku
from public.dim_dates,
(select 
    v.variant_sku,
    p.created_at::date as created_at
    from
    ods_production.variant v 
left join ods_production.product p on v.product_id = p.product_id)
where datum <= CURRENT_DATE AND datum >= DATE_TRUNC('MONTH', DATEADD('MONTH', -13, CURRENT_DATE))
)
-- counting the number of allocated subscriptions on a daily level
-- included with supplier, store information and asset's metadata
,allocated_subs as (select distinct
date_trunc('day',allocated_at)::date as allocated_date,
a2.variant_sku,
count(a.subscription_id) as allocated_subs,
a.asset_id,
o.store_country,
o.store_type,
s.id as store_id,
s.store_label,
s.store_number,
s.store_name,
s.store_short,
a2.supplier,
a2.brand,
a2.product_name,
a2.product_ean,
a2.category_name,
a2.subcategory_name,
a2.serial_number,
pri.purchase_order_number,
s2.rental_period,
a2.times_rented
from ods_production.allocation a 
left join ods_production.asset a2 on a.asset_id = a2.asset_id 
left join ods_production.order o on o.order_id = a.order_id 
left join ods_production.store s on o.store_id = s.id 
left join ods_production.purchase_request_item pri on pri.purchase_request_item_sfid = a2.purchase_request_item_id
left join ods_production.subscription s2 on s2.subscription_id = a.subscription_id 
where supplier is not null and a.rank_allocations_per_subscription >= 1 
group by 1,2,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21)
select 
distinct d.fact_date,
c.*
from days_ d
left join allocated_subs c on c.allocated_date = d.fact_date 
and c.variant_sku = d.variant_sku
with no schema binding;

GRANT SELECT ON dm_brand_reporting.v_soa_report TO tableau;