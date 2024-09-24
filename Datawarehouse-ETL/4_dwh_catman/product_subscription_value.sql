drop table if exists dwh.product_subscription_value;
create table dwh.product_subscription_value as 
WITH FACT_DAYS AS (
SELECT
DISTINCT DATUM AS FACT_DAY,
day_is_last_of_month
FROM public.dim_dates
WHERE DATUM <= CURRENT_DATE
)
select 
f.fact_day,
f.day_is_last_of_month,
coalesce(ss.PRODUCT_SKU,'n/a') as product_sku,
coalesce(p.category_name,'n/a') as category,
coalesce(p.subcategory_name,'n/a') as subcategory,
coalesce(p.brand,'n/a') as brand,
coalesce(ss.country_name,'n/a') as store_country,
coalesce(ss.store_label,'n/a') as store_label,
coalesce(store.store_short,'n/a') as store_short,
coalesce(store.store_name,'n/a') as store_name,
coalesce(ss.store_commercial,'n/a') as store_commercial,
coalesce(ss.customer_type,'n/a') as customer_type,
coalesce(c.company_name,'n/a') as company_name,
coalesce(ss.store_id::text,'n/a') as store_id,
coalesce(ss.subscription_plan,'n/a') as sub_plan,
COALESCE(sum(ss.subscription_value_eur),0) as active_subscription_value,
COALESCE(sum(ss.avg_asset_purchase_price),0) as active_asset_purchase_price,
coalesce(count(distinct ss.subscription_id),0) as active_subscriptions
from fact_days f
left join ods_production.subscription_phase_mapping ss
 on f.fact_day::date >= ss.fact_day::date and
  F.fact_day::date <= coalesce(ss.end_date::date, f.fact_day::date+1)
left join ods_production.product P 
 on ss.product_sku = p.product_sku
 left join ods_production.store store
 on ss.store_id = store.id
left join ods_production.customer c 
 on ss.customer_id = c.customer_id 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;

GRANT SELECT ON dwh.product_subscription_value TO tableau;
