drop table if exists dwh.product_subscription_value_historical;
create table dwh.product_subscription_value_historical as 
WITH FACT_DAYS AS (
SELECT DISTINCT 
 DATUM AS FACT_DAY, 
 day_is_last_of_month
FROM public.dim_dates
WHERE DATUM <= CURRENT_DATE
)
select 
fact_day,
s.date,
day_is_last_of_month,
coalesce(P.PRODUCT_SKU,'n/a') as product_sku,
coalesce(p.category_name,'n/a') as category,
coalesce(p.subcategory_name,'n/a') as subcategory,
coalesce(p.brand,'n/a') as brand,
coalesce(s.country_name,'n/a') as store_country,
coalesce(store.store_label,'n/a') as store_label,
coalesce(store.store_short,'n/a') as store_short,
coalesce(store.store_name,'n/a') as store_name,
coalesce(s.store_commercial,'n/a') as store_commercial,
coalesce(c.customer_type,'n/a') as customer_type,
coalesce(c.company_name,'n/a') as company_name,
coalesce(store.id::text,'n/a') as store_id,
coalesce(s.subscription_plan,'n/a') as sub_plan,
COALESCE(sum(s.subscription_value),0) as active_subscription_value,
COALESCE(sum(s.avg_asset_purchase_price),0) as active_asset_purchase_price,
coalesce(count(distinct s.subscription_id),0) as active_subscriptions
from fact_days f
left join master.subscription_historical as s
 on f.fact_day::date >= s.start_date::date and
  F.fact_day::date < coalesce(s.cancellation_date::date, f.fact_day::date+1)
left join  ods_production.variant V
 on v.variant_sku=s.variant_sku
left join ods_production.subscription ss 
 on s.subscription_id=ss.subscription_id
left join ods_production.product P 
 on p.product_id=v.product_id
left join ods_production.store store 
 on ss.store_id=store.id
left join master.customer_historical c 
 on c.customer_id=s.customer_id
 and c.date=s.date
where s.date='2020-08-31'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
;

GRANT SELECT ON dwh.product_subscription_value_historical TO tableau;
