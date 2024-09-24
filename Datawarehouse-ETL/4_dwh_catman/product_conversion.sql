drop  table if exists dwh.product_conversion;
create table dwh.product_conversion as 
with traffic as (
select 
 v.store_id::CHARACTER VARYING as store_id, 
 v.product_id::CHARACTER VARYING as product_id, 
 v.creation_time::Date as pageview_Date,
 count(*) as pageviews,
 count(case when page_id is not null then session_id end) as pageviews_w_url,
 count(distinct session_id) as pageview_unique_sessions,
 count(distinct user_id) as pageview_unique_customers
from stg_events.product_views v 
group by 1,2,3
order by 3 desc)
,orders as (
select 
 o.store_id as store_id,
 v.product_id as product_id,
 i.created_at::Date as order_date, 
 count(*) as carts, 
 count(distinct case when o.is_in_salesforce then o.order_id end) as completed_orders, 
 count(distinct case when o.status = 'PAID' then o.order_id end) as paid_orders,
 count(distinct case when returned>0 then o.order_id end) as returned_orders,
 avg(subscription_value) as avg_price,
 avg(avg_duration) as avg_duration
from ods_production.order_item i
inner join ods_production."order" o on i.order_id=o.order_id
left join (
select order_id, sum(case when in_transit_at::date-delivered_at::date <=7 then 1 else 0 end) as returned
from ods_production.allocation
where in_transit_at is not null 
group by 1 ) r on r.order_id=o.order_id
left join (
select order_id, avg(subscription_value) as subscription_value, avg(rental_period) as avg_duration
from ods_production.subscription 
group by 1
) s on s.order_id=o.order_id
left join ods_production.variant v on v.variant_id=i.variant_id
group by 1,2,3
order by 3 desc)
select distinct 
coalesce(store_name,'N/A') as store_name,
coalesce(store_label,'N/A') as store_label,
s.id as store_id,
coalesce(o.order_date::date,t.pageview_Date::date) as report_date,
p.product_name,
p.product_sku,
p.category_name,
p.subcategory_name,
p.brand,
coalesce(pageviews_w_url,0) as pageviews,
coalesce(carts,0) as carts,
coalesce(completed_orders,0) as completed_orders,
coalesce(paid_orders,0) as paid_orders,
coalesce(returned_orders,0) as returned_within_7_Days,
coalesce(avg_duration) as avg_duration,
coalesce(avg_price) as avg_price
from traffic t 
full outer join orders o 
 on t.store_id=o.store_id
 and t.product_id=o.product_id
 and t.pageview_Date=o.order_date
left join ods_production.product p 
 on p.product_id::text=coalesce(o.product_id::text,t.product_id::text)
left join ods_production.store s 
 on s.id=coalesce(o.store_id,t.store_id)
order by 3 desc ;
