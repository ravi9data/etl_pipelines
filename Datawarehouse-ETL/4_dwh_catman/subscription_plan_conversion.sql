BEGIN;
create table trans_dev.plan_converion_impressions as 
with a_prep as (
       SELECT DISTINCT 
            max(i_1.user_id) OVER (PARTITION BY i_1.session_id) AS customer_id,
            i_1.session_id,
            date_trunc('day'::text, i_1.creation_time)::date AS creation_date,
            i_1.creation_time,
            i_1.store_id,
            product_id,
            i_1.minimum_duration_months AS plan_duration,
                CASE
                    WHEN i_1.subscription_plan_price > 500::numeric THEN i_1.subscription_plan_price / 100::numeric
                    ELSE i_1.subscription_plan_price
                END AS subscription_plan_price,
                rank() over (partition by 
                					session_id
                					,date_trunc('day', i_1.creation_time)::date
                					,product_id
                					order by i_1.creation_time
                					) as rank_touchpoints
           FROM stg_events.subscription_plan_impressions i_1)
          select 
           creation_date as created_date, 
           store_id,
           coalesce(product_id::text,'n/a') as product_id,
           plan_duration::decimal(10,2) as duration,
           subscription_plan_price::decimal(10,2) as price,
           count(*) count_views,
           sum(case when rank_touchpoints = 1 then 1 else 0 end) as default_view_count,
           count(distinct session_id) count_sessions,
           count(distinct case when rank_touchpoints = 1 then session_id end) as default_sessions,
           count(distinct customer_id)-1 count_customers
          from a_prep
          group by 1,2,3,4,5;
     

create table trans_dev.plan_conversion_orders as 
select 
 i.created_at::Date as created_date, 
 o.store_id as store_id,
 coalesce(p.product_id::text,'n/a') as product_id,
 plan_duration::decimal(10,2) as duration,
 price::decimal(10,2) as price,
 count(*) as carts, 
 count(distinct case when o.submitted_date is not null then o.order_id end) as completed_orders, 
 count(distinct case when o.status = 'PAID' then o.order_id end) as paid_orders
from ods_production.order_item i
inner join ods_production."order" o on i.order_id=o.order_id
left join ods_production.variant v 
 on v.variant_id=i.variant_id
left join ods_production.product p 
on p.product_id=v.product_id
group by 1,2,3,4,5;



create table trans_dev.plan_conversion_subscription as 
select 
 s.created_date::date as created_date,
 s.store_id,
 coalesce(p.product_id::text,'n/a') as product_id,
 rental_period::decimal(10,2) as duration,
 subscription_value::decimal(10,2) as price,
 count(distinct subscription_id) as subscriptions,
 sum(subscription_value) as subscription_value,
 sum(committed_sub_value) as committed_subscruption_value
 --p.product_sku,
-- order_id,
--subscription_id,
-- subscription_duration,
-- subscription_value,
from ods_production.subscription s
left join ods_production.variant v 
 on v.variant_sku=s.variant_sku
left join ods_production.product p 
on p.product_id=v.product_id
group by 1,2,3,4,5;


--dwh
drop table if exists dwh.subscription_plan_conversion;
create table dwh.subscription_plan_conversion as 
select 
s.id as store_id,
s.store_name,
s.store_short as channel,
coalesce(a.created_date,c.created_date,b.created_date) as created_date,
p.product_name,
p.product_sku,
p.category_name,
p.subcategory_name,
p.brand,
coalesce(c.duration::decimal(10,2),b.duration::decimal(10,2),a.duration::decimal(10,2)) as duration,
coalesce(c.price::decimal,b.price::decimal,a.price::decimal) as price,
coalesce(count_views,0) as count_views,
coalesce(default_view_count,0) as default_view_count,
coalesce(carts,0) as carts,
coalesce(completed_orders,0) as completed_orders,
coalesce(paid_orders,0) as paid_orders,
coalesce(subscriptions,0) as subscriptions,
coalesce(subscription_value,0) as subscription_value,
coalesce(committed_subscruption_value,0) as committed_subscruption_value
from trans_dev.plan_converion_impressions a 
full outer join trans_dev.plan_conversion_orders b 
 on a.store_id=b.store_id
 and a.created_date=b.created_date
 and a.product_id=b.product_id
 and a.duration=b.duration
 and a.price=b.price
full outer join trans_dev.plan_conversion_subscription c 
 on c.store_id::text=coalesce(b.store_id::text,a.store_id::text)
 and c.created_date=coalesce(b.created_date,a.created_Date)
 and C.product_id=coalesce(b.product_id,a.product_id)
 and c.duration=coalesce(b.duration,a.duration)
 and c.price=coalesce(b.price,b.price)
left join ods_production.store s 
 on s.id=coalesce(c.store_id::integer,b.store_id::integer,a.store_id::integer)
 left join ods_production.product p 
on p.product_id=coalesce(c.product_id, b.product_id,a.product_id)
;

END;



drop table if exists trans_dev.plan_converion_impressions;
drop table if exists trans_dev.plan_conversion_orders;
drop table if exists trans_dev.plan_conversion_subscription;

GRANT SELECT ON dwh.subscription_plan_conversion TO tableau;
