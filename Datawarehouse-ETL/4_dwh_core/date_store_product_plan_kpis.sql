create temp table plan_orders as --temp table
select 
 i.created_at::Date as created_date, 
 o.store_id,
 o.store_label,
 o.store_name,
 o.store_type,
 i.product_sku,
 i.product_name,
 coalesce(i.category_name::text,'n/a') as category_name,
 coalesce(i.subcategory_name::text,'n/a') as subcategory_name,
 coalesce(i.brand::text,'n/a') as brand,
 plan_duration::decimal(10,2) as duration,
 count(*) as carts, 
 count(distinct case when o.submitted_date is not null then o.order_id end) as completed_orders, 
 count(distinct case when o.status = 'PAID' then o.order_id end) as paid_orders
from ods_production.order_item i
inner join master."order" o on i.order_id=o.order_id
group by 1,2,3,4,5,6,7,8,9,10,11;


create temp table plan_subscription as --temp table
select 
 s.created_date::date as created_date,
 s.store_id,
 s.store_label,
 s.store_name,
 s.store_type,
 s.product_sku,
 s.product_name,
 coalesce(s.category_name::text,'n/a') as category_name,
 coalesce(s.subcategory_name::text,'n/a') as subcategory_name,
  coalesce(s.brand::text,'n/a') as brand,
 rental_period::decimal(10,2) as duration,
 count(distinct subscription_id) as subscriptions,
 sum(subscription_value) as subscription_value,
 sum(committed_sub_value + s.additional_committed_sub_value) as committed_subscruption_value
from master.subscription s
group by 1,2,3,4,5,6,7,8,9,10,11;


drop table if exists dwh.date_store_category_plan_kpis;
create table dwh.date_store_category_plan_kpis as
select 
	o.created_date,
	o.store_id ,
	o.store_label,
	o.store_name,
	o.store_type,
	o.product_sku,
	o.product_name,
	o.category_name,
	o.subcategory_name,
	o.brand,
	o.duration,
	carts,
	completed_orders,
	paid_orders ,
	subscriptions,
	subscription_value,
	committed_subscruption_value
from plan_orders o
full join plan_subscription s
	on o.created_date =s.created_date 
	and o.store_id =s.store_id 
	and o.product_sku =s.product_sku 
	and o.duration = s.duration;

	
drop table plan_orders;
drop table plan_subscription;	