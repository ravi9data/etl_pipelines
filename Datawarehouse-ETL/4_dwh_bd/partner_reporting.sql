--- Impressions per product_sku (soon it will be changed to variant_sku)
drop table if exists _impressions;
create temp table _impressions as  
with impressions_pre as (
	select 
		date_trunc('week', reporting_date::date) as reporting_date ,
		store_id ,
		product_sku ,
		impressions ,
		button_state 
	where store_id  in (select id from ods_production.store where store_short = 'Partners Online')
	and reporting_date::date > current_date - 365	
)
SELECT
	reporting_date,
	store_id,
	product_sku,
   	sum(impressions) as total_impressions,
   	sum(case when button_state = 'available' then impressions end) as available_impressions,
   	sum(case when button_state = 'unavailable' then impressions end) as unavailable_impressions,
   	sum(case when button_state = 'widget' then impressions end) as widget_impressions
from impressions_pre
group by 1,2,3;	
	


--- the Landing page traffic shows how many times the customer clicked on the Grover Partner Button and landed on Grover page
drop table if exists _traffic;
create temp table _traffic as 
with traffic_pre as (
	select 
		date_trunc('week', page_view_date::date) as reporting_date,
		store_id,
		marketing_campaign as variant_sku,
		page_view_id,
		session_id,
		anonymous_id
	FROM traffic.page_views
	where page_type ='landing' 
	 and variant_sku is not null
	 and store_id in (select id from ods_production.store where store_short = 'Partners Online')
	 and page_view_date::date > current_date - 365
),
counting as (
	select 
		reporting_date,
		store_id,  
		variant_sku,
		count(distinct page_view_id) as pageviews,
		--count(distinct session_id) as pageview_unique_sessions,
		count(distinct anonymous_id) as users
	FROM traffic_pre
	group by 1,2,3
)
select 
	c.reporting_date ,
	c.store_id ,
	c.variant_sku ,
	v.product_sku,
	c.pageviews,
	--c.pageview_unique_sessions,
	c.users
from counting c 
left join master.variant v 
on c.variant_sku = v.variant_sku 
;
		

--Counts the Add to cart orders
drop table if exists _orders;
create temp table _orders as 
with recent_orders as (
	select 
		date_trunc('week', created_date ::date) as reporting_date ,
		store_id ::integer,
		order_id,
		submitted_date ,
		paid_date 
	from ods_production."order" 
	where created_date ::date > current_date - 365
	and store_id in (select id from ods_production.store where store_short = 'Partners Online')
)
SELECT 
	o.reporting_date, 
	o.store_id::integer,
	oi.variant_sku, 
	oi.product_sku ,
	count (distinct o.order_id) AS add_to_cart,
   	count(distinct case when o.submitted_date is not null then o.order_id end) AS submitted_orders,
   	count(distinct case when o.paid_date is not null then o.order_id end) AS paid_orders
FROM recent_orders o
inner JOIN ods_production.order_item oi	ON oi.order_id = o.order_id
group by 1,2,3,4;

		
		
-- Information on Active Subscriptions
drop table if exists _subs;
create temp table _subs as 
with 
subs as (
	select 
		subscription_id ,
		store_id::integer ,
		variant_sku ,
		product_sku ,
		subscription_value ,
		rental_period ,
		payment_method ,
		date_trunc('week', start_date) as start_date ,
		cancellation_date 
	from ods_production.subscription 
	where store_short = 'Partners Online' 
	and start_date > current_date - 365
)
	select 
		s.start_date as reporting_date,
		s.store_id,
		s.variant_sku,
		s.product_sku,
		count(distinct subscription_id) as acquired_subscription,
		sum(s.subscription_value) as acquired_subscription_value
from subs s
group by 1,2,3,4 ;


drop table if exists dm_bd.partner_reporting;
create table dm_bd.partner_reporting as 
with base as (
	select reporting_date , store_id , product_sku , variant_sku from _traffic
	union 
	select reporting_date , store_id , product_sku , variant_sku from _orders
	union
	select reporting_date , store_id , product_sku , variant_sku from _subs
)
select 
	b.reporting_date , 
	b.store_id , 
	st.store_name,
	st.store_label ,
	b.product_sku , 
	b.variant_sku ,
	v.ean,
	v.product_name ,
	v.brand ,
	v.variant_color ,
	v.category_name ,
	v.subcategory_name ,
	coalesce(i.total_impressions, 0) as total_impressions, 
	coalesce(i.available_impressions, 0) as available_impressions,
    	coalesce(i.unavailable_impressions, 0) as unavailable_impressions,
   	coalesce(i.widget_impressions, 0) as widget_impressions,
	coalesce(t.pageviews , 0) as pageviews,
	coalesce(t.users, 0) as users,
	coalesce(o.add_to_cart , 0) as add_to_cart,
	coalesce(o.submitted_orders , 0) as submitted_orders,
	coalesce(o.paid_orders, 0) as paid_orders,
   	coalesce(s.acquired_subscription , 0) as acquired_subscription,
	coalesce(s.acquired_subscription_value,  0) as acquired_subscription_value
from base b 
left join master.variant v 
	on b.variant_sku = v.variant_sku 
left join ods_production.store st 
	on b.store_id = st.id 
left join _impressions i 
	on b.reporting_date = i.reporting_date and b.store_id = i.store_id and b.product_sku = i.product_sku 
left join _traffic t 
	on b.reporting_date = t.reporting_date and b.store_id = t.store_id and b.variant_sku = t.variant_sku 
left join _orders o 
	on b.reporting_date = o.reporting_date and b.store_id = o.store_id and b.variant_sku = o.variant_sku 
left join _subs s 
	on b.reporting_date = s.reporting_date and b.store_id = s.store_id and b.variant_sku = s.variant_sku ;

GRANT SELECT ON ALL TABLES IN SCHEMA dm_bd TO slobodan_ilic;
GRANT SELECT ON dm_bd.partner_reporting TO tableau;
