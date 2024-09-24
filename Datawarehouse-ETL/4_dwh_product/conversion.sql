drop table if exists dwh.conversion;
create table dwh.conversion as
with a as (
select 
 page_view_date::date as reporting_date,
 coalesce(store_label,'n/a') as store_label,
 count(distinct page_view_id) as page_views,
 count(distinct session_id) as Sessions,
 count(distinct anonymous_id) as users,
 count(distinct case when page_type ='pdp' then page_view_id end) as pdp_page_views,
 count(distinct case when page_type ='pdp' then session_id end) as pdp_sessions,
 count(distinct case when page_type ='pdp' then anonymous_id end) as pdp_users
from traffic.page_views
where page_view_date::date < current_date
and page_view_date >= '2019-06-18'
group by 1,2
order by 1 desc
)
,b as (
select DISTINCT 
	o.created_date::date as reporting_date,
 coalesce(store_label,'n/a') as store_label,
	sum(COALESCE(o.cart_orders, 0::bigint)) AS carts,
	sum(COALESCE(o.cart_page_orders, 0::bigint)) AS cart_page_orders,
	sum(COALESCE(o.cart_logged_in_orders, 0::bigint)) AS cart_logged_in_orders,
	sum(COALESCE(o.address_orders, 0::bigint)) AS Address,
	sum(COALESCE(o.payment_orders, 0::bigint)) AS payment,
	sum(COALESCE(o.summary_orders, 0::bigint)) AS summary,
    sum(COALESCE(o.completed_orders, 0::bigint)) AS completed_orders,
    sum(COALESCE(o.cancelled_orders, 0::bigint)) AS cancelled_orders,
    sum(COALESCE(o.declined_orders, 0::bigint)) AS declined_orders,
    sum(COALESCE(o.paid_orders, 0::bigint)) AS paid_orders,
    sum(COALESCE(o.failed_first_payment_orders, 0::bigint)) AS failed_first_payment_orders
from master."order" o 
--left join ods_production.order_item oi on oi.order_id=o.order_id
where o.created_date::date >= '2019-06-18'
group by 1,2
order by 2 desc
)
select distinct 
	b.*,
	a.page_views,
	a.Sessions,
	a.users,
	a.pdp_page_views,
	a.pdp_sessions,
	a.pdp_users
from a 
full join b 
  on a.store_label=b.store_label
 and a.reporting_date=b.reporting_date