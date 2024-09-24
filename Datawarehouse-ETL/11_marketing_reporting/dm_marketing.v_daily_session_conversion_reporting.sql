DROP VIEW IF EXISTS dm_marketing.v_daily_session_conversion_reporting;
CREATE VIEW dm_marketing.v_daily_session_conversion_reporting AS
with a as (
select 
 pw.page_view_date::date as reporting_date,
 s.marketing_channel ,
 coalesce(pw.device_type,'n/a') as device_type,
 coalesce(c.customer_type,'n/a') as customer_type,
 coalesce(case when pw.store_name = 'Grover B2B Germany' then 'B2B store' else pw.store_label end,'n/a') as store_label,
 count(distinct pw.page_view_id) as page_views,
 count(distinct pw.session_id) as Sessions,
 count(distinct pw.anonymous_id) as users,
 count(distinct case when pw.page_type ='pdp' then pw.page_view_id end) as pdp_page_views,
 count(distinct case when pw.page_type ='pdp' then pw.session_id end) as pdp_sessions,
 count(distinct case when pw.page_type ='pdp' then pw.anonymous_id end) as pdp_users
from traffic.page_views pw 
left join traffic.sessions s 
 on pw.session_id=s.session_id
left join master.customer c on c.customer_id=s.customer_id
where pw.page_view_date::date < current_date
and pw.page_view_date >= '2019-08-11'
group by 1,2,3,4,5
)
,b as (
select DISTINCT 
	o.created_date::date as reporting_date,
coalesce(oc.last_touchpoint_device_type,'n/a') as device_type,
	coalesce(o.customer_type,'n/a') as customer_type,
  	coalesce(o.new_recurring,'n/a') as new_recurring,
    c.company_type_name,
     o.marketing_channel ,
    coalesce(case when store_name = 'Grover B2B Germany' then 'B2B store' else store_label end,'n/a') as store_label,
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
left join traffic.order_conversions oc on oc.order_id=o.order_id
left join master.customer c on c.customer_id = o.customer_id
where o.created_date::date >= '2019-08-11'
group by 1,2,3,4,5,6,7
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
 and a.customer_type=b.customer_type
 and a.device_type=b.device_type
 and a.marketing_channel=b.marketing_channel
WITH NO SCHEMA BINDING; 