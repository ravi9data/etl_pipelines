drop table if exists a;
create TEMP table a as 
select 
 date_trunc('hour',pv.page_view_start) as fact_date,
 coalesce(pv.os_family,'n/a') as os,
 coalesce(s.store_short,'n/a') as store_short,
 coalesce(se.browser,'n/a') as browser,
 coalesce(pv.device_type,'n/a') as device_type,
 coalesce(se.marketing_channel,'n/a') as marketing_channel,
 count(DISTINCT pv.session_id) as sessions,
 count(DISTINCT pv.anonymous_id) as traffic
from traffic.page_views pv
left join traffic.sessions se
 on pv.session_id = se.session_id 
left join ods_production.store s 
 on s.id=pv.store_id
where pv.page_view_start >=current_date - interval '7 day'
group by 1,2,3,4,5,6;


drop table if exists b;
create TEMP table b as 
select 
date_trunc('hour',o.created_date) as fact_date,
coalesce(s.store_short,'n/a') as store_Short,
coalesce(c.last_touchpoint_excl_direct,'n/a') as marketing_channel,
coalesce(c.os,'n/a') as os,
coalesce(c.browser,'n/a') as browser,
coalesce(c.last_touchpoint_device_type,'n/a') as device_type,
count(distinct o.order_id) as carts,
count(distinct case when o.completed_orders=1 then o.order_id end) as submitted_orders
from master."order" o
left join traffic.order_conversions c 
 on o.order_id=c.order_id
left join ods_production.store s 
 on s.id=o.store_id
 where o.created_date  >= current_date - interval '7 day'
group by 1,2,3,4,5,6; 


delete from dwh.hourly_conversion_report where fact_date >= current_date - interval '7 day';

insert into dwh.hourly_conversion_report
select 
coalesce(a.fact_date,b.fact_date) as fact_date,
coalesce(a.store_short,b.store_short,'n/a') as store_short,
coalesce(a.os,b.os,'n/a') as os,
coalesce(a.browser,b.browser,'n/a') as browser,
coalesce(a.device_type,b.device_type,'n/a') as device_type,
coalesce(a.marketing_channel,b.marketing_channel,'n/a') as marketing_channel,
COALESCE(SESSIONS,0) AS SESSIONS,
COALESCE(traffic,0) AS traffic,
coalesce(b.carts,0) as carts,
coalesce(b.submitted_orders,0) as submitted_orders
from a 
full outer join b 
on a.fact_date=b.fact_date
and a.os=b.os
and a.browser=b.browser
and a.store_short=b.store_SHORT
and a.marketing_channel=b.marketing_channel
and a.device_type=b.device_type
order by 1 desc;

GRANT SELECT ON dwh.hourly_conversion_report TO tableau;
