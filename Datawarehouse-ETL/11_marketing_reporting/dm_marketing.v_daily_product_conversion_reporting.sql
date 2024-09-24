DROP VIEW IF EXISTS dm_marketing.v_daily_product_conversion_reporting;
CREATE VIEW dm_marketing.v_daily_product_conversion_reporting AS
with a as (
select  
	date_trunc('hour',fact_day) as reporting_date,
	store_label,
	store_name,
	product_sku,
	SUM(pageviews) as page_views 
from dwh.product_reporting 
where date_trunc('hour',fact_day)>='2019-08-12'
group by 1,2,3,4
order by 2 desc
)   
,b as (
select DISTINCT 
	date_trunc('hour',o.created_date)::date as reporting_date,
	o.store_label,
	o.store_name,
	oi.product_sku,
	sum(COALESCE(o.cart_orders, 0::bigint)) AS carts,
	sum(COALESCE(o.cart_page_orders, 0::bigint)) AS cart_page_orders,
	sum(COALESCE(o.cart_logged_in_orders, 0::bigint)) AS cart_logged_in_orders,
	sum(COALESCE(o.address_orders, 0::bigint)) AS Address,
	sum(COALESCE(o.payment_orders, 0::bigint)) AS payment,
	sum(COALESCE(o.summary_orders, 0::bigint)) AS summary,
    sum(COALESCE(o.completed_orders, 0::bigint)) AS completed_orders,
    sum(COALESCE(o.declined_orders, 0::bigint)) AS declined_orders,
    sum(COALESCE(o.paid_orders, 0::bigint)) AS paid_orders
from master."order" o 
left join ods_production.order_item oi on oi.order_id=o.order_id
where date_trunc('hour',o.created_date)::date >='2019-08-12'
group by 1,2,3,4
order by 2 desc
)
select distinct 
	b.*,
	case when vv.productsku is not null 
	  and vv.status in ('available' ,'automatic')
	  then 'affected' 
	  when vv.productsku is not null 
	   then 'inthelist'
	   else 'notinthelist' end as sample,
	a.page_views
from a 
full join b on a.store_name=b.store_name 
 and a.reporting_date=b.reporting_date
 and a.product_sku=b.product_sku
left join stg_external_apis.gs_skus_wo_variant vv
 on vv.productsku=coalesce(a.product_sku,b.product_sku)
 WITH NO SCHEMA BINDING;