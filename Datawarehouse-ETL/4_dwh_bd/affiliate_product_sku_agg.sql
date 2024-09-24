drop table if exists web_sessions_;
create temp table web_sessions_ as 
select distinct
		session_start::date,
        marketing_source,
        session_id
from traffic.sessions
where marketing_source in ('breathe_ilo', 'cowboy', 'lg_de')
  and marketing_medium = 'g-pays'
  and session_start > current_date - 365;


drop table if exists page_views_;
create temp table page_views_ as 
select distinct
	session_id ,
	page_type_detail 
from traffic.page_views 
where session_id in (select session_id from web_sessions_)
and page_type = 'pdp';


drop table if exists session_order_mapping_;
create temp table session_order_mapping_ as 
select distinct 
	session_id ,
	order_id 
from traffic.session_order_mapping
where order_id is not null 
and session_id in (select session_id from web_sessions_);



drop table if exists dm_bd.affiliate_product_sku_agg;
create table dm_bd.affiliate_product_sku_agg as
with 
orders_ as (
	select 
		order_id,
		created_date ,
		submitted_date ,
		paid_date 
	from master."order"
	where order_id in (select order_id from session_order_mapping_)
),
order_items_ as (
	select 
		o.order_id,
		o.created_date ,
		o.submitted_date ,
		o.paid_date ,
		oi.product_sku 
	from orders_ o
	left join ods_production.order_item oi 
	on o.order_id = oi.order_id
),
_session_kpis as (
	select 
		date_trunc('week', w.session_start::date) fact_date,
		w.marketing_source,
		coalesce (nullif(pv.page_type_detail, ''), 'No SKU Info Captured') as product_sku ,
		count(distinct w.session_id) kpi_value
	from web_sessions_ w 
	left join page_views_ pv 
	on w.session_id = pv.session_id 
	group by 1,2,3
),
_order_kpis as (
	select distinct  
	  	s.marketing_source,
		o.product_sku, 
		o.order_id, 
		date_trunc('week', o.created_date::date) created_date ,
		date_trunc('week', o.submitted_date::date) submitted_date ,
		date_trunc('week', o.paid_date::date) paid_date 
	FROM web_sessions_ s
	LEFT JOIN session_order_mapping_ m 
		ON m.session_id = s.session_id 
	left join order_items_ o
		on o.order_id = m.order_id
	where o.order_id is not null
),
joins as (
	select 
		fact_date , 
		'sessions' as kpi_name, 
		marketing_source, 
		product_sku, 
		kpi_value 
	from _session_kpis 
	union all 
	select 
		created_date as fact_date , 
		'cart_orders' as kpi_name , 
		marketing_source , 
		product_sku , 
		count(distinct order_id) as kpi_value 
	from _order_kpis 
	group by 1,2,3,4
	union all 
	select 
		submitted_date as fact_date , 
		'submitted_orders' as kpi_name , 
		marketing_source , 
		product_sku , 
		count(distinct order_id) as kpi_value 
	from _order_kpis 
	where submitted_date is not null
	group by 1,2,3,4
	union all 
	select 
		paid_date as fact_date , 
		'paid_orders' as kpi_name , 
		marketing_source , 
		product_sku , 
		count(distinct order_id) as kpi_value 
	from _order_kpis 
	where paid_date is not null
	group by 1,2,3,4
)
select 
	j.fact_date,
	j.kpi_name ,
	j.marketing_source as affiliate_name,
	j.product_sku ,
	p.brand,
	p.product_name,
	j.kpi_value 
from joins j 
left join ods_production.product p 
on j.product_sku = p.product_sku 
;
