with category as (
select
c.category_id as category_id,
c.category_name as category_name,
s.id as subcategory_id,
s.name as subcategory_name
from data_production_rds_datawarehouse_api_production.categories s
left join (select
 id as category_id,
 parent_id as parent_id,
 name as category_name
from data_production_rds_datawarehouse_api_production.categories
where parent_id is null) c  on s.parent_id  = concat(c.category_id,'.0')
where c.category_id is not null)
,
category_product as
(
select distinct
p.id as product_id,
sku as product_sku,
updated_at,
name as product_name,
c.category_id,
c.subcategory_id,
c.category_name,
c.subcategory_name,
upper(p.brand) as brand
from data_production_rds_datawarehouse_api_production.spree_products p
inner join category c on p.category_id=concat(c.subcategory_id,'.0')
),
market_price as (
select *,lead(datum)over(partition by product_sku order by datum  asc) as next_date
from  data_production_redshift.ods_production_grover_market_value_historical
)
,
new_infra_order as (
SELECT
	JSON_EXTRACT_SCALAR(json_parse(payload),'$.order_number') order_number,
	CAST(SUBSTR(kafka_received_at,1,23) AS timestamp) AS submitted_date,
	JSON_EXTRACT_SCALAR(json_parse(payload),'$.created_at') created_at,
	cast(json_extract("payload",'$.line_items') as ARRAY(MAP(VARCHAR, JSON))) AS line_items_array
	
FROM
    data_production_kafka_topics_raw.internal_order_placed
 ),
parsing_new_infra as  ( select
  substr(io.created_at ,1, 10) as created_at,
  io.order_number,
  io.submitted_date,
  json_extract_scalar(cast("line_items" as json),'$.quantity') as quantity,
  json_extract_scalar(cast("line_items" as json),'$.item_price.in_cents') as price,
  json_extract_scalar(cast("line_items" as json),'$.item_price.currency') as currency,
  json_extract_scalar(cast("line_items" as json),'$.variant.product_sku') as product_sku,
  json_extract_scalar(cast("line_items" as json),'$.variant.variant_sku') as variant_sku,
  json_extract_scalar(cast("line_items" as json),'$.variant.variant_id') as variant_id,
  json_extract_scalar(cast("line_items" as json),'$.variant.name') as name,
  json_extract_scalar(cast("line_items" as json),'$.committed_months') as duration
  from
  new_infra_order io, UNNEST(line_items_array) as t(line_items)
 ) ,
new_infra_final_order as (select
 	 cast(date_format(from_unixtime(cast(ni.created_at as bigint)),'%Y-%m-%d %H:%i:%s')as timestamp)  as created_at,
	ni.order_number as order_id,
	cast(ni.submitted_date as timestamp) as submitted_at,
	ni.quantity,
	cast(ni.price as integer)*0.01 as price,
	ni.currency,
	vm.sku as variant_sku,
	vm.product_id
	from
 parsing_new_infra ni
 left join data_production_rds_datawarehouse_api_production.spree_variants vm on vm.id = ni.variant_id
 ),
  old_order_infra as (
  select
  	cast(substr(i.created_at,1,19) as timestamp ) as created_at,
	o."number" as order_id,
	cast(s.createddate  as timestamp) as submitted_at,
	i.quantity,
	cast(i.price as decimal),
	i.currency,
	vm.sku as variant_sku,
	vm.product_id
from data_production_rds_datawarehouse_api_production.spree_line_items i
left join data_production_rds_datawarehouse_api_production.spree_orders o
 on i.order_id=o.id
left join data_production_rds_datawarehouse_salesforce_skyvia.orders s
on o."number" = s.spree_order_number__c
left join data_production_rds_datawarehouse_api_production.spree_variants vm
on vm.id = i.variant_id
),
order_items_final as (
select * from new_infra_final_order
union all
select * from old_order_infra
),
pre_final as (
select
oif.created_at,
oif.order_id,
oif.submitted_at,
oif.quantity,
oif.price,
oif.currency,
trim(cp.product_sku) as product_sku,
oif.variant_sku,
cp.brand,
cp.category_name as category,
om.duration,
cp.subcategory_name as subcategory,
cp.product_name as name
from order_items_final oif
left join category_product cp on oif.product_id = cp.product_id
left join data_production_rds_datawarehouse_order_approval.order_item om on oif.order_id = om.order_id)
,
market_price_closest as (
select val.mkp,
	   f.product_sku,
	   f.created_at,
	   case when date(f.created_at) > datum then  
	   date(f.created_at) - datum else datum-date(f.created_at) end as date_diff,
	   row_number() over(partition by f.order_id,f.product_sku order by case when date(f.created_at) > datum then  
	   date(f.created_at) - datum else datum-date(f.created_at) end asc  ) as rr,
	   f.order_id  
	   from pre_final f 
	   left join data_production_redshift.ods_production_grover_market_value_historical val on f.product_sku = val.product_sku where val.mkp is not null )
,final_mkp as (
select 
product_sku,
created_at,
order_id,
mkp
from market_price_closest where rr=1)
select oif.*,
case when mp.mkp is not null then  cast(mp.mkp as varchar(20)) else cast(fin.mkp as varchar(20)) end as market_price
from pre_final oif
left join market_price mp on oif.product_sku = mp.product_sku and cast(oif.created_at as date)>= datum  and  cast(oif.created_at as date)<coalesce (next_date,cast('2999-01-01' as date))
left join final_mkp fin on oif.product_sku = fin.product_sku and oif.created_at = fin.created_at and oif.order_id = fin.order_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14