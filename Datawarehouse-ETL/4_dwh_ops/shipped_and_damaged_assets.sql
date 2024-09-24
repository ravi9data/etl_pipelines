drop table if exists dm_operations.shipped_and_damaged_assets;
create table dm_operations.shipped_and_damaged_assets as
with allocation_pre as(
select as2.allocation_id
,as2.asset_id 
,as2.shipping_country 
,as2.region 
,as2.customer_type
,as2.carrier 
,coalesce(a.shipment_at, a.delivered_at)::date as fact_date
,a.issue_reason
from ods_production.allocation a 
inner join ods_operations.allocation_shipment as2 on a.allocation_id = as2.allocation_id 
)

,allocation as(
select a.*
,ah.asset_condition
,a2.category_name
,a2.subcategory_name
,a2.product_sku
,a2.product_name
,case 
when a2.subcategory_name = 'TV' then regexp_substr(replace(replace(replace(a2.product_name , '(', ''), ')', ''), '-', ''), '\\d{2}\\d?')
when a2.subcategory_name = 'Monitors'
	then case when length(substring(a2.product_name,position('"' in a2.product_name)-2,2 ))=0 then regexp_substr(replace(replace(replace(a2.product_name , '(', ''), ')', ''), '-', ''), '\\d{2}\\d?') 
		     when substring(a2.product_name,position('"' in a2.product_name)-4,4) like '%.%' then substring(a2.product_name,position('"' in a2.product_name)-4,4)
	else substring(a2.product_name,position('"' in a2.product_name)-2,2 ) end
else null end as inches
,case when inches is null or inches='' then null::text
	  when inches::float>=49 then '>49"'::text 
	  when inches::float<49 then '<49"'::text
	  end as inch_bucket
from allocation_pre a
left join master.asset a2 on a.asset_id =  a2.asset_id 
--check the asset's condition on shipment date
left join master.asset_historical ah on ah.asset_id = a.asset_id and ah."date" = a.fact_date
)

,shipments as(
select 
 fact_date
,region
,shipping_country
,carrier
,customer_type
,asset_condition
,category_name
,subcategory_name
,product_sku
,product_name
,case when inches is null or inches='' then null 
 else inches::float end as inches
,inch_bucket
,count(case when issue_reason in('DOA','Asset Damage','Limited Function','Incomplete') then asset_id end) as damaged_assets
,count(asset_id) as all_shipments
from allocation 
group by 1,2,3,4,5,6,7,8,9,10,11,12
)
select *
from shipments
where carrier in('DHL','UPS','HERMES')
and shipping_country in('Austria','Germany','Netherlands','Spain');

GRANT SELECT ON dm_operations.shipped_and_damaged_assets TO tableau;
