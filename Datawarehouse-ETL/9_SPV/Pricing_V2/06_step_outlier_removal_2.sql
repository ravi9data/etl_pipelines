
drop table if exists trans_dev.pricing_outlier_removal_2;
create table trans_dev.pricing_outlier_removal_2 as 
with d as (
/*###############################
Calculating Avg MM price per month
#################################*/
select 
reporting_month,
product_sku,
avg(price) as avg_mm_price
from
(select
	DISTINCT
	date_trunc('month',valid_from)::date as reporting_month,
  	trim(p.product_sku) as product_sku,
  	price::double precision as price
	from 
	stg_external_apis.mm_price_data m
		INNER join 
		ods_production.variant v 
		on v.ean::int8=m.ean
			left join 
			ods_production.product p 
			on p.product_id=v.product_id
	where 
		m.valid_from::date>DATEADD(day,-14, GETDATE()) 
		and v.ean SIMILAR to '[0-9]*'
		--and product_sku = 'GRB125P1170'
	union all
	select
	DISTINCT
	date_trunc('month',valid_from)::date as reporting_month,
  	trim(p.product_sku) as product_sku,
  	price::double precision as price
	from
	stg_external_apis.mm_price_data m
		INNER join 
		ods_production.variant v 
		on v.ean::int8=m.ean
			left join 
			ods_production.product p 
			on p.product_id=v.product_id
	where 
		m.valid_from::date>DATEADD(day,-14, GETDATE()) 
		and v.ean SIMILAR to '[0-9]*'
	)
	group by 
		1,2
),
b as (
/*##########################################################
Joining Avg MM and Saturn price per month with previous step
############################################################*/
select 
DISTINCT
	date_trunc('month',s.extract_date)::date as reporting_month,
	s.product_sku,
	mm.avg_mm_price
	from 
	ods_spv_historical.union_sources s 
		left join d mm  
		on date_trunc('month',s.extract_date::date)=mm.reporting_month 
		and s.product_sku=mm.product_sku
where  
	s.extract_date::date>DATEADD(day,-14, GETDATE()) 
	--and s.product_sku='GRB204P3935' 
order by 1)
, 
e as ( 
/*##################################
Filling MM prices where its empty
#################################*/
select 
DISTINCT
	reporting_month,
	product_sku,
	avg_mm_price,
	lead(avg_mm_price) ignore nulls over (partition by product_sku order by reporting_month) as next_price,
   	lag(avg_mm_price) ignore nulls over (partition by product_sku order by reporting_month) as previous_price
FROM  
	b
order by 
	reporting_month 
),
mm as (
select 
	reporting_month,
	product_sku,
	COALESCE(avg_mm_price,next_price,previous_price) as avg_mm_price
from e
),
g as (
SELECT 
distinct 
	date_trunc('month',purchased_date)::date as reporting_month, 
  	trim(asset.product_sku) as product_sku,
  	avg(asset.initial_price) AS avg_pp_price
FROM 
	ods_production.asset 
where 
	purchased_date::date>= DATEADD(day,-90, GETDATE()) 
	--  and product_sku = 'GRB150P833'
group by 
	1,2
),
h as (
select 
DISTINCT
	date_trunc('month',s.extract_date)::date as reporting_month,
	s.product_sku,
	mm.avg_pp_price
from 
	ods_spv_historical.union_sources s 
		left join g mm  
		on date_trunc('month',s.extract_date::date)=mm.reporting_month 
		and s.product_sku=mm.product_sku
		--where s.product_sku='GRB246P4411'
order by 
	1 ),
j as ( 
select 
DISTINCT
	reporting_month,
	product_sku,
	avg_pp_price,
	lead(avg_pp_price) ignore nulls over (partition by product_sku order by reporting_month) as next_price,
   	lag(avg_pp_price) ignore nulls over (partition by product_sku order by reporting_month) as previous_price
FROM  
	h
order by 
	reporting_month 
), 
check_ as (
select 
	reporting_month,
	product_sku,
	COALESCE(avg_pp_price,next_price,previous_price) as avg_pp_price
from 
	j),
prep as (
select 
	s.*,
	mm.avg_mm_price,
	c.avg_pp_price,
	coalesce(mm.avg_mm_price,c.avg_pp_price*1.1) as ref_price
	from 
	ods_spv_historical.union_sources s 
		inner join 
		ods_production.product p 
		on p.product_sku=s.product_sku
			left join mm 
 			on date_trunc('month',s.extract_date::date)=mm.reporting_month
 			and s.product_sku=mm.product_sku
				left join check_ c 
 				on date_trunc('month',s.extract_date::date)=c.reporting_month
 				and s.product_sku=c.product_sku
	where true 
	and s.extract_date::date>DATEADD(day,-14, GETDATE()) 
	and asset_condition='Neu'
),
m as (
select 
	current_date::date-1 as reporting_date,
	prep.*,
	case 
		when src='MEDIAMARKT_DIRECT_FEED' then 1 
		when ( (ref_price is  null) or ( (ref_price-prep.price)/ref_price*100 between -20.00 and 20.00)) then 1 
	else 0 end as is_outlier,
max(prep.extract_date::date) OVER (PARTITION BY prep.src,prep.product_sku, prep.asset_condition, preP.extract_date ) AS max_available_date,
rank() 
 OVER (PARTITION BY preP.src, preP.product_sku, preP.asset_condition, preP.extract_date ORDER BY price, prep.itemid, prep.extract_date) AS price_rank,
rank() 
 OVER (PARTITION BY preP.src, preP.product_sku, preP.asset_condition, preP.reporting_month ORDER BY price, prep.itemid, prep.extract_date) AS price_rank_month
from prep 
order by 3 desc
)select * from m where is_outlier=1
;

