--Step2: Removing Outliers
delete from ods_spv_historical.spv_used_asset_price where reporting_date=(current_Date-1) or reporting_date<>last_day(reporting_date) ;
insert into ods_spv_historical.spv_used_asset_price 
with d as (
/*#############################################
 Calculating Avg MM and Saturn price per month
###############################################*/
select 
	reporting_month,
	product_sku,
	avg(avg_mm_price) as avg_mm_price
from(
	select
		distinct 
		date_trunc('month', reporting_month::date)::date::date as reporting_month,
		trim(product_sku) as product_sku,
		price::double precision as avg_mm_price
	from
		ods_external.spv_used_asset_price_mediamarkt_directfeed
	where
		asset_condition = 'Neu'
		and src = 'MEDIAMARKT_DIRECT_FEED'
	union all 
	select
		distinct 
		date_trunc('month', week_date::date)::date::date as reporting_month,
		trim(product_sku) as product_sku,
		price::double precision as avg_mm_price
	from
		ods_external.saturn_price_data
	)
group by
	1,2 
),b as (
       /*################################
     Joining Avg MM price per month with previous step
     #################################*/
select DISTINCT
	date_trunc('month',s.extract_date)::date as reporting_month,
--	mm.reporting_month,
	s.product_sku,
	mm.avg_mm_price
from ods_spv_historical.union_sources s 
left join d mm  on date_trunc('month',s.extract_date::date)=mm.reporting_month and s.product_sku=mm.product_sku
--where s.product_sku='GRB94P3748' and s.product_sku='GRB94P3748'--and mm.asset_condition='Wie neu' 
order by 1 )
, e as ( 
      /*################################
    Filling MM prices where its empty
     #################################*/
select DISTINCT
	reporting_month,
	product_sku,
	avg_mm_price,
	lead(avg_mm_price) 
   	 ignore nulls over (partition by product_sku order by reporting_month) as next_price,
   	 lag(avg_mm_price) 
   	 ignore nulls over (partition by product_sku order by reporting_month) as previous_price
 FROM  b
order by reporting_month 
),mm as (
select 
	reporting_month,
	product_sku,
	COALESCE(avg_mm_price,next_price,previous_price) as avg_mm_price
from e
)
, g as (
SELECT distinct 
  date_trunc('month',purchased_date)::date as reporting_month, 
  trim(asset.product_sku) as product_sku,
  avg(asset.initial_price) AS avg_pp_price
 FROM ods_production.asset 
 where purchased_date::date <= current_date
--  and product_sku = 'GRB150P833'
  group by 1,2
),h as (
select DISTINCT
	date_trunc('month',s.extract_date)::date as reporting_month,
--	mm.reporting_month,
	s.product_sku,
	mm.avg_pp_price
from ods_spv_historical.union_sources s 
left join g mm  on date_trunc('month',s.extract_date::date)=mm.reporting_month and s.product_sku=mm.product_sku
--where s.product_sku='GRB246P4411'-- and s.product_sku='GRB94P3748'--and mm.asset_condition='Wie neu' 
order by 1 )
, j as ( 
select DISTINCT
	reporting_month,
	product_sku,
	avg_pp_price,
	lead(avg_pp_price) 
   	 ignore nulls over (partition by product_sku order by reporting_month) as next_price,
   	 lag(avg_pp_price) 
   	 ignore nulls over (partition by product_sku order by reporting_month) as previous_price
 FROM  h
order by reporting_month 
), check_ as (
select 
	reporting_month,
	product_sku,
	COALESCE(avg_pp_price,next_price,previous_price) as avg_pp_price
from j)
,prep as (
select 
 s.src,s.extract_date,s.reporting_month,s.item_id,s.product_sku,s.asset_condition,s.currency,s.price,
 mm.avg_mm_price,
 c.avg_pp_price,
 coalesce(mm.avg_mm_price,c.avg_pp_price*1.1) as ref_price,
 round((s.price::double precision/
        case when coalesce(mm.avg_mm_price,c.avg_pp_price*1.1)=0 then null else coalesce(mm.avg_mm_price,c.avg_pp_price*1.1) end
       )*100,2) as coeff,
 case when round((s.price::double precision/
                  case when coalesce(mm.avg_mm_price,c.avg_pp_price*1.1)=0 then null else coalesce(mm.avg_mm_price,c.avg_pp_price*1.1) end
                 )*100,2) is null then null 
 else MEDIAN(round((s.price::double precision/
                    case when coalesce(mm.avg_mm_price,c.avg_pp_price*1.1)=0 then null else coalesce(mm.avg_mm_price,c.avg_pp_price*1.1) end
                   )*100,2)) 
  OVER (partition by s.product_sku,asset_condition) end as median_coeff
from ods_spv_historical.union_sources s 
inner join ods_production.product p on p.product_sku=s.product_sku
left join mm 
 on date_trunc('month',s.extract_date::date)=mm.reporting_month
 and s.product_sku=mm.product_sku
left join check_ c 
 on  date_trunc('month',s.extract_date::date)=c.reporting_month
 and s.product_sku=c.product_sku
where true
),
c as (
  select product_sku,asset_condition,avg(median_coeff) as median_coeff from prep group by 1,2
  )
select 
--coeff-median_coeff,
current_date::date-1 as reporting_date,
prep.*,
max(prep.extract_date::date) OVER (PARTITION BY prep.src,prep.product_sku, prep.asset_condition) AS max_available_date,
rank() 
 OVER (PARTITION BY preP.src, preP.product_sku, preP.asset_condition, preP.extract_date ORDER BY price, prep.item_id, prep.extract_date) AS price_rank,
rank() 
 OVER (PARTITION BY preP.src, preP.product_sku, preP.asset_condition, preP.reporting_month ORDER BY price, prep.item_id, prep.extract_date) AS price_rank_month
from prep 
left join  c on c.product_sku=prep.product_sku and c.asset_condition=prep.asset_condition
where  ((ref_price is  null) or ((coeff-c.median_coeff) between -10.00 and 10.00))--((coeff::int >= median_coeff::int-15) or (coeff::int <= median_coeff::int+15)))
--and product_sku='GRB150P85' and asset_condition='Wie neu'
order by 3 desc;
