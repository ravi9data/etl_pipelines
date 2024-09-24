
drop table if exists trans_dev.pricing_final;
create table trans_dev.pricing_final as 
with a as(
/*###############################
Calculating Avg MM price per month
#################################*/
select 
product_sku,
avg(price) as avg_price_mm_saturn
from
(select
	DISTINCT
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
		1
),
b as
(
select * from 
	(select 
		product_sku,
		date_trunc('week',created_at) as created_at,
		avg(initial_price) as initial_price ,
		row_number() over(partition by product_sku order by date_trunc('week',created_at) desc) as rn
	from 
	master.asset 
	where 
	purchased_date::date>= DATEADD(day,-90, GETDATE()) 
	group by 
		1,2 )
where rn = 1   
),
c as (
	select 
	DISTINCT  
		product_sku, 
		min(price)::decimal(10,2) as absolute_lowest_amazon_with_outlier_detection
		from 
		trans_dev.pricing_outlier_removal_ranked
		where 
			src = 'AMAZON'  
			and asset_condition = 'Neu'
			and extract_date > DATEADD(day,-14, GETDATE())::date
		group by 
			1 
),
d as (
select DISTINCT
	product_sku, 
	min(price)::decimal(10,2) as absolute_lowest_amazon_without_outlier_detection
	from 
		ods_spv_historical.union_sources s 
	where  
		extract_date > DATEADD(day,-14, GETDATE()) :: date 
		and src ='AMAZON' 
		and asset_condition ='Neu'
	group by 1
) ,
f as (
SELECT 
	b.product_sku,
	avg(b.price) ::decimal(10,2) as avg_3_lowest_price_without_outlier_detection
	from 
	(
		SELECT 
			product_sku,
			price,
			price_rank
			from
			(
				select 
				distinct
				rank() OVER(Partition by product_sku order by price) as price_rank,
				round(price,2) as price,
				product_sku
				from 
					ods_spv_historical.union_sources s 
				where  
					s.extract_date >DATEADD(day,-14, GETDATE()) :: date
					and src ='AMAZON'
					and asset_condition = 'Neu'
--and product_sku = 'GRB198P10244'
			)
		where 
			price_rank <=3
	)b
group by 
	b.product_sku
)
select 
	current_date-1 as reporting_date,
	asset.product_sku,
	asset.ltd_avg_purchase_price,
	asset.months_since_last_purchase,
	COALESCE 
	((case
		when asset_condition='Neu' and a.avg_price_mm_saturn is not null then (final_market_price+a.avg_price_mm_saturn)/2
		else final_market_price
	end),a.avg_price_mm_saturn) as new_price,
	(new_price/a.avg_price_mm_saturn)-1 as "new_vs_mm_sat",
	a.avg_price_mm_saturn as avg_price_mm_saturn,
	b.initial_price as pp_price,
	absolute_lowest_amazon_with_outlier_detection,
	absolute_lowest_amazon_without_outlier_detection,
	m.avg_price as avg_3_lowest_price_with_outlier_detection,
	avg_3_lowest_price_without_outlier_detection,
	fin.reporting_date as finco_valuation_date,
	fin.new_price_finco,
	fin.months_since_last_valuation_new,
	fin.agan_price_finco,
	fin.months_since_last_valuation_agan,
	fin.used_price_finco,
	fin.months_since_last_valuation_used
	from 
	trans_dev.master_sku_list asset
		left join
		trans_dev.pricing_spv_used_asset_price_master m
		on m.product_sku = asset.product_sku 
			left join a 
			on a.product_sku=asset.product_sku
				left join b 
				on b.product_sku=asset.product_sku
					left join c 
					on c.product_sku=asset.product_sku
						left join d 
						on d.product_sku=asset.product_sku
							left join f 
							on f.product_sku=asset.product_sku
								left join trans_dev.pricing_finco_data fin
								on fin.product_sku= asset.product_sku
;
