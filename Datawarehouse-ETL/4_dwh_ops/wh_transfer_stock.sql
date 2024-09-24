drop table if exists dm_operations.wh_stock_transfer;
create table dm_operations.wh_stock_transfer as 

with rawd as (
select 
	asset_id,
	product_sku ,
    category_name,
	date  as fact_date,
	warehouse ,
	asset_status_original,
	 case when asset_status_original in ('IN STOCK', 'TRANSFERRED TO WH') 
	 then asset_status_original 
			 else 'OTHER TRANSIT' end new_status,
	initial_price ,
    --an asset can be in TRANSFERRED TO WH status in have multiple time
    --we need to take minimum date of each period
	row_number() over (partition by asset_id order by date desc) as seqnum,
	row_number() over (partition by asset_id, asset_status_original order by date desc ) seqnum_2
from master.asset_historical 
	where 
        --let make it aligned with portfolio overview report.
		(date, product_sku) 
			in 
				(select distinct
						fact_day ,
						product_sku
					from dwh.product_reporting  
                    --last 6 months will be enough
					where fact_day >= dateadd('month', -6, current_date)
					)
		and asset_status_original in (
			  'RESERVED',
		      'SHIPPED' ,
		      'READY TO SHIP',
		      'TRANSFERRED TO WH' ,
		      'CROSS SALE' ,
		      'DROPSHIPPED',
		      'PURCHASED',
		      'IN STOCK'
			)  
		and warehouse in ('synerlogis_de', 'office_us', 'ups_softeon_us_kylse', 'office_de', 'ups_softeon_eu_nlrng', 'ingram_micro_eu_flensburg') 
),
days_calc as (
select 
	asset_id ,
	fact_date ,
    category_name,
	asset_status_original,
	new_status ,
	warehouse ,
	product_sku ,
	initial_price,
    --one asset can be in same status in different times
    --if we group them only by status, then query will give as the earliest date
    --that the asset was in that status for the first time ever.
    --difference between seqnum - seqnum2 will allow us to group per each period of status 
	datediff('day', min(fact_date) over (partition by asset_id , seqnum - seqnum_2 ), fact_date) days_in_status,
	case when days_in_status < 4 then '1-3 Day'
		 when days_in_status < 8 then '4-7 Day'
		 when days_in_status < 15 then '8-14 Day'
		 else '+14 Day' end as status_day_bucket
from rawd 
where product_sku is not null
)
--in order not to have all rows, group them all
select 
	fact_date ,
    category_name,
	asset_status_original,
	new_status ,
	warehouse ,
	product_sku ,
	status_day_bucket,
	count(asset_id) as total_assets,
	sum(initial_price) as total_price
from days_calc 
group by 1,2,3,4,5,6,7;

GRANT SELECT ON dm_operations.wh_stock_transfer TO tableau;
