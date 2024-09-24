BEGIN;

DROP TABLE IF EXISTS sku_market_valuation_final;
CREATE TEMP TABLE sku_market_valuation_final as
with new_asset_valuation as(
select 
product_sku,
asset_count,
max_price as max_market_price,
min_price as min_market_price,
avg_price as avg_market_price,
months_since_last_valuation
from 
(
SELECT 
		ah.date,
		ah.asset_id ,
		ah.product_sku ,
		ah.asset_condition_spv,
		ah.residual_value_market_price,
		ah.last_valuation_report_date,
		COALESCE(datediff('month',ah.last_valuation_report_date,current_date),
					datediff('month',ah.date-30,current_date) )as months_since_last_valuation,
		ROW_NUMBER () OVER(PARTITION BY product_sku order by date desc) as latest_date,
		AVG(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as avg_price,
		MIN(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as min_price,
		MAX(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as max_price,
		count(asset_id) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as asset_count
FROM 
	master.asset_historical ah 
where 
	ah.asset_condition_spv = 'NEW'
	and residual_value_market_price > 0
)
where 
	latest_date = 1
) ,
agan_asset_valuation as(
select 
product_sku,
asset_count,
max_price as max_market_price,
min_price as min_market_price,
avg_price as avg_market_price,
months_since_last_valuation
from 
(
SELECT 
		ah.date,
		ah.asset_id ,
		ah.product_sku ,
		ah.asset_condition_spv,
		ah.residual_value_market_price,
		ah.last_valuation_report_date,
		COALESCE(datediff('month',ah.last_valuation_report_date,current_date),
					datediff('month',ah.date-30,current_date) )as months_since_last_valuation,
		ROW_NUMBER () OVER(PARTITION BY product_sku order by date desc) as latest_date,
		AVG(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as avg_price,
		MIN(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as min_price,
		MAX(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as max_price,
		count(asset_id) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as asset_count
FROM 
	master.asset_historical ah 
where 
	ah.asset_condition_spv = 'AGAN'
	and residual_value_market_price > 0
)
where 
	latest_date = 1
),
used_asset_valuation as(
select 
product_sku,
asset_count,
max_price as max_market_price,
min_price as min_market_price,
avg_price as avg_market_price,
months_since_last_valuation
from
(
SELECT 
		ah.date,
		ah.asset_id ,
		ah.product_sku ,
		ah.asset_condition_spv,
		ah.residual_value_market_price,
		ah.last_valuation_report_date,
		COALESCE(datediff('month',ah.last_valuation_report_date,current_date),
					datediff('month',ah.date-30,current_date) )as months_since_last_valuation,
		ROW_NUMBER () OVER(PARTITION BY product_sku order by date desc) as latest_date,
		AVG(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as avg_price,
		MIN(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as min_price,
		MAX(residual_value_market_price) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as max_price,
		count(asset_id) OVER(PARTITION BY product_sku,last_valuation_report_date ::date ROWS BETWEEN UNBOUNDED PRECEDING and unbounded FOLLOWING) as asset_count
FROM 
	master.asset_historical ah 
where 
	ah.asset_condition_spv = 'USED'
	and residual_value_market_price > 0
)
where 
	latest_date = 1
),
all_product_sku as
(
SELECT 
distinct
	a.product_sku
FROM 
	master.asset a
)
select
	CURRENT_TIMESTAMP as last_run_time,
	a.product_sku ,
	'NEW' as asset_condition_new,
	b.asset_count as asset_count_new,
	b.avg_market_price as avg_market_price_new,
	b.months_since_last_valuation as months_since_last_valuation_new,
	'AGAN' as asset_condition_agan,
	c.asset_count as asset_count_agan,
	c.avg_market_price as avg_market_price_agan,
	c.months_since_last_valuation as months_since_last_valuation_agan,
	'USED' as asset_condition_used,
	d.asset_count as asset_count_used,
	d.avg_market_price as avg_market_price_used,
	d.months_since_last_valuation as months_since_last_valuation_used
FROM 
all_product_sku a
	left join
	new_asset_valuation b
	on a.product_sku = b.product_sku
		left join
		agan_asset_valuation c
		on a.product_sku = c.product_sku
			left join
			used_asset_valuation d
			on a.product_sku = d.product_sku
;
DELETE FROM dwh.sku_market_valuation where 1=1;

INSERT INTO dwh.sku_market_valuation
select * from sku_market_valuation_final;
COMMIT;

/*Unload to S3 for rockset collections*/
UNLOAD ('SELECT * FROM dwh.sku_market_valuation') 
IAM_ROLE 'arn:aws:iam::031440329442:role/data-production-redshift-datawarehouse-s3-role' 
PARALLEL OFF ALLOWOVERWRITE FORMAT AS PARQUET MAXFILESIZE 2048 MB REGION AS 'eu-central-1';
