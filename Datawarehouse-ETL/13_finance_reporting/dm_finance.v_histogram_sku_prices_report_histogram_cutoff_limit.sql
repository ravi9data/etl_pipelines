--histogram_sku_prices_report_histogram_cutoff_limit
drop view if exists dm_finance.v_histogram_sku_prices_report_cutoff_limit_histogram;
create view dm_finance.v_histogram_sku_prices_report_cutoff_limit_histogram as 
with a as (	
	select 
	date_trunc('month',reporting_month::date) as reporting_month,
	product_sku,
	max(price) as avg_price
from ods_external.spv_used_asset_price
where asset_condition='Neu' and src='MEDIAMARKT_DIRECT_FEED'
group by 1,2
)
select 
	p.product_sku,
	p.reporting_month,
	p.src,
	p.price,
	p.asset_condition,
--	p.avg_price,
	a.avg_price as new_price,
	round((p.price/a.avg_price)*100,2) as coeff
from ods_external.spv_used_asset_price p
left join a on p.product_sku=a.product_sku and date_trunc('month',p.reporting_month::date)::date=a.reporting_month::Date
with no schema binding;