--histogram_sku_prices_report_sheet1
drop view if exists dm_finance.v_histogram_sku_prices_report;
create view dm_finance.v_histogram_sku_prices_report as
with a as (	
	select 
	product_sku,
	avg_price
from ods_external.spv_used_asset_price_master
where asset_condition='Neu'
)
select 
	p.product_sku, 
	p.asset_condition,
	p.avg_price,
	a.avg_price as new_price,
	round((p.avg_price/a.avg_price)*100,2) as coeff
from ods_external.spv_used_asset_price_master p
left join a on p.product_sku=a.product_sku
with no schema binding;