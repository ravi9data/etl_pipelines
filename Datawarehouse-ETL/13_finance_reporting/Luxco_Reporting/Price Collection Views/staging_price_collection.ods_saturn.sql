create or replace  view staging_price_collection.ods_saturn as (
with a as 
	(
	select 
		a.*,
		b.product_name,
		b.brand
	from ods_external.saturn_price_data  a 
	left join ods_production.asset b  on a.ean=b.product_ean 
	)
select 
distinct
	'GERMANY' as region,
	'SATURN_DIRECT_FEED' as src,
	week_date::date as week_date ,
	valid_from as reporting_month,
	artikelnummer::INTEGER as item_id,
	product_name,
	brand,
	a.product_sku,
	'EUR' as currency,
	price::decimal(38,2),
	price::decimal(38,2) as price_in_euro,
	'Neu' as asset_condition
from a 
)
with no schema binding
;