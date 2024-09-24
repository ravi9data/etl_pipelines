create or replace  view staging_price_collection.ods_mediamarkt as 
(
select 
	DISTINCT
	'GERMANY' as region,
	src,
	reporting_month,
	(case when itemid = 'Null' then '0' else itemid end)::INTEGER as item_id,-- it is all null as a text and we nned to have INT as other views
	product_sku,
	currency,
	price::decimal(38,2),
	price::decimal(38,2) as price_in_euro,
	asset_condition
from ods_external.spv_used_asset_price_mediamarkt_directfeed m 
)
with no schema binding;