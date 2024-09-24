drop table if exists dm_finance.asset_migration_historical;
create table  dm_finance.asset_migration_historical as 
with asset as
(
SELECT 
	reporting_date ,
	asset_id ,
	asset_name ,
	serial_number ,
	category ,
	subcategory ,
	capital_source_name ,
	invoice_number ,
	invoice_url ,
	purchased_date ,
	initial_price as purchase_price,
	sold_date ,
	sold_price ,
	dpd_bucket ,
	asset_status_original as asset_status,
	lag(asset_status_original,1) over (partition by asset_id order by reporting_date) as previous_asset_status
FROM 
dm_finance.spv_report_datatape s
where reporting_date =  last_day(reporting_date)
)
select
*,
case
	when previous_asset_status is null or asset_status is null then 'NO TRANSITION'
	when previous_asset_status = asset_status then 'NO TRANSITION'
	when previous_asset_status <> asset_status then previous_asset_status || ' -> ' || asset_status 
	else 'UNKNOWN'
end as transition_status
from 
asset;
