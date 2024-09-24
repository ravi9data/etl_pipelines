drop table if exists dm_operations.ups_us_quarantine_inventory_blancco;
create table dm_operations.ups_us_quarantine_inventory_blancco as
with quarantine_data_enriched_with_SF as(
select u.reporting_date as report_date
,u.vendor_serial as serial_number
,ah.asset_name
,ah.variant_sku
,u.date_received_gmt as goods_in
,u.rec_ref_2 as return_bucket
,ah.initial_price
,ah.category_name
,ah.subcategory_name 
from stg_external_apis.ups_us_quarantine_inventory u
left join master.asset_historical ah 
--we receive report in early hour of new day
--so we need to take previous day's latest snapshot
--of master asset historical.
on u.reporting_date = dateadd('day', 1, ah."date")
and u.vendor_serial = ah.serial_number)

,quarantine_data_enriched_with_blancco as(
select q.*
,bd.blancco
,bd."extra sanitation" 
from quarantine_data_enriched_with_SF q
left join ods_operations.blancco_data bd on q.variant_sku = bd."part number")

--CTE
select * from quarantine_data_enriched_with_blancco;

GRANT SELECT ON dm_operations.ups_us_quarantine_inventory_blancco TO tableau;
