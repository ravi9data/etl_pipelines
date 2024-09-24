-- This script is a placeholder in the Wemalo import job in Matillion. Will be updated in future

drop table if exists ods_external.wemalo_inventory;
create table ods_external.wemalo_inventory as
with wemalo as
(select
	*,
	lag(reporting_date) over (partition by seriennummer order by reporting_date) as previous_reporting_date
from stg_external_apis.wemalo_inventory
where seriennummer != ''),
 asset_historical as
(
select *
from master.asset_historical
where serial_number != ''
)
SELECT w.seriennummer, merkmal, stellplatz, ah.asset_status_original, ah.asset_id,ah.product_name,ast.final_condition,ah.asset_allocation_id, reporting_date,
case when previous_reporting_date is null then True else False end as is_new_entry,
previous_reporting_date,
ah.initial_price,
ah.category_name,
ah.subcategory_name,
ast.asset_status_original as current_status,
allocation_status_original as current_allocation_status
from wemalo w
left join asset_historical ah on ah.serial_number = w.seriennummer and ah."date" = w.reporting_date
left join ods_production.asset ast on ast.serial_number = w.seriennummer
left join ods_production.allocation a on ast.asset_allocation_id = a.allocation_id;

drop table if exists ods_external.wemalo_inventory;
