DROP TABLE IF EXISTS dm_operations.inventory_asset_overview;
CREATE TABLE dm_operations.inventory_asset_overview as
with sf_assets as (
	select 
		ah.serial_number ,
		ah.asset_id ,
		ah.asset_status_original ,
        ah.asset_name,
		ah.warehouse as sf_warehouse,
		ah.variant_sku ,
		ah.category_name,
		case when ah.asset_condition_spv = 'NEW' then 'NEW'::varchar(10) else 'AGAN'::varchar(10) end asset_condition,
		t.final_condition,
		ah.residual_value_market_price,
		ah.asset_value_linear_depr,
		(select max(createddate::timestamp) 
			from stg_salesforce.asset_history 
			where field = 'Status'
			and newvalue = ah.asset_status_original
			and assetid = ah.asset_id) status_updated_at
	from master.asset_historical ah
	left join ods_production.asset t on t.asset_id = ah.asset_id 
	where ah.date = dateadd('day', -1 , current_date)
	and ah.warehouse not in ('office_us', 'ups_softeon_us_kylse', 'office_de')
),
wh_assets as (
	select 
		 seriennummer as serial_number,
		 wemalo_sku as wh_sku,
		 name as wh_name,
		 warehouse ,
		 sperrlager_type as wh_status
	from dwh.wemalo_sf_reconciliation 
	where reporting_date = current_date--(select max(reporting_date) from dwh.wemalo_sf_reconciliation)
		--and seriennummer not in (select distinct serial_number from sf_assets)
  		and warehouse in ('Kiel', 'Hagenow', 'Lüneburg', 'WH Transfer')
	union 
	select 
		serial_number ,
		sku as wh_sku,
		description as wh_name,
		'Roermond' as warehouse,
		disposition_cd as wh_status
	from dm_operations.ups_eu_reconciliation 
	where report_date = current_date
		--and serial_number not in (select distinct serial_number from sf_assets)
	union
	select 
		distinct serial_number,
		item_number as wh_sku ,
		null as wh_name,
		'Ingram Micro' as warehouse ,
		disposition_code as wh_status
	from ods_operations.ingram_inventory 
	where reporting_date = (select max(reporting_date) from ods_operations.ingram_inventory)
		and is_in_warehouse = 1
		--and serial_number not in (select distinct serial_number from sf_assets)
),
dimensions as (
	select 
		variant_sku ,
		(
		case when is_dimension_missing then category_height else original_height end 
		*
		case when is_dimension_missing then category_width else original_width end 
		*
		case when is_dimension_missing then category_length else original_length end 
		) / 1000000 as volume --cm3 to m3
	from dm_operations.dimensions 
)
select 
	sf.serial_number,
    sf.asset_id,
    sf.asset_status_original,
    sf.asset_name,
    sf_warehouse,
    sf.variant_sku,
    sf.category_name,
    sf.asset_condition,
    sf.final_condition,
    sf.residual_value_market_price,
    sf.asset_value_linear_depr,
    sf.status_updated_at,
	wh.wh_sku,
	wh.wh_name,
	wh.warehouse,
	wh.wh_status,
    wh.serial_number as wh_serial_number,
	d.volume
from sf_assets sf 
full join wh_assets wh 
on sf.serial_number = wh.serial_number 
left join dimensions d 
on coalesce (sf.variant_sku, wh.wh_sku) = d.variant_sku
where not (sf.asset_status_original in ('IN DEBT COLLECTION', 'ON LOAN', 'SOLD', 'WRITTEN OFF DC') 
           and wh.serial_number is null);

GRANT SELECT ON dm_operations.inventory_asset_overview TO tableau;
GRANT SELECT ON dm_operations.inventory_asset_overview TO GROUP recommerce;
