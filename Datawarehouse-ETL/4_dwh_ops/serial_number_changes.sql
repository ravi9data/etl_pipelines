drop table if exists dm_operations.serial_number_changes;
create table dm_operations.serial_number_changes as
select ah.assetid as asset_id,
ah.oldvalue as old_serial_number,
ah.newvalue as serial_number,
ah.createddate as serial_number_change_date,
a.variant_sku,
v.variant_name
from stg_salesforce.asset_history ah
inner join ods_production.asset a on a.asset_id=ah.assetid
left join ods_production.variant v on v.variant_sku=a.variant_sku
where ah.field ='SerialNumber';

GRANT SELECT ON dm_operations.serial_number_changes to hightouch;