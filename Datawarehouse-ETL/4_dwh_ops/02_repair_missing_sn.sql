drop table if exists hightouch_sources.repair_missing_sn;
create table hightouch_sources.repair_missing_sn as

select 
    'cost_estimates' as source,
    file_name ,
    kva_id ,
    serial_number ,
    device ,
    description ,
    repair_partner 
from dm_recommerce.repair_cost_estimates 
where asset_name is null
union all
select 
    'invoices' as source,
    null as file_name ,
    kva_id ,
    serial_number ,
    repaired_asset_name ,
    repair_description ,
    repair_partner 
from dm_recommerce.repair_invoices 
where asset_name is null
;