drop table if exists monitoring.FI2 ;

create table monitoring.FI2 as 
select i.*, a.return_delivery_date
from ods_production.insurance_allocation i
left join ods_production.allocation a on a.allocation_id=i.asset_allocation_id
where true
and contract_end_date is null
and a.return_delivery_date is not null
order by return_Delivery_Date desc;