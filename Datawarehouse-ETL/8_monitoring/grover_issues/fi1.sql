drop table if exists monitoring.fi1;
create table monitoring.fi1 as 
select * 
from ods_production.insurance_allocation
where open_contracts_per_allocation > 1
and contract_end_date is null;

GRANT SELECT ON monitoring.fi1 TO tableau;
