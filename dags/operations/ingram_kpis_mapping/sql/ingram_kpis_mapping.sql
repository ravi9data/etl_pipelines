truncate table recommerce.ingram_kpi_mapping;

insert into recommerce.ingram_kpi_mapping
select disposition_code, 
status_code, 
kpi, 
location_code_b2b_and_b2b_bulky, 
stages,
steps_4r,
detailed_steps,
sf_asset_status,
is_in_warehouse 
from staging_airbyte_bi.ingram_kpis_mapping ;

GRANT SELECT ON staging.ingram_kpis_mapping TO tableau;
