--eiffel_v1_report_deemed_assets
drop view if exists dm_finance.v_eiffel_v1_report_deemed_assets;
create view dm_finance.v_eiffel_v1_report_deemed_assets as 
select * from ods_production.asset
where asset_status_original in ('RETURNED TO SUPPLIER','BUY NOT APPROVED','DELETED','BUY')
with no schema binding;