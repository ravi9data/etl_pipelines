insert into ods_spv_historical.lifetime_historical
select *,current_date-2 as archive_date
from ods_spv_historical.lifetime;

insert into ods_spv_historical.sub_recon_historical
select *,current_date-2 as archive_date
from ods_spv_historical.sub_recon;

insert into ods_spv_historical.due_lifetime_historical
select *,current_date-2 as archive_date
from ods_spv_historical.due_lifetime;

insert into ods_spv_historical.paid_reporting_month_historical
select *,current_date-2 as archive_date
from ods_spv_historical.paid_reporting_month;

insert into ods_spv_historical.due_reporting_month_historical
select *,current_date-2 as archive_date
from ods_spv_historical.due_reporting_month;

insert into ods_spv_historical.cust_historical
select *,current_date-2 as archive_date
from ods_spv_historical.cust;

insert into ods_spv_historical.cashflow_due_historical
select *,current_date-2 as archive_date
from ods_spv_historical.cashflow_due;

insert into ods_spv_historical.cashflow_historical
select *,current_date-2 as archive_date
from ods_spv_historical.cashflow;

insert into ods_spv_historical.cancelled_value_historical
select *,current_date-2 as archive_date
from ods_spv_historical.cancelled_value;

insert into ods_spv_historical.asset_category_historical
select *,current_date-2 as archive_date
from ods_spv_historical.asset_category;

insert into ods_spv_historical.active_value_historical
select *,current_date-2 as archive_date
from ods_spv_historical.active_value;

insert into ods_spv_historical.sub_recon_historical
select *,current_date-2 as archive_date
from ods_spv_historical.sub_recon;

insert into ods_spv_historical.subs_per_asset_historical
select *,current_date-2 as archive_date
from ods_spv_historical.subs_per_asset;

insert into ods_spv_historical.supplier_historical
select *,current_date-2 as archive_date
from ods_spv_historical.supplier;
