--assets_in_transit_report
drop view if exists dm_finance.v_assets_in_transit_report;
create view dm_finance.v_assets_in_transit_report as
select 
*
from master.purchase_request_item_historical 
where 
(date = last_day(date) or date = current_date -1)
and (purchase_date::date <=date_trunc('month',current_date)-1 or purchase_date is null)
with no schema binding;