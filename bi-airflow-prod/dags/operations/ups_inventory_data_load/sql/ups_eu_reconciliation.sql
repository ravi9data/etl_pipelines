drop table if exists reporting_dates_ups;
create temp table reporting_dates_ups as
select distinct report_date
from stg_external_apis.ups_nl_oh_inventory;


drop table if exists dm_operations.ups_eu_reconciliation;
create table dm_operations.ups_eu_reconciliation as

select
	u.report_date ,
	case when u.report_date = (select max(report_date) from stg_external_apis.ups_nl_oh_inventory)
		 then true else false end as is_latest_report,
	u.sku,
	u.description ,
	translate(u.unit_srl_no , CHR(29), '') as serial_number ,
	u.disposition_cd ,
	u.unit_status ,
	u.rcpt_closed ,
    u.po_no,
	a.asset_id ,
	a.asset_status_original,
	a.asset_name,
	a.product_sku,
	a.variant_sku,
	a.ean,
	a.category_name ,
	a.subcategory_name ,
    lag(u.report_date) over (partition by u.unit_srl_no order by u.report_date) as prev_report_date,
	case when prev_report_date is null
	     then 'First Entry'
	     when datediff('day', prev_report_date , u.report_date) > 1
	     	  and prev_report_date not in (select report_date from reporting_dates_ups)
	     then 'Re-Entry'
	     end entry_type,
	case when u.disposition_cd = 'UNALLOCABLE'
		then
		case when a.asset_status_original  = 'INBOUND UNALLOCABLE'
			 then 'UNALLOCABLE'
			 when a.asset_status_original is null
			 then 'MISMATCH'
			 else 'OTHER STATUS'
			 end
		 else
		 case when a.asset_status_original  = 'INBOUND UNALLOCABLE'
			  then 'ONLY UNALLOCABLE IN SF'
		 end
	end as unallocable_bucket
from stg_external_apis.ups_nl_oh_inventory u
left join master.asset_historical a
on translate(u.unit_srl_no , CHR(29), '') = a.serial_number
   and u.report_date = dateadd('day', 1, a."date")
   where u.unit_srl_no is not null;


grant all on dm_operations.ups_eu_reconciliation to group bi;
grant all on dm_operations.ups_eu_reconciliation to bart;
GRANT SELECT ON dm_operations.ups_eu_reconciliation TO tableau;
