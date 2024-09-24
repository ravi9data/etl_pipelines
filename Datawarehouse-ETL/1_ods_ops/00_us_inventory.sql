drop table if exists ods_external.us_inventory;
create table ods_external.us_inventory as

with us_inventory as (
select 
	date_trunc('day', received_at) as report_date ,
	dense_rank() over (order by report_date desc) as day_order,
	warehouse,
	location as "position",
	item_description as asset_name, 
	item_number as variant_sku,
	vendor_serial as serial_number,
	account,
	case when right(date_received_gmt, 1) = 'M' 
		 then to_date(date_received_gmt, 'MM/DD/YYYY HH:MI:SS')
		 else to_date(date_received_gmt, 'YYYY-MM-DD HH:MI:SS') end as goods_in,
    rec_ref_1, --PR number
    rec_ref_2  --Return bucket
from stg_external_apis.us_inventory_info)
select 
	u.report_date,
	u.day_order ,
	u.warehouse ,
	u."position" ,
	u.asset_name ,
	u.variant_sku ,
	u.serial_number ,
	u.account ,
	u.goods_in ,
    u.rec_ref_1 as pr_number, --PR number
	u.rec_ref_2 as return_bucket, --Return bucket
	ah.asset_id ,
	ah.asset_status_original ,
	ah.initial_price ,
	ah.category_name ,
	ah.subcategory_name ,
	case when u.report_date = min(u.report_date) over (partition by u.serial_number)	
		 then 1 end as is_new_entry,
	case when u.report_date = max(u.report_date) over (partition by u.serial_number)	
		 then 1 end as is_last_entry,
	case when u.report_date = min(case when ah.asset_id is not null then u.report_date end) over (partition by u.serial_number)
		 then date_diff ( 'day', 
		 				  min(u.report_date) over (partition by u.serial_number),
		 				  min(case when ah.asset_id is not null then u.report_date end) over (partition by u.serial_number)) end as days_before_booking,
	case when a.asset_id is null then false else true end as is_active
from us_inventory u 
left join master.asset_historical ah 
--we receive report in early hour of new day
--so we need to take previous day's latest snapshot
--of master asset historical.
on u.report_date = dateadd('day', 1, ah."date")
and u.serial_number = ah.serial_number 
left join master.asset a
on u.serial_number = a.serial_number
;

GRANT SELECT ON ods_external.us_inventory TO tableau;
