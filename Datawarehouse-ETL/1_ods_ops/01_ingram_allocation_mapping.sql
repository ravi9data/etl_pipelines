	
--
-- once dbt and entire data flow is ready
-- then ingram_first_return_event part should be moved to 00_ingram_micro_orders.sql
-- 
--
drop table if exists ingram_first_return_event;
create temp table ingram_first_return_event as 
with events as (
select 
	order_number ,
	disposition_code ,
	status_code ,
	source_timestamp ,
	coalesce (
		--
		-- when we get first return event?
		-- before april 19, we were checking disposition code
		-- https://github.com/devsbb/wms/commit/5fbeffa4a253468db5ffc93a162b49b10c6f8eaa
		--
		first_value (case when (disposition_code in ('GOODS_IN', 'RETURNED', 'CLARIFICATION', 'GRADING') and source_timestamp <= '2023-04-19') or 
							   (status_code = 'RETURN RECEIVED' and source_timestamp > '2023-04-19')
						 then serial_number end ignore nulls)
			over (partition by order_number order by source_timestamp asc rows between unbounded preceding and unbounded following) ,
		--
		-- if no return available, then take first non-clarification event
		--
		first_value (case when disposition_code <> 'CLARIFICATION' then serial_number end ignore nulls)
			over (partition by order_number order by source_timestamp asc 
				  rows between unbounded preceding and unbounded following) 
			  ) as serial_number 
from recommerce.ingram_micro_send_order_grading_status )
select
	order_number ,
	serial_number ,
	source_timestamp as return_event
from events
where (disposition_code in ('GOODS_IN', 'RETURNED', 'CLARIFICATION', 'GRADING') and source_timestamp <= '2023-04-19') or 
	(status_code = 'RETURN RECEIVED' and source_timestamp > '2023-04-19') 
qualify row_number () over (partition by order_number order by source_timestamp asc ) = 1;



--
-- sn changes are possible,
-- try to find the exact sn when asset id had
-- when we receive the event 
-- it must be matched with sn in the pre-alert
--
drop table if exists sn_changes;
create temp table sn_changes as 
select 
	assetid as asset_id,
	createddate as sn_updated,
	oldvalue ,
	newvalue ,
	row_number () over (partition by assetid order by createddate asc) rn
from stg_salesforce.asset_history 
where field ='SerialNumber';



drop table if exists asset_sn_updates;
create temp table asset_sn_updates as 
-- 1.assets without any sn changes
select 
	asset_id,
	created_date as sn_updated ,
	serial_number
from ods_production.asset 
where asset_id not in (select asset_id from sn_changes)
union all
-- 2.asset id with multiple sn, what is the first one 
select 
	c.asset_id,
	t.created_date as sn_updated ,
	c.oldvalue as serial_number
from sn_changes c
left join ods_production.asset t
on c.asset_id = t.asset_id
where c.rn = 1
union all
-- 2.asset id with multiple sn, further sn's 
select 
	asset_id,
	sn_updated ,
	newvalue as serial_number
from sn_changes;



drop table if exists ods_operations.ingram_allocation_mapping;
create table ods_operations.ingram_allocation_mapping as 
with get_asset_id as (
	select 
		r.order_number,
		r.serial_number,
		a.asset_id,
		r.return_event 
	from ingram_first_return_event r 
	left join asset_sn_updates a 
	on r.serial_number = a.serial_number
	and r.return_event > a.sn_updated
	where 1=1
	qualify row_number() over (PARTITION BY r.order_number ORDER BY a.sn_updated desc)  = 1
)
select 
	r.order_number ,
	r.serial_number ,
	r.asset_id ,
	a.allocation_id ,
	r.return_event
from get_asset_id r
left join ods_production.allocation a 
on r.asset_id = a.asset_id 
and r.return_event > a.allocated_at 
where 1=1
qualify row_number() over (PARTITION BY r.order_number ORDER BY a.allocated_at desc)  = 1
;
