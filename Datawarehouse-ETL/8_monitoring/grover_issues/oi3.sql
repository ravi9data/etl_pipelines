--asset status both in SF and WH reports not matching 
drop table if exists monitoring.oi3;

create table monitoring.oi3 as
select 
	serial_number ,
	asset_id ,
	asset_name,
	asset_status_original ,
	sf_warehouse ,
	variant_sku ,
	wh_sku ,
	wh_status ,
	warehouse ,
	case 
        --UC1: Roermond received the asset but SF not updated as IN STOCK or INBOUND UNALLOCABLE
        when asset_status_original = 'TRANSFERRED TO WH'
		     and warehouse = 'Roermond'
		then 'UC1'
        --UC2: IM received the assets but SF status not accurate
		when asset_status_original in ('IN DEBT COLLECTION', 'RESERVED', 'LOST', 'WRITTEN OFF DC', 'ON LOAN')
			 and warehouse = 'Ingram Micro'
		then 'UC2'
        --UC3: SF and UPS Roermond status not match for UNALLOCABLE assets
		when (asset_status_original <> 'INBOUND UNALLOCABLE' and wh_status ='UNALLOCABLE')
		 	  or 
		 	 (asset_status_original = 'INBOUND UNALLOCABLE' and wh_status <> 'UNALLOCABLE')
		then 'UC3'
        --UC4: In SF , asset is available but not reported in WH Partners reports
        --     This may cause ReAllocation before shipment
		when asset_status_original = 'IN STOCK' and warehouse is null 
		then 'UC4'
	end as use_case
from dm_operations.inventory_asset_overview 
where use_case is not null;

--firstly delete if already exists
delete from monitoring.oi_summary
where reporting_date = current_date
and monitoring_code = 'OI3';

insert into monitoring.oi_summary
select 
	current_date as reporting_date,
	'OI3' as monitoring_code ,
	o.use_case,
	count(o.serial_number) as total_assets ,
	round(sum(t.residual_value_market_price), 0) as total_residual_price ,
	round(sum(t.initial_price), 0) as total_purchase_price 
from monitoring.oi3 o
left join master.asset t 
on t.asset_id = o.asset_id 
group by 1, 2, 3;


drop table if exists monitoring.product_asset_consistencies;
create table monitoring.product_asset_consistencies as
with total_assets_a as (
	select 
		metric_name , 
		fact_date , 
		metric_value 
	from dm_operations.system_changes_weekly_agg
	where fact_date > current_date - 365
	and metric_name in ('Assets', 'Asset Created in 18M')
	and date_part(dayofweek, fact_date) = 1 --take monday
),
losts_b1 as (
	select 
		'Total Lost Assets' as metric_name ,
		date_trunc('week', lost_date) as fact_date ,
		count(asset_id) as metric_value 
	from dm_operations.lost_assets_and_compensation 
	group by 1, 2
),
lost_and_investigations_b2 as (
	select 
		case when asset_status_original = 'LOST'
			then 'Total Lost Assets (Accum.)'
			else 'Total Assets in Investigation' end as metric_name ,
		date as fact_date ,
		count(asset_id) as metric_value
	from master.asset_historical 
	where date > current_date - 365
		and date_part(dayofweek, date) = 1 --take monday
	and asset_status_original in ('LOST')
	group by 1, 2
),
oi_monitoring_c as (
	select 
		'Total Consistencies ' || monitoring_code as metric_name , 
		reporting_date as fact_date ,
		sum(total_assets) as metric_value 
	from monitoring.oi_summary 
	where reporting_date > current_date - 365
	and date_part(dayofweek, reporting_date) = 1 --take monday
	and not (monitoring_code='OI3' and use_case='UC4')
	group by 1, 2
)
select metric_name , fact_date , metric_value from total_assets_a
union 
select metric_name , fact_date , metric_value from losts_b1
union 
select metric_name , fact_date , metric_value from lost_and_investigations_b2
union 
select metric_name , fact_date , metric_value from oi_monitoring_c;

GRANT SELECT ON monitoring.oi3 TO tableau;
