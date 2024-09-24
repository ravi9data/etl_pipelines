--reported by warehouse partner but missing in salesforce
drop table if exists monitoring.oi1;

create table monitoring.oi1 as
with value as (
	select 
		variant_sku , 
		avg(residual_value_market_price) as residual_value_market_price ,
		avg(initial_price) as initial_price 
	from master.asset 
	group by 1
)
select 
	o.warehouse ,
	o.wh_serial_number ,
	o.wh_sku ,
	o.wh_name ,
	o.wh_status ,
	v.residual_value_market_price,
	v.initial_price
from dm_operations.inventory_asset_overview o 
left join value v on o.wh_sku = v.variant_sku
where asset_id is null;


--will be removed after table is created
CREATE TABLE IF NOT EXISTS monitoring.oi_summary (
	reporting_date date,
	monitoring_code varchar(10),
	use_case varchar(10),
	total_assets int,
	total_residual_price int,
	total_purchase_price int
);


--firstly delete if already exists
delete from monitoring.oi_summary
where reporting_date = current_date
and monitoring_code = 'OI1';

insert into monitoring.oi_summary
select 
	current_date as reporting_date,
	'OI1' as monitoring_code,
	case when warehouse = 'Roermond' then 'UC1'
		 when warehouse = 'Ingram Micro' then 'UC2'
		 end as use_case,
	count(wh_serial_number) as total_assets,
	round(sum(residual_value_market_price), 0) as total_residual_price,
	round(sum(initial_price), 0) as total_purchase_price
from monitoring.oi1 
group by 1, 2, 3;