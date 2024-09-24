--reported by warehouse partner but warehouse code in SF is not correct
drop table if exists monitoring.oi2;

create table monitoring.oi2 as
select 
	serial_number ,
	asset_id ,
	asset_name,
	asset_status_original ,
	sf_warehouse ,
	variant_sku ,
	wh_sku ,
	wh_status ,
	warehouse 
from dm_operations.inventory_asset_overview 
where (warehouse = 'Roermond' and sf_warehouse <> 'ups_softeon_eu_nlrng')
	   or 
	  (warehouse in ('Kiel', 'Hagenow', 'Lüneburg') and sf_warehouse <> 'synerlogis_de')
	   or 
	  (warehouse = 'Ingram Micro' and sf_warehouse <> 'ingram_micro_eu_flensburg');


--firstly delete if already exists
delete from monitoring.oi_summary
where reporting_date = current_date
and monitoring_code = 'OI2';

insert into monitoring.oi_summary
select 
	current_date as reporting_date,
	'OI2' as monitoring_code ,
	case when o.warehouse = 'Ingram Micro' and o.sf_warehouse = 'synerlogis_de' then 'UC1'
		 when o.warehouse = 'Ingram Micro' and o.sf_warehouse = 'ups_softeon_eu_nlrng' then 'UC2'
		 when o.warehouse = 'Roermond' and o.sf_warehouse = 'synerlogis_de' then 'UC3'
		 when o.warehouse = 'Roermond' and o.sf_warehouse = 'ingram_micro_eu_flensburg' then 'UC4'
		 end use_case,
	count(o.serial_number) as total_assets ,
	round(sum(t.residual_value_market_price), 0) as total_residual_price ,
	round(sum(t.initial_price), 0) as total_purchase_price 
from monitoring.oi2 o
left join master.asset t 
on t.asset_id = o.asset_id 
group by 1, 2, 3;
