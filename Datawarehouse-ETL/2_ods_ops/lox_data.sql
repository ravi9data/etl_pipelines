drop table if exists product_requests.lox_data;
create table product_requests.lox_data as
select 
	shipment_tracking_number as tracking_number,
	purchased_date as sales_date,
	a2.brand,
	a2.subcategory_name,
	a2.asset_name as description,
	a2.serial_number,
	count(a2.asset_id) over (partition by shipment_tracking_number) as quantity,
	initial_price as total_price_excl_vat,
	case when a.warehouse = 'ups_softeon_eu_nlrng' then 0.21 * initial_price
	     when a.warehouse in ('ingram_micro_eu_flensburg', 'synerlogis_de') then 0.19 * initial_price
	     else 0 end as total_vat_amount
from ods_production.allocation a
left join ods_production.asset a2 on a.asset_id = a2.asset_id 
where tracking_number is not null 
	  and shipment_at > current_date - 90
union all
select 
	return_shipment_tracking_number as tracking_number,
	purchased_date as sales_date,
	a2.brand,
	a2.subcategory_name,
	a2.asset_name as description,
	a2.serial_number,
	count(a2.asset_id) over (partition by return_shipment_tracking_number) as quantity,
	initial_price as total_price_excl_vat,
	case when a.warehouse = 'ups_softeon_eu_nlrng' then 0.21 * initial_price
		 when a.warehouse in ('ingram_micro_eu_flensburg', 'synerlogis_de') then 0.19 * initial_price
		 else 0 end as total_vat_amount
from ods_production.allocation a
left join ods_production.asset a2 on a.asset_id = a2.asset_id 
where return_shipment_tracking_number  is not null 
	  and return_shipment_at > current_date - 90;

GRANT SELECT ON product_requests.lox_data TO GROUP recommerce;