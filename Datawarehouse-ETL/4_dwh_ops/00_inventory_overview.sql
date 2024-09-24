drop table if exists dm_operations.inventory_overview;
create table dm_operations.inventory_overview as
with staging_all as (
	--- SALESFORCE
	select 
		case when asset_status_original = 'IN STOCK' 
			 then 'IN STOCK (SF)'
			 when asset_status_original = 'INBOUND UNALLOCABLE' 
			 then 'REPLENISHMENT (SF)' end as kpi_name,
		case when warehouse in ('office_us', 'ups_softeon_us_kylse') then 'US' else 'EU' end as region,
		date_add('day', 1, date)  as reporting_date ,
		warehouse::varchar(60) ,
		coalesce (category_name, 'Not Defined') as category_name  ,
		case when asset_condition_spv = 'NEW' then 'NEW'::varchar(10) else 'AGAN'::varchar(10) end asset_condition,
		count(1) as stock_level
	from master.asset_historical 
	where asset_status_original in ('IN STOCK', 'INBOUND UNALLOCABLE')
	   and warehouse not in ('office_de', 'ingram_micro_eu_flensburg')
	   and date < current_date
	group by 1,2,3,4,5,6
	union all
	--- UPS ROERMOND
	select 
		case when disposition_cd in ('GOOD', 'NEW')
			 then 'IN STOCK (UPS)'
			 when disposition_cd in ('UNALLOCABLE')
			 then 'REPLENISHMENT (UPS)'
			 end as kpi_name,
		'EU' as region,
		report_date as reporting_date ,
		'Roermond'::varchar(60) as warehouse,
		coalesce (category_name, 'Not Defined') as category_name ,
		'Total' as asset_condition ,
		count(1) as stock_level
	from dm_operations.ups_eu_reconciliation
	where disposition_cd in ('GOOD', 'NEW', 'UNALLOCABLE')
	group by 1,2,3,4,5,6
	union all
	--- SYNERLOGIS
	select  
		'IN STOCK (SYN)'::varchar(20) as kpi_name,
		'EU' as region,
		reporting_date ,
		warehouse::varchar(60) ,
		coalesce (category_name, 'Not Defined') as category_name ,
		'Total' as asset_condition ,
		count(1) as stock_level 
	from dwh.wemalo_sf_reconciliation
	where sperrlager_type = 'Stock'
	 and (asset_status_original is null or asset_status_original = 'IN STOCK')
	group by 1,2,3,4,5,6
	union all
	--- US UPS
	select  
		'IN STOCK (US UPS)'::varchar(20) as kpi_name,
		'US' as region,
		report_date as reporting_date,
		'US UPS'::varchar(60) as warehouse ,
		coalesce (category_name, 'Not Defined') as category_name ,
		'Total' as asset_condition ,
		count(1) as stock_level 
	from ods_external.us_inventory
	group by 1,2,3,4,5,6
),
active_reservations_pre as (
	select 
		case when ir.store_id in (14, 621, 629) then 'US' else 'EU' end as region,
	  	d.datum as reporting_date,
	  	p.category_name,
	  	sum(initial_quantity) as reserved_count 
	from public.dim_dates d 
	left join ods_production.inventory_reservation ir 
	   on (date_trunc('day', created_at::timestamp) < datum	and --1.what was the picture in first minute of the day?
	   															--2.checking paid event is non-sense, even it is paid or not, 
																----we block available stock
	      (deleted_at::timestamp is null 	or date_trunc('day', deleted_at::timestamp)   >= datum) and 
	      (declined_at::timestamp is null 	or date_trunc('day', declined_at::timestamp)  >= datum) and 
	      (expired_at::timestamp is null 	or date_trunc('day', expired_at::timestamp)   >= datum) and 
	      (fulfilled_at::timestamp is null or date_trunc('day', fulfilled_at::timestamp) >= datum) and 
	      (cancelled_at::timestamp is null or date_trunc('day', cancelled_at::timestamp) >= datum))
	left join ods_production.variant v 
	    on ir.sku_variant_code = v.variant_sku
	left join ods_production.product p 
	    on v.product_id = p.product_id
	where datum > dateadd('day', -360, current_date) and datum <= current_date
	group by 1,2,3
),
in_stock_pre as (
	select 
		region,
		reporting_date ,
		category_name ,
		sum(stock_level) as stock_level 
	from staging_all 
	where kpi_name = 'IN STOCK (SF)'
	group by 1,2,3
)
select 
	kpi_name ,
	region,
	reporting_date ,
	warehouse ,
	category_name ,
	asset_condition ,
	stock_level 
from staging_all 
union all
select 
	'STOCK ON HAND' as kpi_name,
	st.region,
	st.reporting_date,
	'Total' as warehouse ,
	st.category_name,
	'Total' as asset_condition ,
	st.stock_level - coalesce(ar.reserved_count, 0) as stock_level 
from in_stock_pre st 
left join active_reservations_pre ar 
   on 	ar.reporting_date = st.reporting_date 
   		and ar.region = st.region
   		and ar.category_name = st.category_name
;

GRANT SELECT ON dm_operations.inventory_overview TO tableau;
