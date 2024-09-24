drop table if exists dm_operations.supply_chain_daily_b2b ;
create table dm_operations.supply_chain_daily_b2b as
with 
------------
------------ COMMON QUERIES
------------
outbound_historical as (
	select 
		a."date",
		oa.order_id,
  		oa.warehouse,
		a.allocation_status_original,
		a.shipment_label_created_at::timestamp,
		a.ready_to_ship_at::timestamp,
		a.push_to_wh_at::timestamp,
		oa.region
	from master.allocation_historical a 
    left join ods_operations.allocation_shipment oa
    on oa.allocation_id = a.allocation_id 
	where a."date" >= dateadd ('day', -365, current_date)
  	and a.asset_id is not null
  	and a.store_short not in ('Partners Online', 'Partners Offline')
	and oa.warehouse in ('synerlogis_de', 'ups_softeon_eu_nlrng', 'office_us', 'ups_softeon_us_kylse')
	and oa.customer_type = 'business_customer'
	),
outbound_pre as (
	select 
		ob_shipment_unique as shipment_unique,
		s.carrier,
		s.shipment_service,
	    s.shipping_country,
	    s.region,
		s.customer_type,
		s.warehouse,
		s.warehouse_detail,
		s.order_id,
		s.asset_id,
		s.allocation_status_original,
		min(s.allocated_at) as allocated_at,
		min(s.shipment_label_created_at) as shipment_label_created_at ,
		min(s.shipment_at) as shipment_at,
		min(s.delivered_at) as delivered_at,
		min(s.ready_to_ship_at) as ready_to_ship_at
	from ods_operations.allocation_shipment as s
	where s.outbound_bucket not in ('Partners Offline', 'Cancelled Before Shipment') 
	  and s.warehouse in ('synerlogis_de', 'ups_softeon_eu_nlrng', 'office_us', 'ups_softeon_us_kylse')
	  and s.customer_type = 'business_customer'
	group by 1, 2, 3, 4, 5, 6, 7, 8,9, 10, 11
),
outbound as (
	select 
		order_id,
  		warehouse,
  		asset_id,
  		warehouse_detail,
  		allocation_status_original,
  		ready_to_ship_at,
  		ready_to_ship_at as push_to_wh_at,
		shipment_label_created_at,
		shipment_at,
		delivered_at,
		region
    from outbound_pre
	where  asset_id is not null
	),
------------
------------ DELIVERY MANAGEMENT
------------
	select 
		'delivery_mgmt' as section_name,
		region,
  		'PENDING ALLO. '  ||
  		case when store_short in ('Partners Online', 'Partners Offline')
		 	 then '(PARTNERS)'
  			 else '(GROVER)' end as kpi_name,
		datum as reporting_date, 
		sum(pending_allocations) as kpi_value 
	from ods_production.inventory_reservation_pending_historical
	where (datum < current_date and hours = 13) --let's take middle of the day, 
					 						    --orders should be reviewed and allocated until 13
	   or (datum = current_date and hours = (select least(max(hours), 13)
	   										 from ods_production.inventory_reservation_pending_historical
											 where datum = current_date))
	   and customer_type ='business_customer'
	group by 1, 2, 3, 4
	) , 
------------
------------ OUTBOUND
------------
outbound_ods as (
	select 
		s.order_completed_at::timestamp, 
		s.delivered_at::timestamp,
		s.shipment_label_created_at::timestamp, 
		s.shipment_at::timestamp,
		s.region,
		s.carrier_first_mile,
		s.is_delivered_on_time
	from ods_operations.allocation_shipment s
	where s.allocated_at > current_date - 180
	and s.customer_type ='business_customer'
	and s.store <> 'Partners Offline'
	--left join ods_production.allocation a on a.allocation_id = s.allocation_id 
	),
ready_to_ship_orders_sf as (
	select 
		'outbound' as section_name,
		region,
		'READY TO SHIP' ||
  			case when warehouse = 'ups_softeon_eu_nlrng' then ' (UPS)'
  				 when warehouse in ('office_us', 'ups_softeon_us_kylse') then ''  
			end as kpi_name,
		date as reporting_date, 
		count(distinct order_id) as kpi_value 
	from outbound_historical 
	where "date" = date_trunc('day', convert_timezone('Europe/Berlin', push_to_wh_at::timestamp))
	  and warehouse in ('ups_softeon_eu_nlrng', 'office_us', 'ups_softeon_us_kylse')
	group by 1, 2, 3, 4
	),
shipped_assets_sf as (
	select 
		'outbound' as section_name,
		region,
		'SHIPPED ASSETS' ||
  			case when warehouse = 'ups_softeon_eu_nlrng' then ' (UPS)'
  				 when warehouse in ('office_us', 'ups_softeon_us_kylse') then ''  
  			end as kpi_name,
  		warehouse_detail,
		date_trunc('day', coalesce (shipment_at,  delivered_at)) as reporting_date,
		count(asset_id) as kpi_value
	from outbound
	where warehouse in ('ups_softeon_eu_nlrng', 'office_us', 'ups_softeon_us_kylse')
	group by 1, 2, 3, 4, 5
	),
aged_more_2_days as (
	select
		'outbound' as section_name,
		region,
		'AGING ORDERS >2 DAYS' ||
  			case when warehouse = 'ups_softeon_eu_nlrng' then ' (UPS)'
  				 when warehouse in ('office_us', 'ups_softeon_us_kylse') then '' 
  			end as kpi_name,
		date as reporting_date,
		count(distinct order_id) as kpi_value
	from outbound_historical 
	where allocation_status_original = 'READY TO SHIP' and shipment_label_created_at::timestamp is null --and shipment_tracking_number is null 
	and datediff(day, convert_timezone('Europe/Berlin', ready_to_ship_at::timestamp), date) > 1 --dismiss today (0), yesterday (1) and the day before (2)
	and warehouse in ('ups_softeon_eu_nlrng', 'office_us', 'ups_softeon_us_kylse')
	group by 1, 2, 3, 4
	) , 
delivered_on_time as (
	select 
		'outbound' as section_name,
		region,
		'DELIVERED ON TIME' as kpi_name,
		date_trunc('day', delivered_at::timestamp) as reporting_date,
		--exclude sundays, expected delivery time is 5
		round(
			count(case when is_delivered_on_time then 1 end)::float 
			/ count(1)::float * 100 , 1 ) as kpi_value
	from outbound_ods 
	where delivered_at is not null
	group by 1, 2, 3, 4
	) ,
picking_time_by_carrier as (
	select 
		'outbound' as section_name,
		region,
		'PICKING TIME BY CARRIER' as kpi_name,
		date_trunc('day', convert_timezone('Europe/Berlin', shipment_at::timestamp)) as reporting_date,
		round( avg(carrier_first_mile), 2 ) kpi_value
	from outbound_ods 
	where carrier_first_mile is not null
	group by 1, 2, 3, 4
	),
packed_within_24_hours as (	
	select 
		'outbound' as section_name,
		region,
		'PICKED WITHIN 24 HOURS' as kpi_name,
		date_trunc('day', convert_timezone('Europe/Berlin', shipment_at::timestamp)) as reporting_date,
		case when count(convert_timezone('Europe/Berlin', shipment_at::timestamp)) = 0 then 0 --avoid divide by zero
			 else round( count(case 	when carrier_first_mile <= 24 then 1 end)::float / 
			 			 count(convert_timezone('Europe/Berlin', shipment_at::timestamp))::float * 100 
						, 1) end as kpi_value
    from outbound_ods
    where carrier_first_mile is not null
    group by 1, 2, 3, 4
	),
------------
------------ TRANSPORTATION
------------
transportation_initial as (
	select 
  	    a.warehouse,
		a.region,
		a.shipping_country as country,
		a.shipment_at,
		a.first_delivery_attempt,
		a.delivered_at,
		a.warehouse_lt,
		a.transit_time,
		a.delivery_time
	from ods_operations.allocation_shipment a 
	where a.warehouse in ('synerlogis_de', 'ups_softeon_eu_nlrng', 'office_us', 'ups_softeon_us_kylse')
	 and a.allocated_at > current_date - 180
	 and a.customer_type ='business_customer'
	 and a.store <> 'Partners Offline'
	) ,
warehouse_lt as ( 
	select
		'outbound' as section_name, -- was in TRANSPORTATION before, do not move upwards since this uses transportation_initial
		region,
		'WAREHOUSE LT'  ||
  			case when warehouse = 'ups_softeon_eu_nlrng' then ' (UPS)'
  				 when warehouse in ('office_us', 'ups_softeon_us_kylse') then '' 
				 end
  			as kpi_name, -- Days to Process WH @ Tableau
		date_trunc('day', shipment_at) as reporting_date,
		round(avg(warehouse_lt::float), 2) as kpi_value
	from transportation_initial
	where warehouse in ('ups_softeon_eu_nlrng', 'office_us', 'ups_softeon_us_kylse')
	group by 1, 2, 3, 4	
	) ,
avg_transit_time as (
	select
		'transportation' as section_name,
		region, 
		'AVG.TRANSIT TIME' ||
  		case when region = 'EU' then
  			case when country = 'Germany' then ' (DE)'
  				 when country = 'Austria' then ' (AT)'
  				 when country = 'Netherlands' then ' (NL)'
  				 when country = 'Spain' then ' (ES)'
  				 else ' (OTH)' end
  			else '' end --for united states
  		 as kpi_name, -- Median Days to Process Shipping @ Tableau
		date_trunc('day', delivered_at) as reporting_date,
		avg(transit_time) as kpi_value
	from transportation_initial
	group by 1, 2, 3, 4
	),
med_net_ops_cycle as (
	select
		'transportation' as section_name,
		region,
		'MED.NET OPS CYCLE' ||
  			case when region = 'EU' then
              case when country = 'Germany' then ' (DE)'
                   when country = 'Austria' then ' (AT)'
                   when country = 'Netherlands' then ' (NL)'
                   when country = 'Spain' then ' (ES)'
                   else ' (OTH)' end
              else '' end --for united states 
  		as kpi_name, -- Median Delivery Time @ Tableau
		date_trunc('day', first_delivery_attempt) as reporting_date,
		median(delivery_time) as kpi_value
	from transportation_initial
	group by 1, 2, 3, 4
	),
avg_net_ops_cycle as (
	select
		'transportation' as section_name,
		region,
		'AVG.NET OPS CYCLE' as kpi_name, -- Avg Delivery Time @ Tableau
		date_trunc('day', first_delivery_attempt) as reporting_date,
		round(avg(delivery_time::float), 2) as kpi_value
	from transportation_initial
	group by 1, 2, 3, 4
	),
unioned as (
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 1 as is_increase_good from ready_to_ship_orders_sf
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 1 as is_increase_good from shipped_assets_sf
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 0 as is_increase_good from aged_more_2_days
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 1 as is_increase_good from delivered_on_time
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 0 as is_increase_good from picking_time_by_carrier
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 1 as is_increase_good from packed_within_24_hours
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 0 as is_increase_good from warehouse_lt
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 0 as is_increase_good from avg_transit_time
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 0 as is_increase_good from med_net_ops_cycle
union all
select section_name, region, kpi_name, reporting_date, kpi_value, 1 as show_yesterday, 0 as is_increase_good from avg_net_ops_cycle  
)
-----------------------------
------END OF CTE-------------
-----------------------------
select 
	section_name, 
    region, 
    kpi_name, 
    reporting_date, 
    kpi_value ,
    case when dense_rank() over (order by reporting_date desc) = 1 + show_yesterday then 'Main'
    	 when dense_rank() over (order by reporting_date desc) = 2 + show_yesterday then 'Previous'
    	 else null end as scorecard_indicator,
    is_increase_good
from unioned
where reporting_date not in (select holiday_date from dm_operations.holidays)
and date_part(dow, reporting_date) not in (0, 6)
and reporting_date <= current_date;

GRANT SELECT ON dm_operations.supply_chain_daily_b2b TO tableau;
