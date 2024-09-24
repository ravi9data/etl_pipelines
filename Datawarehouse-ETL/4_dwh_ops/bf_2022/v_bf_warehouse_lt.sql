create or replace view dm_operations.v_bf_warehouse_lt AS
with
rd as (
select 
	allocation_id ,
	region,
	warehouse ,
	ob_shipment_unique ,
	infra ,
	carrier ,
	customer_type ,
	--allocated_at ,
	case when warehouse in ('office_us' , 'ups_softeon_us_kylse')
		 then convert_timezone('EST', allocated_at) 
		 else convert_timezone ('CET', allocated_at)
		 end as allocated_at_conv,
	case when warehouse in ('office_us' , 'ups_softeon_us_kylse') 
		 then convert_timezone('EST', shipment_label_created_at) 
		 else convert_timezone ('CET', shipment_label_created_at)
		 end as shipment_label_created_at_conv,
	case when warehouse in ('office_us' , 'ups_softeon_us_kylse') 
		 then convert_timezone('CET', 'EST', shipment_at) --already in CET in infra
		 else
		    case when infra = 'Legacy' 
		 		 then convert_timezone ('CET', shipment_at)
		 		 else shipment_at --already in CET
		 	end
		 end as shipment_at_conv,
		 ---
		 ---
	greatest (
		datediff('hour', allocated_at_conv , shipment_label_created_at_conv ) 
        - --exclude non_working_days
        case when warehouse in ('office_us' , 'ups_softeon_us_kylse') then 0 
        	 else (select count(1) from public.dim_dates	
			        where datum >= date_trunc('day', allocated_at_conv) 
			        	  and datum < shipment_label_created_at_conv
			        	  and 
			        	case when warehouse = 'synerlogis_de' 
			        			  or 
			        			  (warehouse = 'ups_softeon_eu_nlrng' 
			        			   and allocated_at_conv < '2022-11-14')
			        		 then week_day_number in (6, 7)
			        		 else week_day_number in (6) 
			        	 end)
			 end * 24 
		, 0 ) 
		as lt_label_created_at,
		 ---
		 ---
	greatest (
		datediff('hour', allocated_at_conv , shipment_at_conv ) 
        - --exclude non_working_days
        case when warehouse in ('office_us' , 'ups_softeon_us_kylse') then 0 
        	 else (select count(1) from public.dim_dates	
			        where datum >= date_trunc('day', allocated_at_conv) 
			        	  and datum < shipment_at_conv
			        	  and 
			        	case when warehouse = 'synerlogis_de' 
			        			  or 
			        			  (warehouse = 'ups_softeon_eu_nlrng' 
			        			   and allocated_at_conv < '2022-11-14')
			        		 then week_day_number in (6, 7)
			        		 else week_day_number in (6) 
			        	 end)
			 end * 24 
		, 0 ) 
		as lt_shipment_at
from ods_operations.allocation_shipment 
where allocated_at >= '2022-11-07'
  and not (shipment_label_created_at is null and shipment_at is null)
  and store <> 'Partners Offline'
  order by allocated_at 
  )
  select distinct
  	allocation_id,
  	region,
  	warehouse,
  	ob_shipment_unique,
  	infra,
  	carrier,
  	customer_type,
  	allocated_at_conv,
  	shipment_label_created_at_conv,
  	shipment_at_conv,
	lt_label_created_at,
	lt_shipment_at
  from rd
with no schema binding;