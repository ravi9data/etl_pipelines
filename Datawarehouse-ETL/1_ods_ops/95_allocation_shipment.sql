drop table if exists tmp_weekly_back_in_stock;
create temp table tmp_weekly_back_in_stock as
with allo_pre as (
select 
	asset_id ,
	serial_number ,
	date_trunc('day', 
	greatest(a.return_delivery_date_old ,
			     a.return_delivery_date,
			     a.failed_delivery_at,
			     a.refurbishment_start_at)) as return_fact_date_greatest
from ods_production.allocation a
where return_fact_date_greatest is not null
)
select d.asset_id , d.reporting_date , case when count(1) > 0 then 'Kiel' end return_wh
from dwh.wemalo_sf_reconciliation d , allo_pre a 
where a.asset_id = d.asset_id 
and d.reporting_date >= a.return_fact_date_greatest 
and d.reporting_date <= dateadd('day', 7, a.return_fact_date_greatest)
group by 1, 2
union  
select a.asset_id, im.reporting_date , case when count(1) > 0 then 'Ingram Micro' end return_wh
from ods_operations.ingram_inventory im , allo_pre a 
where a.serial_number = im.serial_number  
and im.reporting_date >= a.return_fact_date_greatest
and im.reporting_date <= date_add('day', 7, a.return_fact_date_greatest)
and im.is_in_warehouse = 1
group by 1, 2;


--to be loaded incremental later
drop table if exists ods_operations.order_shipping_data;
create table ods_operations.order_shipping_data as
select 
	order_id,
	case when o.shippingcountry = 'Soft Deleted Record' 
			 or o.shippingcountry is null 
	  	 then o.store_country  
	  	 else 
           case when o.shippingcountry = 'Deutschland' then 'Germany'
     	        when o.shippingcountry = 'The Netherlands' or
                     o.shippingcountry = 'Netherlands.'  then 'Netherlands'
     	        when o.shippingcountry = 'Österreich' then 'Austria'
     	        when o.shippingcountry = 'UK' then 'United Kingdom'
     	        when o.shippingcountry = 'Czech Republic' then 'Czechia'
     	        when o.shippingcountry = 'ITALY' then 'Italy'
     	        when o.shippingcountry = 'SPAIN' then 'Spain'
     	        when o.shippingcountry = 'SLOVAKIA' then 'Slovakia'
				when o.shippingcountry = 'United States of America' then 'United States'
                else o.shippingcountry 
             end
    end as shipping_country ,
	shippingcity,
	coalesce (shippingpostalcode ,	billingpostalcode ) shippingzipcode
from ods_production."order" o
where o.approved_date is not null;


drop table if exists allocation_pre;
create temp table allocation_pre
as 
select 
          a.allocation_id,
		  a.allocation_status_original,
          so.shipment_uid,
          case when s.store_short != 'Partners Offline' then 'Grover' else s.store_short end store,
      	  nullif(a.shipment_tracking_number, '') as tracking_number,
      	  nullif(a.shipment_id, '') as tracking_id,
          case when a.allocation_sf_id is null 
               then 'New Infra' 
               else 'Legacy' end as infra,
          coalesce(upper(a.shipping_provider), 'Missing') as shipping_provider,
          coalesce(a.shipping_profile, 'Missing') as shipping_profile,
          coalesce(a.shipcloud_profile, 'Missing') as shipcloud_profile,
          osd.shipping_country, 
          case when coalesce(so.receiver_country, osd.shipping_country) is null 
          	   then 
          		  case when a.warehouse in ('office_us', 'ups_softeon_us_kylse')
                       then 'US' else 'EU' end 
         	   when coalesce(so.receiver_country, osd.shipping_country) in ('United States','United States of America')
	  		   then 'US'
	  		   else 'EU' end as region,
          c.customer_type ,
		  a.subscription_id,
		  a.order_id,
          a.asset_id ,
		  t.variant_sku,
		  t.asset_name,
          a.serial_number,
		  so.sender_city,
		  so.sender_country,
		  coalesce(so.receiver_zipcode, osd.shippingzipcode) as receiver_zipcode,
		  coalesce(so.receiver_city, osd.shippingcity) as receiver_city,
		  coalesce(so.receiver_country, osd.shipping_country) as receiver_country,
          a.warehouse ,
          --find the latest record of asset before shipment
          (select r.warehouse 
             from dwh.wemalo_sf_reconciliation r
            where r.asset_id = a.asset_id
                  and date_trunc('day', a.shipment_at) >= r.reporting_date  
           order by r.reporting_date desc 
           limit 1) warehouse_detail_pre,
          case when a.warehouse = 'office_us' then 'UPS Louisville Old'
		  	   when a.warehouse = 'ups_softeon_us_kylse' then 'UPS Louisville'
               when a.warehouse = 'ups_softeon_eu_nlrng' then 'UPS Roermond'
			   when a.warehouse = 'ingram_micro_eu_flensburg' then 'Ingram Micro'
               when a.warehouse = 'synerlogis_de' then
                  case when warehouse_detail_pre is null then 'Synerlogis'
                       else warehouse_detail_pre end
               else a.warehouse end as warehouse_detail ,
          a.allocated_at::timestamp ,
		  a.order_completed_at::timestamp,
          a.ready_to_ship_at::timestamp, --push_to_wh_at
          a.shipment_label_created_at::timestamp,
          a.shipment_at::timestamp ,
          a.delivered_at::timestamp ,
  		  a.failed_delivery_at::timestamp,
  		  a.failed_delivery_candidate::timestamp,
		  si.shipment_uid as return_shipment_uid,
  		  coalesce(si.tracking_number, a.return_shipment_tracking_number) as return_tracking_number,
  		  coalesce(si.tracking_id, a.return_shipment_id) as return_tracking_id,
  		  coalesce(si.carrier, a.return_shipment_provider) as return_carrier,
  		  si.sender_country as return_sender_country,
  		  si.sender_city as return_sender_city,
  		  si.receiver_country as return_receiver_country,
  		  si.receiver_city as return_receiver_city,
  		  coalesce(si.created_date, a.return_shipment_label_created_at)::timestamp as return_shipment_label_created_at,
  		  coalesce(si.shipped_date, a.return_shipment_at)::timestamp as return_shipment_at,
  		  a.return_delivery_date_old::timestamp,
  		  coalesce(si.delivered_date, a.return_delivery_date)::timestamp as return_delivery_date,
  		  si.failed_delivery_at::timestamp as return_failed_delivery_at,
  		  a.refurbishment_start_at::timestamp,
		  a.is_last_allocation_per_asset
      from ods_production.allocation a 
      left join ods_production.subscription s 
          on a.subscription_id = s.subscription_id 
	  left join ods_production.asset t 
	  	  on a.asset_id = t.asset_id
      left join ods_production.customer c
          on a.customer_id  = c.customer_id 
      left join staging.shipment_outbound so 
      	  on a.allocation_id = coalesce (so.salesforce_allocation_id, so.allocation_uid)
      left join staging.shipment_inbound si
      	  on a.allocation_id = coalesce (si.salesforce_allocation_id, si.allocation_uid)
	  left join ods_operations.order_shipping_data osd 
	  	  on a.order_id = osd.order_id
      where --s.store_short != 'Partners Offline' and
      	  (a.ready_to_ship_at is not null or 
      	   a.shipment_label_created_at is not null or
           a.shipment_at is not null or 
           a.delivered_at is not null) ;



drop table if exists tmp_allocation_shipment;
create temp table tmp_allocation_shipment 
--sortkey(allocation_id)
--distkey(allocation_id)
as
with 
delivery_attempts as (
	select 	
		tracking_id, 
		shipment_uid,
		max(number_of_attempts) as number_of_attempts ,
		min(event_timestamp) as event_timestamp 
	from ods_operations.delivery_attempts 
	group by 1, 2
),
shipment_mapping as (
	select 
		a.allocation_id ,
		a.allocation_status_original,
		a.shipment_uid,
		a.tracking_number,
		a.tracking_id,
        coalesce(
        coalesce(a.shipment_uid,
        coalesce(a.tracking_id) ,
                 a.tracking_number) ,
                 a.allocation_id ) as ob_shipment_unique,
		a.store,
		a.infra,
		sm.carrier_mapping as carrier ,
		sm.shipment_service_mapping as shipment_service,
		a.shipping_country,
		a.region,
		a.subscription_id,
		a.order_id,
		a.customer_type,
		a.asset_id,
		a.variant_sku,
		a.asset_name,
        a.serial_number,
		a.sender_city,
		a.sender_country,
		a.receiver_zipcode,
		a.receiver_city,
		a.receiver_country,
		uzc."state name" as receiver_state_name,
		a.warehouse,
		a.warehouse_detail,
		a.order_completed_at,
		a.allocated_at,
		a.ready_to_ship_at ,
		a.shipment_label_created_at,
		a.shipment_at,
		a.delivered_at ,
  		coalesce (da.event_timestamp, a.delivered_at) as first_delivery_attempt,
  		da.number_of_attempts,
  		coalesce (fd.failed_timestamp, a.failed_delivery_at) as failed_delivery_at,
  		coalesce (a.failed_delivery_candidate, fd.failed_timestamp) as failed_delivery_candidate,
		a.return_shipment_uid,
  		a.return_tracking_number,
		a.return_tracking_id,
        coalesce (
	    coalesce (a.return_shipment_uid ,
	    coalesce (a.return_tracking_id ,
			      a.return_tracking_number ),
			      a.allocation_id )) as ib_shipment_unique,
  		a.return_carrier ,
		a.return_sender_city,
		a.return_sender_country,
		a.return_receiver_city,
		a.return_receiver_country,
		least(a.return_shipment_label_created_at,
			  a.return_shipment_at,
			  a.return_delivery_date_old ,
			  a.return_delivery_date,
			  a.return_failed_delivery_at,
			  a.refurbishment_start_at) as return_fact_date_least,
		greatest(a.return_delivery_date_old ,
			     a.return_delivery_date,
			     a.return_failed_delivery_at,
			     a.refurbishment_start_at) as return_fact_date_greatest,
		case when a.return_receiver_city = 'Louisville' or a.region = 'US' then 'UPS Louisville'
			 when a.return_receiver_city = 'Flensburg' then 'Ingram Micro'
			 when a.return_receiver_city = 'Altenholz' 
			 	  or
			 	  return_fact_date_least < '2022-09-01' then 'Kiel'
			 else bis.return_wh
			end as return_warehouse,
  		a.return_shipment_label_created_at,
  		a.return_shipment_at,
  		a.return_delivery_date_old ,
  		a.return_delivery_date,
		a.return_failed_delivery_at ,
  		a.refurbishment_start_at,
		fd.failed_reason ,
		fd.event_bucket as failed_event_bucket,
		fd.failed_location, 
		fd.failed_details, 
		fd.failed_timestamp,
		a.is_last_allocation_per_asset,
  		case 
	  		when a.store = 'Partners Offline'
	  		then a.store 
	  		when a.failed_delivery_at is not null 
  		  		 or 
  		  		 (
				  a.failed_delivery_candidate is not null 
				  and a.delivered_at is null 
  		  	   	  and coalesce (a.return_delivery_date_old ,
  		  	   	   	  coalesce (a.return_delivery_date, 
  		  	   	   	  			a.refurbishment_start_at) ) is not null
  		  	   	 )
  		  	   	 or 
  		  	   	 (
  		  	   	  a.failed_delivery_candidate is null 
  		  	   	  and a.delivered_at is null 
  		  	   	  and a.shipment_at is not null
  		  	   	  and coalesce (a.refurbishment_start_at,
  		  	   	   	  			a.return_delivery_date_old)  is not null
  		  	   	 )
  		  	then 'Failed Delivery'
  		  	----------------------------------------------------------------
  		  	--having a failed reason, but successfully delivered to customer 
  		  	when a.failed_delivery_candidate is not null 
  		  	   	 and a.delivered_at is not null 
  		  	then 'Delivered - FD Attempt'
  		  	----------------------------
  		  	--shipment still in progress
  		  	when a.delivered_at is null 
  		   		 and coalesce(a.return_delivery_date_old ,
  		  	   	   	 coalesce(a.return_delivery_date, 
  		  	   	   	 		  a.refurbishment_start_at) ) is null
  		  	then 
  		  	   	case when a.failed_delivery_candidate is not null 
  		  	   		 then 'Failed Delivery Candidate'
  		  	   		 when greatest (a.shipment_at , a.ready_to_ship_at , a.shipment_label_created_at, a.allocated_at )		
  		  	   		 		< current_date - 28 
  		  	   			  or fd.last_timestamp::timestamp 	< current_date - 14
  		  	   		 then 'Unclear Shipments'
  		  	   		 when a.shipment_at is null 
  		  	   		 then 'Ready to Ship'
  		  	   		 else 'In Transit'
  		  	    end 
  		  	----------------------------------------------------------
  		  	--to be fixed cases, pushed to wh but not possible to pack
  		  	when a.delivered_at is null 
  		  		 and a.shipment_at is null 
  		  		 and a.refurbishment_start_at is not null 
  		  	then 'Cancelled Before Shipment'
  		  	------------------------
  		  	--successfully delivered
  		  	when a.delivered_at is not null
  		  	then 'Delivered'
  		  	else 'Unclear Shipments'
  		end outbound_bucket  		  	   
	from allocation_pre a 
  	left join public.shipment_mapping sm 
  	on (a.region = sm.region and 
  		a.shipping_provider = sm.carrier and 
  		a.shipping_profile = sm.shipping_profile and
  		a.shipcloud_profile = sm.shipcloud_profile)
	left join tmp_weekly_back_in_stock bis
	on bis.asset_id = a.asset_id and greatest(	a.return_delivery_date_old ,
												a.return_delivery_date,
												a.return_failed_delivery_at,
												a.refurbishment_start_at) = bis.reporting_date
  	left join ods_operations.failed_deliveries fd
  	on coalesce(a.shipment_uid, a.tracking_id) = coalesce(fd.shipment_uid, fd.tracking_id)
    left join delivery_attempts da 
  	on coalesce(a.shipment_uid, a.tracking_id) = coalesce(da.shipment_uid, da.tracking_id)
	left join staging.us_zip_codes uzc 
	on a.receiver_zipcode = uzc.zip and a.receiver_country in('United States','United States of America')
	)
select 
   	allocation_id,
	allocation_status_original,
    shipment_uid,
    tracking_number,
    tracking_id,
    ob_shipment_unique,
    store,
    infra,
    carrier,
    shipment_service,
    shipping_country,
    region,
	subscription_id,
	order_id,
    customer_type,
    asset_id,
	variant_sku,
	asset_name,
	serial_number,
	sender_city,
	sender_country,
	receiver_zipcode,
	receiver_city,
	receiver_country,
	receiver_state_name,
    warehouse,
    warehouse_detail,
    allocated_at,
	order_completed_at,
    ready_to_ship_at,
    shipment_label_created_at,
    shipment_at,
    delivered_at,
    first_delivery_attempt,
    number_of_attempts,
    case when outbound_bucket = 'Failed Delivery'
     	 then coalesce(failed_delivery_at, shipment_at)
         end as failed_delivery_at,
    failed_delivery_candidate,
	return_shipment_uid,
	return_tracking_number,
	return_tracking_id,
    ib_shipment_unique,
    return_carrier,
	return_sender_city,
	return_sender_country,
	return_receiver_city,
	return_receiver_country,
	return_warehouse,
    return_shipment_label_created_at,
    return_shipment_at,
    return_delivery_date_old,
    return_delivery_date,
	return_failed_delivery_at,
    refurbishment_start_at,
    case when outbound_bucket in ('Failed Delivery', 
                                  'Delivered - FD Attempt', 
                                  'Failed Delivery Candidate')
              and failed_reason is null
         then 'Shipment Events Not Available'
         else failed_reason end as failed_reason,
	failed_event_bucket,
    failed_location,
    failed_details,
	failed_timestamp,
    greatest (datediff('day', sm.order_completed_at, sm.first_delivery_attempt ) 
        		- (select count(1) from public.dim_dates d
					where d.datum >= sm.order_completed_at and d.datum < sm.first_delivery_attempt
					  and d.week_day_number in (6, 7))
			, 0) as delivery_time,
    greatest (datediff('day', sm.allocated_at, sm.first_delivery_attempt ) 
        		- (select count(1) from public.dim_dates d
					where d.datum >= sm.allocated_at and d.datum < sm.first_delivery_attempt
					  and d.week_day_number in (6, 7))
			, 0) as net_ops_cycle,
    greatest (datediff('hour', sm.ready_to_ship_at, sm.shipment_at ) --push to wh and ready to ship are same
        		- (select count(1) from public.dim_dates d
					where d.datum >= sm.ready_to_ship_at and d.datum < sm.shipment_at
					  and d.week_day_number in (6, 7)) * 24
			, 0) as warehouse_lt,
    greatest (datediff('hour', sm.shipment_label_created_at, sm.shipment_at ) 
        		- (select count(1) from public.dim_dates d
					where d.datum >= sm.shipment_label_created_at and d.datum < sm.shipment_at
					  and d.week_day_number in (6, 7)) * 24
			, 0) as carrier_first_mile,
    greatest (datediff('hour', sm.shipment_at, sm.first_delivery_attempt ) 
        		- (select count(1) from public.dim_dates d
					where d.datum >= sm.shipment_at and d.datum < sm.first_delivery_attempt
					  and d.week_day_number in (6, 7)) * 24
			, 0) as transit_time,
	case when datediff('day', sm.order_completed_at, sm.delivered_at ) 
        		- (select count(1) from public.dim_dates d
					where d.datum >= sm.order_completed_at and d.datum < sm.delivered_at
					  and d.week_day_number in (6, 7)) <= 5
		then true else false end as is_delivered_on_time,
	is_last_allocation_per_asset,
    outbound_bucket
from shipment_mapping sm;

truncate table ods_operations.allocation_shipment;

insert into ods_operations.allocation_shipment(allocated_at,
allocation_id,
allocation_status_original,
asset_id,
asset_name,
carrier,
carrier_first_mile,
customer_type,
delivered_at,
delivery_time,
failed_delivery_at,
failed_delivery_candidate,
failed_details,
failed_event_bucket,
failed_location,
failed_reason,
failed_timestamp,
first_delivery_attempt,
ib_shipment_unique,
infra,
is_delivered_on_time,
is_last_allocation_per_asset,
net_ops_cycle,
number_of_attempts,
ob_shipment_unique,
order_completed_at,
order_id,
outbound_bucket,
ready_to_ship_at,
receiver_city,
receiver_country,
receiver_state_name,
receiver_zipcode,
refurbishment_start_at,
region,
return_carrier,
return_delivery_date,
return_delivery_date_old,
return_failed_delivery_at,
return_receiver_city,
return_receiver_country,
return_sender_city,
return_sender_country,
return_shipment_at,
return_shipment_label_created_at,
return_shipment_uid,
return_tracking_id,
return_tracking_number,
return_warehouse,
sender_city,
sender_country,
serial_number,
shipment_at,
shipment_label_created_at,
shipment_service,
shipment_uid,
shipping_country,
store,
subscription_id,
tracking_id,
tracking_number,
transit_time,
variant_sku,
warehouse,
warehouse_detail,
warehouse_lt)
select allocated_at,
allocation_id,
allocation_status_original,
asset_id,
asset_name,
carrier,
carrier_first_mile,
customer_type,
delivered_at,
delivery_time,
failed_delivery_at,
failed_delivery_candidate,
failed_details,
failed_event_bucket,
failed_location,
failed_reason,
failed_timestamp,
first_delivery_attempt,
ib_shipment_unique,
infra,
is_delivered_on_time,
is_last_allocation_per_asset,
net_ops_cycle,
number_of_attempts,
ob_shipment_unique,
order_completed_at,
order_id,
outbound_bucket,
ready_to_ship_at,
receiver_city,
receiver_country,
receiver_state_name,
receiver_zipcode,
refurbishment_start_at,
region,
return_carrier,
return_delivery_date,
return_delivery_date_old,
return_failed_delivery_at,
return_receiver_city,
return_receiver_country,
return_sender_city,
return_sender_country,
return_shipment_at,
return_shipment_label_created_at,
return_shipment_uid,
return_tracking_id,
return_tracking_number,
return_warehouse,
sender_city,
sender_country,
serial_number,
shipment_at,
shipment_label_created_at,
shipment_service,
shipment_uid,
shipping_country,
store,
subscription_id,
tracking_id,
tracking_number,
transit_time,
variant_sku,
warehouse,
warehouse_detail,
warehouse_lt from tmp_allocation_shipment;

GRANT SELECT ON ods_operations.allocation_shipment TO operations_redash;
GRANT SELECT ON ods_operations.allocation_shipment TO redash_pricing;
GRANT SELECT ON ods_operations.allocation_shipment TO GROUP risk_users;
