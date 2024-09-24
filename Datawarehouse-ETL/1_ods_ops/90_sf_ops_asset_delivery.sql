--drop table if exists staging.sf_ops_asset_delivery; --full_load
--create table staging.sf_ops_asset_delivery as --full_load
insert into staging.sf_ops_asset_delivery --increamental
select * from ( --increamental
with 
numbers as ( 
  	select * from public.numbers where ordinal < 100
),
legacy as (
	select
    	shipment_id as tracking_id, 
    	shipment_carrier as carrier,
  		case when json_extract_path_text(json_extract_path_text(shipment, 'metadata'), 'is_return') = 'true'
  			 then 'inbound' else 'outbound' end as shipment_type,
    	json_extract_path_text(shipment, 'carrier_tracking_no') as tracking_number,
	    	json_extract_path_text(json_extract_path_text(shipment, 'to'),'country') as shipping_country,
	    	json_extract_path_text(
	    	json_extract_array_element_text(json_extract_path_text(shipment, 'packages'), 0),'tracking_events') as shipment_extract 
		from stg_salesforce.sf_ops_asset_delivery
		where updatedat > (current_date - 7) --increamental
	),
	legacy_joined as (
	    select 
	        l.tracking_id,
	        l.carrier,
      		l.shipment_type,
	        l.tracking_number,
	        l.shipping_country,
	        json_array_length(l.shipment_extract, true) as number_of_items,
	        json_extract_array_element_text(l.shipment_extract, numbers.ordinal::int,true) as item
	    from legacy l
	    cross join numbers
	    where numbers.ordinal < json_array_length(l.shipment_extract, true)
	  )
	select 
	   tracking_id,
	   carrier,
  	   shipment_type,
	   tracking_number,
	   shipping_country,
	   json_extract_path_text(item, 'id') as event_id,
	   json_extract_path_text(item, 'status') as status,
	   json_extract_path_text(item, 'details') as details,
	   json_extract_path_text(item, 'location') as location,
	   json_extract_path_text(item, 'timestamp')::timestamp as event_timestamp
	from legacy_joined
	where event_id not in (select event_id from staging.sf_ops_asset_delivery)) --incremental
  ;