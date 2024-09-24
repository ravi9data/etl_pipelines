--drop table if exists staging.shipcloud_incoming; --full_load
--create table staging.shipcloud_incoming as --full_load
insert into staging.shipcloud_incoming --incremental
select * from (  --incremental
with 
numbers as ( 
  	select * from public.numbers where ordinal < 100
),
new_infra_pre as (
	select 
		shipment_id,
		unified_carrier as carrier ,
		unified_carrier_tracking_no as tracking_number ,
  		case when json_extract_path_text(unified_metadata,'is_return') = 'true'
  			 then 'inbound' else 'outbound' end as shipment_type ,
		json_extract_path_text(unified_to,'country') as shipping_country ,
		json_extract_path_text (
			json_extract_array_element_text(original_packages, 0),
			'tracking_events' ) as tracking_events  ,
		row_number() over (partition by shipment_id order by event_timestamp desc) as idx
		from stg_kafka_events_full.stream_shipping_shipcloud_incoming_notification 
  	    where event_timestamp > (current_date - 7) --incremental
	),
new_infra as (
	select 
		shipment_id as tracking_id,
		carrier ,
		tracking_number,
  		shipment_type,
		shipping_country ,
		tracking_events
	from new_infra_pre 
	where idx = 1 --take the latest notifications, this includes all notifications 
),
new_infra_joined as (
	select
		tracking_id,
		carrier ,
		tracking_number,
  		shipment_type,
		shipping_country ,
        json_extract_array_element_text(s.tracking_events, numbers.ordinal::int,true) as item
	from new_infra s
	cross join numbers
	where numbers.ordinal < json_array_length(s.tracking_events, true)
)
select 
	tracking_id,
	carrier ,
	tracking_number,
  	shipment_type,
	shipping_country ,
    json_extract_path_text(item, 'id') as event_id,
    json_extract_path_text(item, 'status') as status,
    json_extract_path_text(item, 'details') as details,
    json_extract_path_text(item, 'location') as location,
    json_extract_path_text(item, 'timestamp')::timestamp as event_timestamp
from new_infra_joined
where event_id not in (select event_id from staging.shipcloud_incoming)) --incremental
  ;