drop table if exists monitoring.ot1;
create table monitoring.ot1 as 
with tracking_events as (
	select
		shipment_uid, 
		tracking_id,
		"location" as last_location,
		details as last_details,
		event_timestamp as last_timestamp,
		"status" as last_status
	from ods_operations.tracking_events 
	where is_last_event
),
union_shipment as (
	select 
		'Outbound' as shipment_type,
		allocation_id ,
		asset_id,
		serial_number,
		shipment_uid ,
		tracking_number ,
		tracking_id ,
		order_id,
		infra ,
		carrier ,
		shipment_service ,
		shipping_country ,
		receiver_city,
		customer_type ,
		outbound_bucket ,
		failed_reason ,
		failed_event_bucket ,
		failed_timestamp 
	from ods_operations.allocation_shipment a
	where allocation_status_original = 'SHIPPED'
	  and delivered_at is null 
	  and shipment_at < current_date  - 10
	  and return_shipment_at is null 
	  and shipping_country != 'United States'
	  and failed_delivery_at is null
	  and a.store <> 'Partners Offline'
	--
	union 
	--  
	select 
		'Return' as shipment_type,
		allocation_id ,
		asset_id,
		serial_number,
		return_shipment_uid as shipment_uid ,
		return_tracking_number as tracking_number ,
		return_tracking_id tracking_id ,
		order_id,
		infra ,
		return_carrier ,
		shipment_service ,
		return_sender_country as shipping_country ,
		a.return_sender_city as receiver_city,
		customer_type ,
		outbound_bucket ,
		null as failed_reason ,
		null as failed_event_bucket ,
		null as failed_timestamp 
	from ods_operations.allocation_shipment a
	where allocation_status_original = 'IN TRANSIT'
	  and coalesce (return_delivery_date, return_delivery_date_old ) is null 
	  and return_shipment_at  < current_date  - 10
	  and return_sender_country != 'United States'
)
select 
	a.shipment_type,
	a.allocation_id ,
	a.asset_id,
	a.serial_number,
	a.shipment_uid ,
	a.tracking_number ,
	a.tracking_id ,
	a.order_id,
	a.infra ,
	a.carrier ,
	a.shipment_service ,
	a.shipping_country ,
	a.receiver_city,
	a.customer_type ,
	te.last_status ,
	te.last_location ,
	te.last_details ,
	te.last_timestamp ,
	a.outbound_bucket ,
	a.failed_reason ,
	a.failed_event_bucket ,
	a.failed_timestamp 
from union_shipment a
left join tracking_events te on coalesce(a.shipment_uid, a.tracking_id) = coalesce(te.shipment_uid, te.tracking_id)
;

GRANT SELECT ON monitoring.ot1 TO tableau;
