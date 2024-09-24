drop table if exists ods_operations.failed_deliveries;
create table ods_operations.failed_deliveries as
with  
list_fd_shipments as (
	select 
  		shipment_uid,
  		tracking_id,
  		coalesce(shipment_uid, tracking_id) as unique_temp,
  		tracking_number,
  		status,
  		location,
  		details,
  		event_timestamp,
  		failed_reason,
		event_bucket,
		case when event_bucket = 'failed' then 1 
			 when event_bucket = 'exception' then 2
			 else 3 end bucket_rank,
		case when failed_reason in ('Customer Refused',
									'Wrong Identity & Age',
									'Shipment Damaged & Not Found')
			 then 1 
			 when failed_reason in ('Address Unknown / Wrong')
			 then 2
			 when failed_reason in ('Customer Not Available',
			 						'Shipment Conditions Not Met')
			 then 3
			 else 4 end reason_rank,
		count(case when failed_reason is not null then 1 end) 
  		  over (partition by coalesce(shipment_uid, tracking_id)) as is_failed
   from ods_operations.tracking_events  
   ),
tracking_events_pre as (
	select 
  		shipment_uid,
		tracking_id ,
  		tracking_number,
		status as last_status,
		"location" as last_location,
		details as last_details,
		event_timestamp as last_timestamp ,
		last_value (failed_reason ignore nulls) 
			over (partition by unique_temp order by bucket_rank desc, reason_rank desc, event_timestamp asc 
                  rows between unbounded preceding and unbounded following) as failed_reason_last,
		last_value (case when failed_reason is not null then event_bucket end ignore nulls) 
			over (partition by unique_temp order by bucket_rank desc, reason_rank desc, event_timestamp asc 
                  rows between unbounded preceding and unbounded following) as event_bucket,
		last_value (case when failed_reason is not null then "location" end ignore nulls) 
			over (partition by unique_temp order by bucket_rank desc, reason_rank desc, event_timestamp asc 
                  rows between unbounded preceding and unbounded following) as failed_location_last,
		last_value (case when failed_reason is not null then details end ignore nulls) 
			over (partition by unique_temp order by bucket_rank desc, reason_rank desc, event_timestamp asc 
                  rows between unbounded preceding and unbounded following) as failed_details_last,
		last_value (case when failed_reason is not null then event_timestamp end ignore nulls) 
			over (partition by unique_temp order by bucket_rank desc, reason_rank desc, event_timestamp asc 
                  rows between unbounded preceding and unbounded following) as failed_timestamp,
		row_number () over (partition by unique_temp order by event_timestamp desc) idx 
	from list_fd_shipments
	where is_failed > 0)
select 
  	shipment_uid,
	tracking_id ,
  	tracking_number,
	last_status ,
	last_location  ,
	last_details ,
	last_timestamp ,
	failed_reason_last as failed_reason ,
	event_bucket,
	failed_location_last as failed_location, 
	failed_details_last as failed_details, 
	failed_timestamp
from tracking_events_pre 
where idx = 1;

GRANT SELECT ON ods_operations.failed_deliveries TO operations_redash;
