drop table if exists dm_operations.failed_deliveries;
create table dm_operations.failed_deliveries as
select 
	date_trunc('day', 
				case when outbound_bucket in (	'Failed Delivery', 
												'Failed Delivery Candidate', 
												'Delivered - FD Attempt')
					 then coalesce(
					 	  coalesce( 
					 	  coalesce(failed_delivery_at, 
					 			   failed_delivery_candidate),
					 			   return_delivery_date_old),
					 			   refurbishment_start_at)
					 when outbound_bucket = 'Delivered'
					 then delivered_at 
					 when outbound_bucket = 'Unclear Shipments'
					 then coalesce(
					 	  coalesce( 
					 	  --coalesce(last_timestamp, 
					 			   shipment_at, --),
					 			   shipment_label_created_at),
					 			   ready_to_ship_at)
					 end) as fact_date,
	customer_type,
	warehouse,
	shipping_country,
	carrier,
	shipment_service,
	infra,
	case when outbound_bucket in (	'Failed Delivery', 
								  	'Failed Delivery Candidate', 
								  	'Delivered - FD Attempt')
		 then coalesce (failed_reason, 'Not Available') end failed_reason ,
	count(case when outbound_bucket = 'Failed Delivery' then allocation_id end) as total_failed_deliveries,
	count(case when outbound_bucket = 'Failed Delivery Candidate' then allocation_id end) as  total_fd_candidates,
	count(case when outbound_bucket = 'Delivered - FD Attempt' then allocation_id end) as total_fd_attempt,
	count(case when outbound_bucket = 'Delivered' then allocation_id end) as total_delivered,
	count(case when outbound_bucket = 'Unclear Shipments' then allocation_id end) as total_unclear_shipments,
	count(allocation_id) as total_shipments
from ods_operations.allocation_shipment 
where outbound_bucket not in ('Partners Offline', 'Cancelled Before Shipment', 'Ready to Ship', 'In Transit')
group by 1, 2, 3, 4, 5, 6, 7, 8;

GRANT SELECT ON dm_operations.failed_deliveries TO tableau;
