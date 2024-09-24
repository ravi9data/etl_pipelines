create or replace view dm_operations.v_bf_failed_deliveries AS
select 
    allocation_id,
    tracking_id,
    tracking_number,
    ob_shipment_unique,
	outbound_bucket ,
	store,
	infra,
	carrier,
	shipment_service,
	shipping_country,
	region,
	customer_type,
	warehouse,
	warehouse_detail,
	--timestamps are already in berlin time
	case when outbound_bucket = 'Failed Delivery' 
		 then coalesce(
			  coalesce (failed_delivery_at,
			  			failed_timestamp), 
	                  	failed_delivery_candidate)
	     else coalesce(
			  coalesce(	failed_timestamp, 
	                  	failed_delivery_candidate),
	                  	failed_delivery_at) end as failed_at,
	failed_reason,
        case when failed_event_bucket is null 
             then 
                case when outbound_bucket = 'Failed Delivery' then 'failed'
                     else 'exception'
                end
             else failed_event_bucket end as failed_event_bucket,
        failed_details
from ods_operations.allocation_shipment
where outbound_bucket in ('Failed Delivery Candidate', 'Failed Delivery')
and coalesce(
    coalesce( 
    coalesce(
    coalesce(
            failed_timestamp,
            failed_delivery_at), 
            failed_delivery_candidate),
            return_delivery_date_old),
            refurbishment_start_at) > '2022-11-07'
with no schema binding;