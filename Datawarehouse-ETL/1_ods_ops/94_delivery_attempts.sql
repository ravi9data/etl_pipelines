drop table if exists ods_operations.delivery_attempts;
create table ods_operations.delivery_attempts as
with identify_attempts as (
select 
	shipment_uid ,
	tracking_id ,
	carrier ,
	status ,
	tracking_number ,
	shipping_country ,
	details ,
	"location" ,
	event_timestamp ,
	max(case when event_bucket = 'failed' then event_timestamp end) 
	  over (partition by coalesce(shipment_uid, tracking_id)) as latest_failed,
  	--specific to ups austria and spain shipments, latest location scan means out for delivery
    case when event_timestamp = 
  			max(case when details = 'Location Scan' then event_timestamp end)
  			   over (partition by coalesce(shipment_uid, tracking_id))
			and 
			--only if there is no out for delivery event
			count(case when status in ('out for delivery', 'out_for_delivery') 
	  		  				and details <> 'Die Sendung wurde im Paketzentrum bearbeitet.' then 1 end) 
							over (partition by coalesce(shipment_uid, tracking_id)) = 0
			and 
			--only checking Location Scan is not enough. Especially, in early stages of shipment
			--we my have this even right after shipped, it is impossible to know if it is really out for delivery or not
			--so, be on safe side, and look for a delivered event
			count(case when status in ('delivered') then 1 end) 
							over (partition by coalesce(shipment_uid, tracking_id)) > 0
			--also exclude NL based events shipped from Roermond
			--this must be reworked if we open a wh in Madrid
			and right("location", 2) in ('AT', 'ES')
  		 then true end as is_latest_location_scan,
	case 
	   when 
	   	  --check for failed event to exclude attempts created while parcel is on way back to wh 
	      --if there is a failed event, then exclude all attempts afterwards
		  (latest_failed is null or event_timestamp < latest_failed )
		   and
		  --1. look for out for delivery events, one exception exists 
		  --2. look for specific details even status is not out for delivery
		  (
		    (status in ('out for delivery', 'out_for_delivery') 
	  		  and details <> 'Die Sendung wurde im Paketzentrum bearbeitet.')
			 or
			(details like 'Fahrzeugbeladung%'
			  or details like '%Zustellfahrzeug geladen%')
             or 
            is_latest_location_scan
		  )
		then true  
		end as is_delivery_attempt
from ods_operations.tracking_events 
where shipment_type = 'outbound'
)
	select 
		shipment_uid,
		tracking_id ,
		carrier ,
		status ,
		tracking_number ,
        shipping_country,
		details ,
		"location" ,
		event_timestamp ,
		row_number () over 
			 (partition by coalesce(shipment_uid, tracking_id), date_trunc('day', event_timestamp)
			  order by event_timestamp asc) as number_of_attempts
	from identify_attempts 
	where is_delivery_attempt;