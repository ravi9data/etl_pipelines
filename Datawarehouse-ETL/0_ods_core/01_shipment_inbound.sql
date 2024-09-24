drop table if exists staging.shipment_inbound;
create table staging.shipment_inbound as
with 
	--since there could be multiple return labels for an allocation
	--based on shipping events, need to decide which label is not valid
	out_pre as (
		select 
			s.uid as shipment_uid,
			s.event_name ,
			s.event_timestamp::timestamp as transition_timestamp ,
			s.package_items_order_number as order_id,
			case when s.order_mode = 'MIX' then 'SWAP' else s.order_mode end as order_mode,
			case 
				when s.tracking_url ilike 'https://myhes.de/de/tracking/%' 
				then 29
				when s.tracking_url ilike 'https://track.shipcloud.io/%' 
				then 27
				else null
			end as cvar_count,
			nullif (right(s.tracking_url, (len(s.tracking_url) - cvar_count)) , '') as tracking_id,
			nullif (s.tracking_number , '') as tracking_number,
			nullif (s.tracking_url , '')  as tracking_url,
			nullif (s.shipping_label_url , '')  as shipping_label_url,
			nullif (s.shipping_profile , '')  as shipping_profile,
			nullif (upper(s.carrier) , '') as carrier ,
			s.user_id as customer_id,
			s.user_type as customer_type,
			s.sender_country ,
			s.sender_city,
			s.sender_zipcode ,
			s.receiver_country ,
			s.receiver_city ,
			s.receiver_zipcode ,
			a.variant_sku ,
			s.package_items_allocation_uid as allocation_uid,
			nullif(a.salesforce_allocation_id , '') salesforce_allocation_id,
			a.asset_uid ,
			nullif (a.asset_id , '') as salesforce_asset_id,
			a.serial_number ,
			nullif (s.package_items_contract_id, '') as contract_id ,
			ROW_NUMBER () OVER (PARTITION BY s.package_items_allocation_uid, s.uid, s.event_name  ORDER BY s.event_timestamp asc) AS idx
		from staging.stream_shipping_shipment_inbound s 
		left join staging.spectrum_operations_order_allocated a
		on s.package_items_allocation_uid = a.allocation_uid 
		where nullif (s.package_items_allocation_uid , '') is not null
		--and s.service = 'outbound'
	)
, evaluate_labels as (
	select 
		--look for delivered or failed event for allocation_uid 
		case when count(case when event_name in ('delivered', 'failed to deliver') then 1 end) over (partition by allocation_uid) > 0
			 then true else false end has_allocation_delivered_event,
			 --look for shipping event for allocation 
		case when count(case when event_name in ('delivered', 'shipped', 'ready for pick up by receiver', 'failed to deliver') then 1 end) over (partition by allocation_uid) > 0
			 then true else false end has_allocation_shipping_event,
		--look for a delivere or failed  event
		case when count(case when event_name in ('delivered', 'failed to deliver') then 1 end) over (partition by shipment_uid, allocation_uid) > 0
			 then true else false end has_delivered,
		--look for a regular shipment event
		case when count(case when event_name in ('shipped', 'ready for pick up by receiver') then 1 end) over (partition by shipment_uid, allocation_uid) > 0
			 then true else false end has_regular_event,
		--check if it is cancelled or not
		case when count(case when event_name = 'cancelled' then 1 end) over (partition by shipment_uid, allocation_uid) > 0 
			 then true else false end has_cancelled,
		--identify created events, but exclude cancelled ones
		case when event_name ='created' and not has_cancelled and not has_allocation_shipping_event 
			 then true else false end is_created,
		max(case when event_name in ('delivered', 'failed to deliver') then transition_timestamp end) over (partition by allocation_uid, shipment_uid) max_delivered_at,
		* 
	from out_pre 
	where idx = 1 
)
, decide_label as (
		select 
			case when is_created 
				 then row_number () over (partition by allocation_uid, is_created order by transition_timestamp desc) end created_idx,
			case when has_delivered  
				 then dense_rank () over (partition by allocation_uid order by max_delivered_at, shipment_uid asc) end delivered_idx,
			case /*1. exclude cancelled labels
				   2. include if there is a delivered event or not delivered yet, still in transit, 
				   3. if there is another label delivered for that allocation, then exclude others even they are in transit
				   4. if not in transit, and all in created stage then select latest one */
				 when has_cancelled then false 
				 when has_delivered and delivered_idx = 1 then true  
				 when has_regular_event and not has_allocation_delivered_event then true
				 when created_idx = 1 then true 
				 end decision,
				 *
		from evaluate_labels where is_created=false)
select 
	allocation_uid ,
	salesforce_allocation_id,
	shipment_uid,
	max(case when asset_uid  is not null 			then asset_uid end) 			as asset_uid,
    max(case when salesforce_asset_id is not null 	then salesforce_asset_id end)	as salesforce_asset_id,
    max(case when serial_number is not null 		then serial_number end)			as serial_number,
	max(case when order_id is not null 				then order_id end)				as order_id,
	max(case when order_mode  is not null 			then order_mode end)			as order_mode,
    max(case when contract_id is not null 			then contract_id end)			as contract_id,
	max(case when customer_id is not null 			then customer_id end)			as customer_id,
	max(case when sender_country is not null 		then sender_country end)		as sender_country, 
	max(case when sender_city is not null 			then sender_city end)			as sender_city,
	max(case when sender_zipcode is not null 		then sender_zipcode end)		as sender_zipcode,
	max(case when receiver_country is not null 		then receiver_country end)		as receiver_country,
	max(case when receiver_city is not null 		then receiver_city end)			as receiver_city,
	max(case when receiver_zipcode is not null 		then receiver_zipcode end)		as receiver_zipcode,
	max(case when tracking_id is not null 			then tracking_id end) 			as tracking_id,
	max(case when tracking_number is not null 		then tracking_number end) 		as tracking_number,
	max(case when tracking_url is not null		 	then tracking_url end) 			as tracking_url,
	max(case when shipping_label_url is not null 	then shipping_label_url end) 	as shipping_label_url,
	max(case when shipping_profile is not null 		then shipping_profile end) 		as shipping_profile,
	max(case when carrier is not null 				then carrier end) 				as carrier,
	min(case when event_name = 'created' 			then transition_timestamp end ) as created_date,
	min(case when event_name = 'shipped' 			then transition_timestamp end ) as shipped_date,
	max(case when event_name = 'exception' 			then transition_timestamp end ) as exception_date,
	max(case when event_name = 'failed to deliver' 	then transition_timestamp end ) as failed_delivery_at,
	min(case when event_name = 'delivered' 			then transition_timestamp end ) as delivered_date,
	max(transition_timestamp) as updated_at
from decide_label 
where decision
group by 1,2,3;


grant select on staging.shipment_inbound to bart;
