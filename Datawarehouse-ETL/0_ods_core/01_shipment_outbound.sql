drop table if exists staging.shipment_outbound;
create table staging.shipment_outbound as 
with 
--
-- inital edits, define first events per label
--
deduplication_pre as (
	select 
		s.uid as shipment_uid,
		s.event_name ,
		s.order_number as order_id,
		case when s.order_mode = 'MIX' then 'SWAP' else s.order_mode end as order_mode,
		nullif (json_extract_path_text(s.payload, 'package_id'), '') as package_id,
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
		s.warehouse_code ,
		s.sender_country ,
		s.sender_city,
		s.sender_zipcode ,
		s.receiver_country ,
		s.receiver_city ,
		s.receiver_zipcode ,
		s.allocation_uid ,
		nullif (s.contract_id, '') as contract_id ,
		coalesce (nullif (s.transition_timestamp, '')::timestamp , s.event_timestamp::timestamp) _timestamp,
		--
		case when s.event_name in ('created', 'shipped', 'delivered', 'ready for pick up by receiver') then
			row_number () over (partition by s.allocation_uid, s.uid, s.event_name order by _timestamp asc)
		--
			 when s.event_name in ('failed to deliver', 'exception', 'cancelled') then
			row_number () over (partition by s.allocation_uid, s.uid, s.event_name order by _timestamp desc)
		end rn 
	from staging.stream_shipping_shipment_change_v3  s
	where nullif (s.allocation_uid , '') is not null
	and s.service = 'outbound'
 ),
 --
 -- prioritize events, count total events and check if the label has cancelled event
 --
 prioritize as (
	 select 
	 	allocation_uid,
	 	shipment_uid,
	 	event_name ,
	 	order_id,
	 	order_mode,
	 	package_id,
	 	tracking_id,
	 	tracking_number,
	 	tracking_url,
	 	shipping_label_url,
	 	shipping_profile,
	 	carrier,
	 	customer_id,
	 	customer_type,
	 	warehouse_code,
	 	sender_country,
	 	sender_city,
	 	sender_zipcode ,
	 	receiver_country ,
		receiver_city ,
		receiver_zipcode ,
		contract_id ,
 		case when event_name = 'delivered' then 1 
			 when event_name = 'ready for pick up by receiver' then 5 
			 when event_name = 'failed to deliver' then 11
			 when event_name = 'shipped' then 15 
			 when event_name = 'exception' then 21
			 when event_name = 'created' then 25 
			 when event_name = 'cancelled' then 31
		 end event_prio,
		 count(event_name) over (partition by allocation_uid, shipment_uid) total_events,
		 count(case when event_name = 'cancelled' then 1 end) over (partition by allocation_uid, shipment_uid) total_cancelled,
		 _timestamp as transition_timestamp 
	 from deduplication_pre
	 where rn=1
),
--
-- select most recent label, 
-- if the label has a cancelled event, then avoid 
-- if the label has a delivered (or shipped if not delivered) event, then consider it first 
-- if the label has more event, then consider it
--
define_events as (
	select 
	 	allocation_uid,
	 	shipment_uid,
	 	event_name ,
	 	order_id,
	 	order_mode,
	 	package_id,
	 	tracking_id,
	 	tracking_number,
	 	tracking_url,
	 	shipping_label_url,
	 	shipping_profile,
	 	carrier,
	 	customer_id,
	 	customer_type,
	 	warehouse_code,
	 	sender_country,
	 	sender_city,
	 	sender_zipcode ,
	 	receiver_country ,
		receiver_city ,
		receiver_zipcode ,
		contract_id ,
		transition_timestamp ,
		event_prio ,
		total_events,
		case when total_cancelled > 0 then 1 else 0 end cancelled_label,
		first_value (shipment_uid) over (partition by allocation_uid order by cancelled_label , event_prio, transition_timestamp , total_events desc 
			rows between unbounded preceding and unbounded following)  as selected_label ,
		case when shipment_uid = selected_label then 0 else 1 end as label_ranking 
	from prioritize
),
--
-- add row number per prioritization rules
--
final_prio as (
	select 
	 	allocation_uid,
	 	shipment_uid,
	 	event_name ,
	 	order_id,
	 	order_mode,
	 	package_id,
	 	tracking_id,
	 	tracking_number,
	 	tracking_url,
	 	shipping_label_url,
	 	shipping_profile,
	 	carrier,
	 	customer_id,
	 	customer_type,
	 	warehouse_code,
	 	sender_country,
	 	sender_city,
	 	sender_zipcode ,
	 	receiver_country ,
		receiver_city ,
		receiver_zipcode ,
		contract_id ,
		transition_timestamp ,
		row_number () over (partition by allocation_uid order by label_ranking , event_prio , transition_timestamp, total_events desc) rn
	from define_events 
),
--
-- get first non-null value of each column sorted by ranking
--
first_val as (
select 
	allocation_uid,
	first_value(shipment_uid ignore nulls) 	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as shipment_uid,
	first_value(order_id ignore nulls)		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as order_id,
	first_value(order_mode ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as order_mode,
	first_value(package_id ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as package_id,
	first_value(tracking_id ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as tracking_id,
	first_value(tracking_number ignore nulls) over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as tracking_number,
	first_value(tracking_url ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as tracking_url,
	first_value(shipping_label_url ignore nulls) over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as shipping_label_url,
	first_value(shipping_profile ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as shipping_profile,
	first_value(carrier ignore nulls)			over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as carrier,
	first_value(customer_id ignore nulls)		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as customer_id,
	first_value(customer_type ignore nulls)		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as customer_type,
	first_value(warehouse_code ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as warehouse_code,
	first_value(sender_country ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as sender_country,
	first_value(sender_city ignore nulls)		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as sender_city,
	first_value(sender_zipcode  ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as sender_zipcode,
	first_value(receiver_country  ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as receiver_country,
	first_value(receiver_city  ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as receiver_city,
	first_value(receiver_zipcode  ignore nulls)	over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as receiver_zipcode,
	first_value(contract_id  ignore nulls) 		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as contract_id,
	first_value(case when event_name = 'created' then transition_timestamp end ignore nulls) 
		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as created_date,
	first_value(case when event_name = 'shipped' then transition_timestamp end ignore nulls) 
		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as shipped_date,
	first_value(case when event_name = 'exception' then transition_timestamp end ignore nulls) 
		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as exception_date,
	first_value(case when event_name = 'cancelled' then transition_timestamp end ignore nulls) 
		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as cancelled_date,
	first_value(case when event_name = 'failed to deliver' then transition_timestamp end ignore nulls) 
		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as failed_delivery_at,
	first_value(case when event_name = 'delivered' then transition_timestamp end ignore nulls) 
		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as delivered_date,
	first_value(transition_timestamp ignore nulls) 
		over (partition by allocation_uid order by rn rows between unbounded preceding and unbounded following) as updated_at,
	rn
from final_prio s 
)
select
	s.allocation_uid ,
	s.shipment_uid,
	s.order_id,
	s.order_mode,
	s.package_id,
	s.tracking_id,
	s.tracking_number,
	s.tracking_url,
	s.shipping_label_url,
	s.shipping_profile,
	s.carrier,
	s.customer_id,
	s.customer_type,
	s.warehouse_code,
	s.sender_country,
	s.sender_city,
	s.sender_zipcode,
	s.receiver_country,
	s.receiver_city,
	s.receiver_zipcode,
	a.variant_sku,
	nullif(a.salesforce_allocation_id , '') salesforce_allocation_id,
	a.asset_uid,
	nullif (a.asset_id , '') as salesforce_asset_id,
	a.serial_number,
	s.contract_id ,
	s.created_date ,
	s.shipped_date ,
	s.exception_date ,
	s.cancelled_date ,
	s.failed_delivery_at ,
	s.delivered_date ,
	s.updated_at 
from first_val s 
left join staging.spectrum_operations_order_allocated a
on s.allocation_uid = a.allocation_uid 
where rn = 1;