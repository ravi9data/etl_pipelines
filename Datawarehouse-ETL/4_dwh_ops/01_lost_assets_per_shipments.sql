drop table if exists dm_operations.lost_assets_per_shipments;
create table dm_operations.lost_assets_per_shipments as

select 
	'shipments' as kpi_type,
	s.shipment_type, 
	null as lost_reason_bucket,
	date_trunc('day', coalesce(s.shipment_at, s.allocated_at)) event_timestamp,
	s.shipping_country as country_name ,
	s.carrier,
	s.shipment_service,
	count(distinct s.shipment_unique) total_number
from dm_operations.shipments_unique s
where s.shipment_service not in ('Exclude')
--and country_name ='Germany' and shipment_service='UPS Direct Delivery Only'
group by 1,2,3,4,5,6,7
union
select 
	'lost' as kpi_type,
	case when lost_reason_bucket = 'Return-With Label'
		 then 'return' else 'outbound' end as shipment_type,
	l.lost_reason_bucket,
	date_trunc('day', lost_date) as event_timestamp, 
	l.country_name,
	l.carrier,
	l.shipment_service,
	count(distinct l.serial_number) total_number
from dm_operations.lost_assets_and_compensation l
where l.lost_reason in ('DHL', 'UPS', 'Hermes') 
	  and l.found_date is null	
group by 1,2,3,4,5,6,7;

GRANT SELECT ON dm_operations.lost_assets_per_shipments TO tableau;
