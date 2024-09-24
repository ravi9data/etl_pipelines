DROP TABLE if exists monitoring.or3;

Create TABLE monitoring.or3 as

WITH allocation_rank as (
SELECT id,
rank () over (partition by asset__c order by allocated__c) as allocationrank_per_asset,
count(*) over (partition by asset__c) as total_allocations
from
stg_salesforce.customer_asset_allocation__c),
shipcloud_events as (
select shipment_id,
		status,
		details,
		location,
	    event_timestamp,
		count(*) OVER (PARTITION BY shipment_id) as total_events, 
		rank() OVER (PARTITION BY shipment_id ORDER BY event_timestamp ASC)as rank_event
	    from trans_dev.tracking_events_new 
	    group by 1,2,3,4,5),
last_event as (
select shipment_id,
		status,
		details,
		location,
	    event_timestamp,
	    total_events,
	    rank_event
	    from shipcloud_events
	    where  total_events = rank_event),
allo_mapping as (
select a.id as allocation_id, l.*
from stg_salesforce.customer_asset_allocation__c a 
left join last_event l on a.shipcloud_shipment_id__c = l.shipment_id)
Select 
a.allocation_id,
ast.serial_number,
a.failed_delivery_at,
a.asset_id,
ast.initial_price,
ast.asset_status_original,
a.picked_by_carrier_at,
ast.asset_name,
ar.allocationrank_per_asset as allocation_rank_,
ar.total_allocations as total_allocations_,
sysdate as sys_date,
datediff(day,a.failed_delivery_at::timestamp without time zone,sys_date) as time_diff,
am.shipment_id,
am.status,
am.details,
am.location,
event_timestamp
	FROM
ods_production.allocation a
inner join ods_production.asset ast on ast.asset_id = a.asset_id
left join allo_mapping am on a.allocation_id = am.allocation_id
left join allocation_rank ar on ar.id = a.allocation_id
where failed_delivery_at IS NOT NULL
and ast.asset_status_original = 'RETURNED'
and total_allocations_ = allocation_rank_
order by failed_delivery_at DESC;

GRANT SELECT ON monitoring.or3 TO tableau;
