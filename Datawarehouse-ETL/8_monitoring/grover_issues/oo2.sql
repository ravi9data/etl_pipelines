Drop table if exists monitoring.oo2;

Create TABLE monitoring.oo2 as
WITH allocation_rank as (
SELECT id,
rank () over (partition by asset__c order by allocated__c) as allocationrank_per_asset,
count(*) over (partition by asset__c) as total_allocations
from
stg_salesforce.customer_asset_allocation__c)
Select 
	sea.id,
	sea.allocation_id,
	a.subscription__c,
	ast.asset_status_original,
	sea.previous_state,
	sea.state,
	a.allocated__c,
	sea."date",
	a.shipment_tracking_number__c,
	a.delivered__c,
	ar.allocationrank_per_asset,
    ar.total_allocations,
	rank () over (partition by a.subscription__c order by a.allocated__c DESC ) as rank_per_subs,
	datediff(day,a.allocated__c::timestamp,sea."date") as time_diff
from 
	stg_salesforce_events.allocation sea
inner join stg_salesforce.customer_asset_allocation__c a on sea.allocation_id = a.id
inner join stg_salesforce.subscription__c s on a.subscription__c = s.id
inner join ods_production.asset ast on a.asset_serial_number__c = ast.serial_number
left join allocation_rank ar on a.id = ar.id
	where state = 'DELIVERED'
	and previous_state = 'READY TO SHIP'
	and a.asset_status__c != 'LOCKED DEVICE'
	order by a.allocated__c DESC;

GRANT SELECT ON monitoring.oo2 TO tableau;
