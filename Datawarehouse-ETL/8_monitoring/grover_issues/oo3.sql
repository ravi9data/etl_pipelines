DROP TABLE if exists monitoring.oo3;

Create TABLE monitoring.oo3 as
	
SELECT
caa.id,
a.previous_state,
a.state,
s.status,
s.subscription_value,
a."date",
caa.wh_goods_order_id__c,
caa.picked_by_carrier__c,
caa.shipment_tracking_number__c,
caa.return_picked_by_carrier__c,
caa.cancelltion_returned__c,
datediff(day,caa.picked_by_carrier__c::timestamp,caa.return_picked_by_carrier__c::timestamp) as time_diff
from
stg_salesforce_events.allocation a
inner join stg_salesforce.customer_asset_allocation__c caa on a.allocation_id = caa.id
inner join ods_production.subscription s on caa.subscription__c = s.subscription_id
where a.state = 'IN TRANSIT'
and a.previous_state = 'SHIPPED'
order by a."date" DESC;

GRANT SELECT ON monitoring.oo3 TO tableau;
