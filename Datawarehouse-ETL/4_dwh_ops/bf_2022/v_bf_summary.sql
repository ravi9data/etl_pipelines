create or replace view dm_operations.v_bf_summary AS
with time_range as 
	(select date_add('hours', -168, current_timestamp  at time zone 'CET') sd)
--ORDERS SUBMITTED
select 
	date_trunc('hours', convert_timezone('Europe/Berlin',o.submitted_date::timestamp)) as fact_date,
	--,country_name 
	'Orders Item Submitted' as metric_name,
	case when store_id in (14, 629, 621) then 'US' else 'EU' end as region,
	sum(oi.quantity) as metric_value
from ods_production.order_item  oi 
left join ods_production.order o on oi.order_id = o.order_id
where o.created_date > current_date - 14
and o.submitted_date > (select sd from time_range)
group by 1, 2, 3
union
--ORDERS APPROVED
select 
	date_trunc('hours', convert_timezone('Europe/Berlin', o.approved_date::timestamp)) as fact_date,
	--,country_name 
	'Orders Item Approved' as metric_name,
	case when store_id in (14, 629, 621) then 'US' else 'EU' end as region,
	sum(oi.quantity) as metric_value
from ods_production.order_item  oi 
left join ods_production.order o on oi.order_id = o.order_id
where o.created_date > current_date - 14
and o.approved_date > (select sd from time_range)
group by 1, 2, 3
union
--PENDING ALLOCATION
select 
	date_add('hours', hours, datum) fact_date , 
	'Pending Allocations' as metric_name ,
	region,
	pending_allocations as metric_value
from dm_operations.v_bf_pending_allocations
where fact_date > (select sd from time_range)
union
--PENDING PUSH TO WH 
select 
	date_add('hours', hours, datum) fact_date , 
	'Pending Shipments' as metric_name ,
	region,
	pending_shipments as metric_value
from dm_operations.v_bf_pending_push_to_wh
where fact_date > (select sd from time_range)
union
--PENDING LABEL CREATION
select 
	date_add('hours', hours, datum) fact_date , 
	'Pending Label Creation' as metric_name ,
	region,
	pending_assets as metric_value
from dm_operations.v_bf_pending_label_creation 
where fact_date > (select sd from time_range)
union
--PENDING CARRIER RECEIVE
select 
	date_add('hours', hours, datum) fact_date , 
	'Pending Carrier Receive' as metric_name ,
	region,
	processed_assets as metric_value
from dm_operations.v_bf_pending_carrier_receive 
where fact_date > (select sd from time_range)
union
--PENDING DELIVERY
select 
	date_add('hours', hours, datum) fact_date , 
	'Pending Delivery' as metric_name ,
	region,
	pending_assets as metric_value
from dm_operations.v_bf_pending_deliveries 
where fact_date > (select sd from time_range)
union
--FAILED DELIVERY 
select 
	date_trunc('hours', failed_at) as fact_date , 
	'Failed Deliveries' as metric_name ,
	region,
	count(ob_shipment_unique) as metric_value_us
from dm_operations.v_bf_failed_deliveries 
where fact_date > (select sd from time_range)
group by 1, 2, 3
with no schema binding;
