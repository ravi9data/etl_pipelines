drop table if exists ods_production.payment_refund_operations;
create table ods_production.payment_refund_operations as

WITH error_cases AS 
(
SELECT
sp.subscription_id,
sp.allocation_id,
payment_number,
date_trunc('month',due_date) as due_month,
count(subscription_payment_id) as payments,
TRUE AS ERROR
from ods_production.payment_subscription sp
left join ods_production.subscription s on s.subscription_id=sp.subscription_id
where due_date <CURRENT_DATE
AND payment_number > 1
group by 1,2,3,4
having payments > 1
order by date_trunc('month',due_date) desc 
),
a as (
select pr.*, 
ps.subscription_payment_name as sf_id,
ps.created_at as payment_created_at,
ps.asset_id as payment_asset_id,
ps.allocation_id,
ps.payment_type,
ps.payment_number,
ps.subscription_payment_id as related_subscription_payment_id
from ods_production.payment_refund PR 
left join ods_production.payment_subscription ps on pr.subscription_payment_id = ps.subscription_payment_id 
where pr.status = 'PAID' 
and related_payment_type = 'SUBSCRIPTION_PAYMENT'
UNION
select pr.*, 
ap.asset_payment_sfid as sf_id,
ap.created_at as payment_created_at,
ap.asset_id as payment_asset_id,
ap.allocation_id,
ap.payment_type,
NULL as payment_number,
ap.related_subscription_payment_id as related_subscription_payment_id
from ods_production.payment_refund PR 
left join ods_production.payment_asset ap on pr.asset_payment_id = ap.asset_payment_id
where pr.status = 'PAID' 
and related_payment_type = 'ASSET_PAYMENT'
),
user_change as (
select id,name, case when name in	('DA & IT Backend Grover', 'DA & IT Grover', 'Tech Script', 'Tech Backend')
then 'System'
	else 'Manual' end as user_type
	 from 
stg_salesforce."user")
SELECT a.*, u.user_type,u.name, e.error,
cr.cancellation_date,
cr.cancellation_reason_new,
case when s.first_asset_delivery_date is not null then TRUE 
	else case when  allo.delivered_at is not null then TRUE else FALSE end
	 end as delivered,
	 ps.subscription_id as related_subscription_id,
	 s.allocation_status,
Case 
	when related_payment_type = 'ASSET_PAYMENT' AND a.payment_type = 'SHIPMENT' AND delivered = FALSE AND allocation_status = 'PENDING ALLOCATION' THEN 'FAIL TO FULFIL'
	when related_payment_type = 'SUBSCRIPTION_PAYMENT' AND a.payment_type = 'FIRST' AND delivered = FALSE AND allocation_status = 'PENDING ALLOCATION' THEN 'FAIL TO FULFIL'
when related_payment_type = 'SUBSCRIPTION_PAYMENT' AND a.payment_type = 'FIRST' AND delivered = FALSE AND allocation_status = 'ALLOCATED' THEN 'FAILED DELIVERY'
when related_payment_type = 'ASSET_PAYMENT' AND a.payment_type = 'SHIPMENT' AND delivered = FALSE AND allocation_status = 'ALLOCATED' THEN 'FAILED DELIVERY'
when related_payment_type = 'SUBSCRIPTION_PAYMENT' AND a.payment_type = 'FIRST' AND delivered = TRUE AND allocation_status = 'ALLOCATED' AND error is null AND (reason = 'Revocation' OR  reason = 'Order Cancelled') THEN 'WIDERRUF'
when related_payment_type = 'ASSET_PAYMENT' AND a.payment_type = 'SHIPMENT' AND delivered = TRUE AND allocation_status = 'ALLOCATED' AND error is null THEN 'WIDERRUF'
when related_payment_type = 'SUBSCRIPTION_PAYMENT' AND a.payment_type = 'FIRST' AND delivered = TRUE AND allocation_status = 'ALLOCATED' AND error is null THEN 'EXCEPTIONS'
when related_payment_type = 'SUBSCRIPTION_PAYMENT' AND a.payment_type = 'RECURRENT' AND delivered = TRUE AND allocation_status = 'ALLOCATED' AND error is null THEN 'RECURRENT'
when error = TRUE THEN 'ERROR'
when related_payment_type = 'ASSET_PAYMENT' AND a.payment_type = 'CUSTOMER BOUGHT' AND a.allocation_id is not null THEN 'PURCHASE REVOCATION'
ELSE 'OTHERS'
END as refund_category
From a 
left join error_cases e on e.allocation_id = a.allocation_id and e.due_month = date_trunc('month',a.created_at)
left join ods_production.payment_subscription ps on a.related_subscription_payment_id = ps.subscription_payment_id
left join ods_production.subscription s on s.subscription_id = ps.subscription_id 
left join ods_production.subscription_cancellation_reason cr on s.subscription_id = cr.subscription_id
left join ods_production.allocation allo on allo.allocation_id = a.allocation_id
left join user_change u on a.created_by = u.id;

GRANT SELECT ON ods_production.payment_refund_operations TO tableau;
