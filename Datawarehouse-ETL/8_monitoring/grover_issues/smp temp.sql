Drop table if exists monitoring.smp1_temp;

Create TABLE monitoring.smp1_temp as 

select 
 sp.subscription_payment_id, 
 sp.subscription_id,
 sp.asset_id,
 sp.payment_type,
 sp.status as payment_status,
 sp.payment_number,
 s.created_date as sub_created,
 sp.created_at as sp_created_at,
 sp.due_Date,
 s.cancellation_date,
 a.cancellation_returned_at,
 a.return_delivery_date,
 sp.amount_due,
 sa.allocated_assets,
 a.is_last_allocation_per_asset,
 ma.asset_status_original as asset_status,
 a.allocation_status_original as allocation_status,
 s.status as subscription_status,
 ma.amount_rrp,
 s.subscription_value,
 s.subscription_revenue_due,
 s.subscription_revenue_paid,
 s.subscription_revenue_refunded,
 s.subscription_revenue_chargeback,
 s.outstanding_assets,
 s.outstanding_asset_value,
 s.store_name,
 s.replacement_attempts
 from ods_production.payment_subscription sp 
  left join master.subscription s on s.subscription_id = sp.subscription_id
  left join ods_production.allocation a on a.allocation_id = sp.allocation_id
  left join ods_production.subscription_assets sa on sa.subscription_id = sp.subscription_id
  left join master.asset ma on ma.asset_id = sp.asset_id
 where sp.status = 'PLANNED' 
 and due_date::date < CURRENT_DATE::date
 and sp.due_Date < '2019-04-24'
/* and subscription_status = 'ACTIVE'*/
 /*and a.is_last_allocation_per_asset = 'true'
 and sa.allocated_assets != 1*/
 order by sp.due_Date DESC;
 
 
 
 
 
 
 
drop table if exists monitoring.smp1_v2;
create table monitoring.smp1_v2 as 
with sample as (
select distinct subscription_id, min(due_date) as first_planned_due_date 
from ods_production.payment_subscription sp
where sp.status = 'PLANNED' 
and due_date::date < CURRENT_DATE::date
group by 1)
select 
 sample.subscription_id,
 ss.is_bundle,
 s.status,
 s.subscription_value,
 s.start_date,
 s.cancellation_date,
 sample.first_planned_due_date,
 case when s.outstanding_assets>0 then null else s.last_return_shipment_at end as last_return_shipment_at,
 (coalesce((case when s.outstanding_assets>0 then null else s.last_return_shipment_at end)::date,current_date) 
 - (sample.first_planned_due_date::date+15)) as days_abused,
 floor((coalesce((case when s.outstanding_assets>0 then null else s.last_return_shipment_at end)::date,current_date) 
 - (sample.first_planned_due_date::date+15))::decimal/30::decimal) as months_abused,
 floor((coalesce((case when s.outstanding_assets>0 then null else s.last_return_shipment_at end)::date,current_date) 
 - (sample.first_planned_due_date::date+15))::decimal/30::decimal)*s.subscription_value as amount_to_charge,
 case when least(
 (((case when s.outstanding_assets>1 and ss.is_bundle = false then s.avg_asset_purchase_price else s.outstanding_rrp end)+(s.subscription_value*3)) - 
 (coalesce(s.subscription_revenue_paid,0)
 -coalesce(s.subscription_revenue_refunded,0)
 -coalesce(s.subscription_revenue_chargeback,0))),
 (floor((coalesce((case when s.outstanding_assets>0 and ss.is_bundle = false then null else s.last_return_shipment_at end)::date,current_date) 
 - (sample.first_planned_due_date::date+15))::decimal/30::decimal)*s.subscription_value)
 ) <= 0 then 0 else 
 least(
 (((case when s.outstanding_assets>1 and ss.is_bundle = false then s.avg_asset_purchase_price else s.outstanding_rrp end)+(s.subscription_value*3)) - 
 (coalesce(s.subscription_revenue_paid,0)
 -coalesce(s.subscription_revenue_refunded,0)
 -coalesce(s.subscription_revenue_chargeback,0))),
 (floor((coalesce((case when s.outstanding_assets>0 then null else s.last_return_shipment_at end)::date,current_date) 
 - (sample.first_planned_due_date::date+15))::decimal/30::decimal)*s.subscription_value)
 ) end as amount_to_charge_mietkauf_cap,
 s.delivered_assets,
 s.returned_assets,
 s.outstanding_assets,
 s.outstanding_rrp,
 s.avg_asset_purchase_price,
 (case when s.outstanding_assets>1 then s.avg_asset_purchase_price else s.outstanding_rrp end)+(s.subscription_value*3) as mietkauf_price,
 coalesce(s.subscription_revenue_paid,0)
 -coalesce(s.subscription_revenue_refunded,0)
 -coalesce(s.subscription_revenue_chargeback,0) as net_amount_paid
from sample 
inner join master.subscription s 
 on s.subscription_id = sample.subscription_id
left join ods_production.subscription ss on ss.subscription_id=s.subscription_id
;

GRANT SELECT ON monitoring.smp1_temp TO tableau;
GRANT SELECT ON monitoring.smp1_v2 TO tableau;
