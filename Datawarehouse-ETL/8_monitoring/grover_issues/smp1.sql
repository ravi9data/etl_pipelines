 drop table if exists monitoring.smp1;
 create table monitoring.smp1 as 
 select 
 sp.subscription_payment_id, 
 sp.subscription_id,
 sp.asset_id,
 sp.payment_type,
 sp.payment_number,
 sp.created_at,
 sp.due_Date,
 sp.amount_due,
 s.status as subscription_status,
 s.subscription_value,
 s.subscription_revenue_due,
 s.subscription_revenue_paid,
 s.subscription_revenue_refunded,
 s.subscription_revenue_chargeback,
 s.outstanding_assets,
 s.outstanding_asset_value,
 s.store_name
 from ods_production.payment_subscription sp 
  left join master.subscription s on s.subscription_id = sp.subscription_id
 where sp.status = 'PLANNED' 
 and due_date::date < CURRENT_DATE::date;

GRANT SELECT ON monitoring.smp1 TO tableau;
