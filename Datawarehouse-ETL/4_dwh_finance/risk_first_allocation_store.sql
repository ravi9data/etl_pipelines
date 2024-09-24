drop table if exists dwh.risk_first_allocation_store;
create table dwh.risk_first_allocation_store as(
SELECT
sp.*,
case 
 when s.store_commercial like ('%B2B%') 
  then 'B2B-Total'
 when csd.first_subscription_acquisition_channel like ('%Partners%') 
  then 'Partnerships-Total'
 when csd.first_subscription_store like 'Grover - Germany%'
  then 'Grover-DE'
 when csd.first_subscription_store in ('Grover - UK online')
  then 'Grover-UK'
   when csd.first_subscription_store in ('Grover - Netherlands online')
  then 'Grover-NL'
   when csd.first_subscription_store in ('Grover - Austria online')
  then 'Grover-Austria'
   when csd.first_subscription_store in ('Grover - Spain online')
  then 'Grover-Spain'
 when csd.first_subscription_store in ('Grover - United States online')
  then 'Grover-US'
 else 'Grover-DE'
 end as first_subscription_store
FROM
master.subscription_payment sp
left join
master.subscription s
on s.subscription_id = sp.subscription_id 
left join
 ods_production.customer_subscription_details csd 
 on csd.customer_id = s.customer_id);

 GRANT SELECT ON dwh.risk_first_allocation_store TO tableau;
 