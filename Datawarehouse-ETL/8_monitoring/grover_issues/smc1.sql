truncate table monitoring.smc1;
insert into monitoring.smc1  
with a as (
select 
s.id, 
s.date_cancellation__c, 
s.date_cancellation_requested__c, 
s.cancellation_note__c, 
s.cancellation_reason__c, 
s.cancellation_reason_picklist__c, 
s.lastmodifiedbyid,
case 
when s.lastmodifiedbyid ='00558000000FxVXAA0' then 'Backend Grover'
when s.lastmodifiedbyid ='00558000003VWkIAAW' then 'Claudia Runge'
when s.lastmodifiedbyid ='00558000004TXI8AAO' then 'Pascal Braatz'
when s.lastmodifiedbyid ='00558000003rAvkAAE' then 'Dennies Möller'
when s.lastmodifiedbyid ='00558000004mejYAAQ' then 'Lisa Pfeifer'
when s.lastmodifiedbyid ='00558000004SmWXAA0' then 'Tobiasz Klatt'
when s.lastmodifiedbyid ='00558000004JFXyAAO' then 'Dennis Bayram'
when s.lastmodifiedbyid ='00558000003VWkHAAW' then 'Chloe Martin'
when s.lastmodifiedbyid ='00558000004Qbr1AAC' then 'Tugce Telli'
when s.lastmodifiedbyid ='0051t000003964ZAAQ' then 'Tusamba Lubaka'
when s.lastmodifiedbyid ='00558000003VWkGAAW' then 'Mehdi Sangsari'
when s.lastmodifiedbyid ='00558000004yirRAAQ' then 'Iraida Pak'
when s.lastmodifiedbyid ='00558000000FVaEAAW' then 'Grover Backend 2'
when s.lastmodifiedbyid ='00558000003VWkBAAW' then 'Juan Barredo de Valenzuela'
when s.lastmodifiedbyid ='00558000000FdKrAAK' then 'OPS Manager Grover'
else 'Others' end as modified_by,
s.lastmodifieddate,
s.status__c,
date_first_asset_delivery__c,
s."name" as subscription_name
from stg_salesforce.subscription__c s 
where (date_cancellation__c is not null or status__c = 'CANCELLED')
order by 2 desc)
select distinct 
a.id, 
a.date_cancellation__c, 
a.date_cancellation_requested__c, 
a.cancellation_note__c, 
a.cancellation_reason__c, 
a.cancellation_reason_picklist__c, 
a.lastmodifiedbyid,
a.modified_by,
a.lastmodifieddate,
a.status__c,
a.date_first_asset_delivery__c,
sa.delivered_assets,
sa.returned_assets,
sa.in_transit_assets,
sa.outstanding_assets, 
sa.outstanding_purchase_price, 
sa.days_on_rent,
sa.last_return_shipment_at,
sc.subscription_revenue_due, 
sc.subscription_revenue_paid, 
sc.subscription_revenue_refunded, 
sc.subscription_revenue_chargeback,
sc.last_payment_status,
sr.cancellation_reason_churn,
sr.cancellation_reason_new,
sr.cancellation_reason,
s.subscription_value,
a.subscription_name
from a 
left join ods_production.subscription s on s.subscription_id=a.id
left join ods_production.subscription_assets sa on sa.subscription_id=a.id
left join ods_production.subscription_cashflow sc on sc.subscription_id=a.id
left join ods_production.subscription_cancellation_reason sr on sr.subscription_id=a.id;