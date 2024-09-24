drop table if exists dm_operations.ups_claim_data;
create table dm_operations.ups_claim_data as
with crm_data as(
 select 
 a.subscription_id 
,s.status as subscription_status
,a.allocation_id
,a.allocation_status_original as allocation_status
,coalesce(a.shipment_tracking_number, a.return_shipment_tracking_number) as shipment_tracking_number
,a2.asset_id 
,a2.asset_status_original 
,a.order_id
,o.status as order_status
,a.serial_number
from ods_production.order o 
left join ods_production.subscription s on s.order_id = o.order_id
left join ods_production.allocation a on a.subscription_id = s.subscription_id
left join ods_production.asset a2 on a.asset_id  = a2.asset_id
where s.country_name = 'United States'
)
select 
trim(replace(uc."claim amount", '$', '')) as claim_amount,
uc."claim status" as claim_status,
uc."claim type" as claim_type,
uc."delivery scan date"::timestamp as delivery_scan_date,
uc."inquiry date" as inquiry_date,
uc."paid amount" as paid_amount,
uc."paid date"::timestamp as paid_date,
uc."role of initiator" as role_of_initiator,
uc."tracking #" as tracking_number
,cd.*, 
row_number() over(partition by uc."tracking #" order by cd.allocation_id) rn --It is used in Tableau to avoid duplicates
from staging.claims uc
left join crm_data cd on uc."tracking #" = cd.shipment_tracking_number;

grant select on dm_operations.ups_claim_data to tableau;
grant select on table dm_operations.ups_claim_data to GROUP bi;
