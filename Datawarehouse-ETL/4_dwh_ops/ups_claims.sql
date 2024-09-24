drop table if exists dm_operations.ups_claims;
create table dm_operations.ups_claims as

---US---
with us_ups_claims as(
select to_date(uc."inquiry date", 'MM/DD/YYYY') as inquiry_date,
null as claim_number,
uc."claim status" as claim_status,
replace(trim(replace(uc."claim amount", '$', '')), ',','')::float as claim_amount,
null as claim_status_detail,
uc."claim type" as claim_type,
uc."tracking #" as tracking_number,
null as ups_account_number,
uc."delivery scan date"::date as delivery_scan_date,
replace(uc."paid amount",'.','')/100 as paid_amount,
uc."paid date"::timestamp as paid_date,
null::date as credit_date,
null as credit_invoice_number,
null as credited_by,
null::date as transaction_entry_date,
null::text as fee,
null::float as saved_amount,
uc."role of initiator" as role_of_initiator,
'US' as region
from staging.claims uc),

---EU---
transactions as(
select account,
position('REMI' in description) as position_,
substring(description , position_+5, length(description)-position_ ) as sub_description,
split_part(sub_description,'/', 1) as claim_number,
replace(replace(replace(" amount ", ')',''), '(', ''),',','')::float as amount,
to_date("entry date",'dd/mm/yyyy') as entry_date
from staging_airbyte_bi.ups_bank_transactions_eu ubte
),

eu_ups_claims as(
select
to_date(ucde."enquiry date", 'MM/DD/YYYY') as inquiry_date,
ucde."claim no."  as claim_number,
ucde."claim status" as claim_status,
replace(li.due_amount,'.','')/100 as claim_amount,
ucde."status detail" as claim_status_detail,
li.reason_refund as claim_type,
ucde."tracking no." as tracking_number,
ucde."ups account number" as ups_account_number,
null::date as delivery_scan_date,
t.amount::float as paid_amount,
li.invoice_date::date as paid_date,
li.credit_date::date as credit_date,
li.credit_invoice_number,
li.credited_by,
t.entry_date::date as transaction_entry_date,
li.fee,
replace(li.saved_amount,'.','')/100 as saved_amount,
null::text as role_of_initiator,
'EU' as region
from staging_airbyte_bi.ups_claim_data_eu ucde 
inner join staging_airbyte_bi.lox_invoice li on ucde."tracking no." = li.tracking_number
inner join transactions t on t.claim_number = replace(li.claim_number,'A','')),

union_of_claims as(
select * from eu_ups_claims
union
select * from us_ups_claims),

crm_data as(
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
where a.allocated_at::date>='2022-01-01'
)

select 
inquiry_date,
claim_number,
claim_status,
claim_amount,
claim_status_detail,
claim_type,
tracking_number,
ups_account_number,
delivery_scan_date,
paid_amount,
paid_date,
credit_date,
credit_invoice_number,
credited_by,
transaction_entry_date,
fee,
saved_amount,
c.role_of_initiator,
region,
subscription_id,
subscription_status,
allocation_id,
allocation_status,
shipment_tracking_number,
asset_id,
asset_status_original,
order_id,
order_status,
serial_number,
row_number() over(partition by tracking_number order by cd.allocation_id) rn --It is used in Tableau to avoid duplicates
from union_of_claims c
left join crm_data cd on c.tracking_number = cd.shipment_tracking_number;