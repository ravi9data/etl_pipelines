drop table if exists ods_production.supplier;
create table ods_production.supplier as
with a as(
select 
s.id as supplier_id,
s.ownerid as owner_id,
s.name as default_supplier_name,
case 
	when lower(s.name) like '%media%' and lower(s.name) like '%markt%' then 'Media Markt'
	when lower(s.name) like '%saturn%' then 'Saturn'
    when lower(s.name) like '%thalia%' then 'Thalia'
    when lower(s.name) like  ('%qbo%') or lower(s.name) like  ('%tchibo%') then 'Tchibo'
	when lower(s.name) like '%conrad%' then 'Conrad'
	when lower(s.name) like '%gravis%' then 'Gravis'
	when lower (s.name) like '%quelle%' then 'Quelle'
	when lower (s.name) like '%weltbild%' then 'Weltbild'
  when lower (s.name) like '%aldi%' then 'Aldi Talk'
  when lower(s.name) like '%comspot%' then 'Comspot'
  when lower(s.name) like '%irobot%' then 'iRobot'
  when lower(s.name) like '%shifter%' then 'Shifter'
  when lower(s.name) like '%samsung%' then 'Samsung'
  else s.name
end as supplier_name,
s.createddate as supplier_created_date,
s.createdbyid as supplier_created_id, 
s.lastmodifieddate as last_modified_date,
s.lastmodifiedbyid as last_modified_id,
s.default_purchase_payment_method__c as purchase_payment_method,
s.dropship_ability__c as dropship_ability,
s.default_capital_source__c as default_capital_source,
cs.name as capital_source_name,
cs.type__c as capital_source_type,
s.default_warehouse__c as default_warehouse,
s.delivery_time__c as delivery_time,
s.delivery_cost__c as delivery_cost,
s.active_rules_count__c as rules_count,
case 
 when lower(s.name) like ('%janado%')
 or lower(s.name) like ('%rebuy%')
 or lower(s.name) like ('%revived%')
 or lower(s.name) like ('%webinstore%')
 or lower(s.name) like ('%retail%')
 then 'true' else 'false'
end as agan_asset_supplier,
 s.locale__c
from stg_salesforce.supplier__c s
left join stg_salesforce.capital_source__c cs on s.default_capital_source__c = cs.id)
select 
a.supplier_id,
a.owner_id,
a.default_supplier_name,
a.supplier_name,
  case
    when coalesce(a.supplier_name, 'N/A') 
     in ('Media Markt','Saturn','Thalia','Tchibo','Conrad','Gravis','Quelle','Weltbild','Aldi Talk','Comspot','Samsung','iRobot','Shifter') then a.supplier_name
    else 'Others'
  end as supplier_account,
  CASE
   WHEN  coalesce(a.supplier_name, 'N/A')  
     in  ('Media Markt','Saturn','Conrad','Gravis','Quelle','Weltbild','Aldi Talk','Comspot','Samsung','iRobot','Shifter') THEN a.supplier_name
    ELSE 'Others'::character varying
   END AS request_partner,
a.supplier_created_date,
a.supplier_created_id, 
a.last_modified_date,
a.last_modified_id,
a.purchase_payment_method,
a.dropship_ability,
a.default_capital_source,
a.capital_source_name,
a.capital_source_type,
a.default_warehouse,
a.delivery_time,
a.delivery_cost,
a.rules_count,
a.agan_asset_supplier,
locale__c
from a 
;