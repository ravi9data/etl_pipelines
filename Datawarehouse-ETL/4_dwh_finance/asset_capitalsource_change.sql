DROP TABLE IF EXISTS dwh.asset_capitalsource_change;
  create table dwh.asset_capitalsource_change as 
select 
ah.assetid as asset_id,
ah.createddate as created_at,
ah.field,
cs.name as old_value,
cs2.name as new_value
from stg_salesforce.asset_history ah
inner join  stg_salesforce.capital_source__c  cs on cs.id=ah.oldvalue
inner join  stg_salesforce.capital_source__c  cs2 on cs2.id=ah.newvalue
