drop table if exists ods_production.order_subscription_user_payment_methods;
create table ods_production.order_subscription_user_payment_methods as
with a as (
select 
 *,
 json_extract_path_text(meta,'migrated_at')::timestamp as migrated_at
 from stg_api_production.user_payment_methods
 where meta like ('%migrated_at%'))
 select distinct 
  o.spree_order_number__c as order_id,
  o.id as sf_order_id,
  s.id as subscription_id,
  coalesce(upm2.created_at,upm.created_at) payment_method_auth_at
  ,coalesce(a2.migrated_at,a.migrated_at) as migrated_At
  from stg_salesforce."order" o 
  left join stg_salesforce.subscription__c s
   on s.spree_order_number__c=o.spree_order_number__c
  left join stg_api_production.user_payment_methods upm
   on o.spree_customer_id__c=upm.user_id
   and o.payment_method_id_1__c=upm.merchant_transaction_id
   and o.payment_method_id_2__c=upm.reference_id
 left join stg_api_production.user_payment_methods upm2
   on s.spree_order_number__c=o.spree_order_number__c
   and s.payment_method_id_1__c=upm2.merchant_transaction_id
   and s.payment_method_id_2__c=upm2.reference_id
  left join a 
   on a.id=upm.id
  left join a a2 
   on a2.id=upm2.id
  where coalesce(upm.created_at,upm2.created_at) is not null;

GRANT SELECT ON ods_production.order_subscription_user_payment_methods TO tableau;
