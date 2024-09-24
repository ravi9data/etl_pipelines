drop table if exists ods_production.waiting_list;
create table ods_production.waiting_list as 
with a as (
select distinct 
wle.id as waiting_list_id,
u.id as customer_id,
wle.created_at as created_at,
wle.updated_at as updated_at,
wle.deleted_at as deleted_at,
wle.ready_email_sent_at as email_sent, 
wle.variant_id,
s.id as store_id,
rank() over (partition by wle.email,date_trunc('week',wle.created_at),wle.variant_id,s.id order by wle.created_at) as duplicate_cleanup
from stg_api_production.waiting_list_entries as wle
left  join stg_api_production.spree_stores as s on s.code = replace(wle.store, 'germany', 'de')
left join stg_api_production.spree_users u on u.email=wle.email
order by wle.created_at desc )
select 
waiting_list_id, 
customer_id, 
created_at,
email_sent,
variant_id,
store_id,
updated_at,
deleted_at
from a 
where duplicate_cleanup = 1;

