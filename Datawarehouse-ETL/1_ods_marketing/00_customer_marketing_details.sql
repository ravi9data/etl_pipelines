drop table if exists ods_production.customer_marketing_details;
create table ods_production.customer_marketing_details as 
select 
	external_user_id AS customer_id
	,max(timestamptz 'epoch' + time::int * interval '1 second') as event_time 
	,TRUE as is_unsubscribed
from stg_external_apis.braze_unsubscribe_event
group by 1;

GRANT SELECT ON ods_production.customer_marketing_details TO tableau;
