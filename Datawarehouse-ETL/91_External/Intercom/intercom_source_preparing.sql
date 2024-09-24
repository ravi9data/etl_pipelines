-- normalisation here to bypass the airbyte normalisation 


drop table if exists staging.intercom_conversations;
create table staging.intercom_conversations as
with intercom_dedup as (
select *,row_number () over(partition by replace(JSON_SERIALIZE("_airbyte_data".id)::varchar,'"','') order by JSON_SERIALIZE("_airbyte_data"."updated_at")::bigint desc) as rr
from staging._airbyte_raw_intercom_conversations
)
select distinct
	replace(JSON_SERIALIZE("_airbyte_data".id)::varchar,'"','')  as id,
	case when 
	replace(JSON_SERIALIZE("_airbyte_data"."open")::varchar,'"','')='false' then false else true end as "open",
	case when replace(JSON_SERIALIZE("_airbyte_data"."read")::varchar,'"','')='false' then false else true end as "read",
	"_airbyte_data"."tags",
	replace(JSON_SERIALIZE("_airbyte_data"."type")::varchar,'"','') as type,
	"_airbyte_data"."user",
	replace(JSON_SERIALIZE("_airbyte_data"."state")::varchar,'"','') as state,
	case when replace(JSON_SERIALIZE("_airbyte_data"."title")::varchar,'"','')='null' then null else replace(JSON_SERIALIZE("_airbyte_data"."title")::varchar,'"','') end as title,
	"_airbyte_data"."source",
	null::int as sent_at,
	"_airbyte_data"."assignee",
	"_airbyte_data"."contacts",
	replace(JSON_SERIALIZE("_airbyte_data"."priority")::varchar,'"','') as priority,
	null::boolean as redacted,
	"_airbyte_data"."customers",
	"_airbyte_data"."teammates",
	JSON_SERIALIZE("_airbyte_data"."created_at")::bigint as created_at,
	"_airbyte_data"."statistics",
	JSON_SERIALIZE("_airbyte_data"."updated_at")::bigint as updated_at,
	"_airbyte_data"."sla_applied",	
	case when JSON_SERIALIZE("_airbyte_data"."snoozed_until")::varchar ='null' then null::bigint else
	JSON_SERIALIZE("_airbyte_data"."snoozed_until")::bigint end as snoozed_until,
	case when JSON_SERIALIZE("_airbyte_data"."waiting_since")::varchar ='null' then null::bigint else
	JSON_SERIALIZE("_airbyte_data"."waiting_since")::bigint end as waiting_since,
	case when JSON_SERIALIZE("_airbyte_data"."team_assignee_id")::varchar ='null' then null::bigint else
	JSON_SERIALIZE("_airbyte_data"."team_assignee_id")::bigint end as team_assignee_id,
	case when JSON_SERIALIZE("_airbyte_data"."admin_assignee_id")::varchar ='null' then null::bigint else
	JSON_SERIALIZE("_airbyte_data"."admin_assignee_id")::bigint end as admin_assignee_id,
	"_airbyte_data"."custom_attributes",		
	"_airbyte_data"."conversation_rating",		
	"_airbyte_data"."first_contact_reply",		
	"_airbyte_data"."conversation_message",		
	"_airbyte_data"."customer_first_reply",		
	case when JSON_SERIALIZE("_airbyte_data"."_airbyte_ab_id")::varchar='null' then null
	else JSON_SERIALIZE("_airbyte_data"."_airbyte_ab_id")::varchar end as "_airbyte_ab_id",		
  null::timestamp as "_airbyte_emitted_at",		
  null::timestamp as _airbyte_normalized_at	,		
  case when	JSON_SERIALIZE("_airbyte_data"."_airbyte_intercom_conversations_hashid")::varchar ='null' then null
  else  JSON_SERIALIZE("_airbyte_data"."_airbyte_intercom_conversations_hashid")::varchar end as "_airbyte_intercom_conversations_hashid"	
  from intercom_dedup
  where rr=1;


-- delete newly updated data from source for conversations
delete from stg_external_apis.intercom_conversations 
using staging.intercom_conversations b 
where intercom_conversations.id = b.id ;

-- capturing latest and updated records from staging table to source table for conversations

insert into stg_external_apis.intercom_conversations
select id::VARCHAR(100),
created_at::INTEGER,
updated_at::INTEGER,
JSON_SERIALIZE("source".attachments)::varchar as attachements,
JSON_SERIALIZE("source".author)::varchar as author,
"source".body::VARCHAR(65535) as body,
"source".delivered_as::VARCHAR(100) as delivered_as,
"source".id::VARCHAR(100) as source_id,
"source".subject::VARCHAR(65535) as subject,
"source".url::VARCHAR(65535) as url,
case when JSON_SERIALIZE("source".redacted)::varchar='null' then null else JSON_SERIALIZE("source".redacted)::varchar end    as redacted,
case when JSON_SERIALIZE(teammates)::varchar='null' then null else JSON_SERIALIZE(teammates)::varchar end   contacts,
case when JSON_SERIALIZE(teammates)::varchar='null' then null else JSON_SERIALIZE(teammates)::varchar end  as teammates,
title,
admin_assignee_id::VARCHAR(100),
team_assignee_id::VARCHAR(100),
case when JSON_SERIALIZE(custom_attributes)::varchar='null' then null else JSON_SERIALIZE(custom_attributes)::varchar end as custom_attributes,
(case when "open" is true then 'T' else 'F' end)::varchar as "open" ,
state::VARCHAR(20),
(case when "read" is true then 'T' else 'F' end)::varchar as "read",
waiting_since,
snoozed_until,
case when JSON_SERIALIZE(tags)::varchar='null' then null else JSON_SERIALIZE(tags)::varchar end as tags,
case when JSON_SERIALIZE(first_contact_reply)::varchar='null' then null else JSON_SERIALIZE(first_contact_reply)::varchar end first_contact_reply,
priority::VARCHAR(50),
case when JSON_SERIALIZE(sla_applied)::varchar='null' then null else JSON_SERIALIZE(sla_applied)::varchar end  as sla_applied,
case when JSON_SERIALIZE(conversation_rating)::varchar='null' then null else JSON_SERIALIZE(conversation_rating)::varchar end as conversation_rating,
case when JSON_SERIALIZE("statistics")::varchar='null' then null else JSON_SERIALIZE("statistics")::varchar end as "statistics",
null as conversation_parts,
null as total_count
from staging.intercom_conversations; 



-- nromalising here to bypass the airbyte normalisation step
insert into staging.intercom_conversation_parts
select 
replace(JSON_SERIALIZE("_airbyte_data".id)::varchar,'"','')  as id,
null as body,
replace(JSON_SERIALIZE("_airbyte_data".type)::VARCHAR,'"','') as type,
JSON_SERIALIZE("_airbyte_data".author)::VARCHAR as type,
case when JSON_SERIALIZE("_airbyte_data".redacted)::varchar='null' then null 
when JSON_SERIALIZE("_airbyte_data".redacted)::varchar ='true' then true  
when JSON_SERIALIZE("_airbyte_data".redacted)::varchar ='false' then false end    as redacted,
replace(JSON_SERIALIZE("_airbyte_data".part_type)::VARCHAR,'"','') as part_type,
JSON_SERIALIZE("_airbyte_data".created_at)::bigint  as created_at,
JSON_SERIALIZE("_airbyte_data".updated_at)::bigint as updated_at,
JSON_SERIALIZE("_airbyte_data".assigned_to)::VARCHAR as assigned_to,
JSON_SERIALIZE("_airbyte_data".attachments)::VARCHAR as attachments,
JSON_SERIALIZE("_airbyte_data".external_id)::VARCHAR as external_id,
JSON_SERIALIZE("_airbyte_data".notified_at)::bigint as notified_at,
replace(JSON_SERIALIZE("_airbyte_data".conversation_id)::VARCHAR,'"','') as conversation_id,
null as conversation_created_at,
null as conversation_updated_at,
null as conversation_total_parts,
_airbyte_ab_id,
_airbyte_emitted_at
_airtbyte_normalized_at,
null as airbyte_intecom_conversation_parts_hashid
from staging._airbyte_raw_conversation_parts 
where "_airbyte_emitted_at" > (select max("_airbyte_emitted_at") from staging.intercom_conversation_parts) ;


-- delete newly updated data from source for conversation parts

delete from stg_external_apis.intercom_conversation_parts  
using staging.intercom_conversation_parts b 
where intercom_conversation_parts.conversation_id = b.conversation_id 
and intercom_conversation_parts.conversation_part_id=b.id ;

-- capturing latest and updated records from staging table to source table for conversation parts

insert into stg_external_apis.intercom_conversation_parts
with dedup as (
select row_number() over(partition by conversation_id ,id order by "_airbyte_emitted_at" desc ) as rr
,* from staging.intercom_conversation_parts
)
select md5(conversation_id || id) as row_id,
conversation_id,
ROW_NUMBER() OVER (PARTITION BY conversation_id) AS conversation_part_number,
id as conversation_part_id,
part_type,
body,
created_at,
updated_at,
notified_at,
case when JSON_SERIALIZE(assigned_to)::varchar ='null' then null else JSON_SERIALIZE(assigned_to)::varchar end as assigned_to,
case when JSON_SERIALIZE(author)::varchar ='null' then null else JSON_SERIALIZE(author)::varchar end as author,
case when JSON_SERIALIZE(attachments)::varchar ='null' then null else JSON_SERIALIZE(attachments)::varchar end as attachments,
external_id,
redacted
from  dedup
where  rr=1 ;

update stg_external_apis.intercom_conversation_parts
set assigned_to = null
where assigned_to ='"null"';

update stg_external_apis.intercom_conversation_parts
set assigned_to = replace(replace(REGEXP_REPLACE(assigned_to, '[^0-9A-Za-z:,"{}]', ''),'"{','{'),'}"','}')
where  is_valid_json(assigned_to) =false and assigned_to like '%\%' and assigned_to <> '"null"';

update stg_external_apis.intercom_conversation_parts
set author = null
where author ='"null"';

update stg_external_apis.intercom_conversation_parts
set author = replace(replace(REGEXP_REPLACE(author, '[^0-9A-Za-z:,"{}]', ''),'"{','{'),'}"','}')
where  is_valid_json(author) =false and author like '%\%' and author <> '"null"';
