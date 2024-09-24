
drop table if exists missing_parts;
create temp table missing_parts as
select icp.conversation_id  , icp.conversation_part_id as id 
from stg_external_apis.intercom_conversation_parts icp 
left join  ods_data_sensitive.intercom_conversation_parts b
on  icp.conversation_part_id  = b.conversation_part_id
left join staging.intercom_deleted_conversations del on icp.conversation_id = del.id 
where b.conversation_part_id is null and del.id is null;



-- capturing the newly created parts for the older conversation

drop table if exists hist_parts_pre;
create temp table hist_parts_pre as 
select icp.conversation_id  , icp.conversation_part_id as id 
from stg_external_apis.intercom_conversation_parts icp 
inner join  ods_data_sensitive.intercom_conversation_parts b
on  icp.conversation_id  = b.conversation_id  
where b.conversation_part_id is null;

--union of missing parts of already exist conversations;

drop table if exists hist_parts;
create temp table hist_parts as
select * from missing_parts
union 
select * from hist_parts_pre;


-- extracting the conversations data for the parts capturing in first step

drop table if exists conv_id;
create temp table conv_id as
select a.* from stg_external_apis.intercom_conversations a 
inner join hist_parts mp 
on a.id = mp.conversation_id;

-- extracting the relevant features from conversations which requires in conversation part data

drop table if exists hist_conv_with_new_parts ;
create temp table  hist_conv_with_new_parts as
select distinct src.id, src.created_at, src.updated_at, src.state, src.total_count,trg.id as conversation_part_id
from conv_id  src
inner join hist_parts trg
on trg.conversation_id = src.id
left join ods_data_sensitive.intercom_conversation_parts icp on trg.id = icp.conversation_part_id 
where icp.conversation_part_id is null;



-- capturing the latest conversations and it's parts
drop table if exists conv_id_to_be_refreshed ;
create temp table  conv_id_to_be_refreshed as
with cte_to_be_refreshed 
as
(
	select distinct src.id, src.created_at, src.updated_at, src.state, src.total_count
	from stg_external_apis.intercom_conversations src
	left join ods_data_sensitive.intercom_conversation_parts trg
	on trg.conversation_id = src.id 
	where 
	trg.conversation_id is null
	or trg.conversation_updated_at != 	(timestamptz 'epoch' + src.updated_at::int * interval '1 second')
)
,excluding_deleted as 
(
	select ref.* 
	from cte_to_be_refreshed ref
	left join staging.intercom_deleted_conversations del
	on ref.id = del.id
	where del.id is null
)
select * from excluding_deleted ;


delete from ods_data_sensitive.intercom_conversation_parts
using conv_id_to_be_refreshed
where ods_data_sensitive.intercom_conversation_parts.conversation_id=conv_id_to_be_refreshed.id;

delete from ods_data_sensitive.intercom_conversation_parts
using staging.intercom_deleted_conversations del
where ods_data_sensitive.intercom_conversation_parts.conversation_id=del.id;


delete from hist_conv_with_new_parts 
using conv_id_to_be_refreshed  
where hist_conv_with_new_parts.id = conv_id_to_be_refreshed.id;


insert into ods_data_sensitive.intercom_conversation_parts 	
WITH admins AS (
SELECT distinct admin_id,"team assigned id" FROM 
stg_external_apis.teams_for_assignment_intercom t
LEFT JOIN ods_operations.intercom_admin ia
	ON ia.teams_ids  ILIKE ('%' || CASE WHEN t."team participated" ='Yes' THEN t."team assigned id" END || '%')
WHERE admin_id IS NOT NULL ),
  a as (select 
		con.id as conversation_id,
		(timestamptz 'epoch' + con.created_at::int * interval '1 second') as conversation_created_at,
        (timestamptz 'epoch' + con.updated_at::int * interval '1 second') as conversation_updated_at,
		con.state,
		(timestamptz 'epoch' + icp.created_at  * interval '1 second') as conversation_part_created_at,
		(timestamptz 'epoch' + icp.updated_at  * interval '1 second') as conversation_part_updated_at,
		(timestamptz 'epoch' + icp.notified_at * interval '1 second') as conversation_part_notified_at,
		icp.conversation_part_id,
		icp.part_type as conversation_part_type,
		icp."body" as conversation_part_body,
		icp.assigned_to as conversation_part_assigned_to,
		JSON_EXTRACT_PATH_text(assigned_to,'type') as conversation_part_assigned_to_type,
		case when conversation_part_type ='close' then null else 
		JSON_EXTRACT_PATH_text(assigned_to,'id') end as conversation_part_assigned_to_id_current,
		Case when conversation_part_type in ('assignment','assign_and_unsnooze','away_mode_assignment','message_assignment','assignment','comment','assign_and_reopen','workflow_assignment','default_assignment') 
		then JSON_EXTRACT_PATH_text(assigned_to,'id') end as conversation_part_assigned_to_id_assign,
		
		Case when conversation_part_type in( 'open','assign_and_unsnooze', 'unsnoozed','timer_unsnooze','note_and_reopen','assign_and_reopen','close') then 
	    lag(conversation_part_assigned_to_id_assign) ignore nulls over (Partition by conversation_id order by conversation_part_created_at) else null end as conversation_part_assigned_to_id_lag,
	    
		JSON_EXTRACT_PATH_text(author ,'id') as conversation_part_author_id,
		JSON_EXTRACT_PATH_text(author ,'type') as conversation_part_author_type,
		JSON_EXTRACT_PATH_text(author ,'name') as conversation_part_author_name,
		JSON_EXTRACT_PATH_text(author ,'email') as conversation_part_author_email,
		attachments as conversation_part_attachments,
		external_id as conversation_part_external_id,
		con.total_count	
		from conv_id_to_be_refreshed con
		inner join stg_external_apis.intercom_conversation_parts icp
		on con.id = icp.conversation_id
		left join staging.intercom_deleted_conversations  del on con.id = del.id
		where con.id is not null and del.id is null
		and is_valid_json(assigned_to) or is_valid_json(assigned_to) is null
		
		union 
		
  	select 
		con.id as conversation_id,
		(timestamptz 'epoch' + con.created_at::int * interval '1 second') as conversation_created_at,
        (timestamptz 'epoch' + con.updated_at::int * interval '1 second') as conversation_updated_at,
		con.state,
		(timestamptz 'epoch' + icp.created_at  * interval '1 second') as conversation_part_created_at,
		(timestamptz 'epoch' + icp.updated_at  * interval '1 second') as conversation_part_updated_at,
		(timestamptz 'epoch' + icp.notified_at * interval '1 second') as conversation_part_notified_at,
		icp.conversation_part_id,
		icp.part_type as conversation_part_type,
		icp."body" as conversation_part_body,
		icp.assigned_to as conversation_part_assigned_to,
		JSON_EXTRACT_PATH_text(assigned_to,'type') as conversation_part_assigned_to_type,
		case when conversation_part_type ='close' then null else 
		JSON_EXTRACT_PATH_text(assigned_to,'id') end as conversation_part_assigned_to_id_current,
		Case when conversation_part_type in ('assignment','assign_and_unsnooze','away_mode_assignment','message_assignment','assignment','comment','assign_and_reopen','workflow_assignment','default_assignment') 
		then JSON_EXTRACT_PATH_text(assigned_to,'id') end as conversation_part_assigned_to_id_assign,
		Case when conversation_part_type in( 'open','assign_and_unsnooze', 'unsnoozed','timer_unsnooze','note_and_reopen','assign_and_reopen','close') then 
	    lag(conversation_part_assigned_to_id_assign) ignore nulls over (Partition by conversation_id order by conversation_part_created_at) else null end as conversation_part_assigned_to_id_lag,  
		JSON_EXTRACT_PATH_text(author ,'id') as conversation_part_author_id,
		JSON_EXTRACT_PATH_text(author ,'type') as conversation_part_author_type,
		JSON_EXTRACT_PATH_text(author ,'name') as conversation_part_author_name,
		JSON_EXTRACT_PATH_text(author ,'email') as conversation_part_author_email,
		attachments as conversation_part_attachments,
		external_id as conversation_part_external_id,
		con.total_count	
		from hist_conv_with_new_parts con
		inner join stg_external_apis.intercom_conversation_parts icp
		on con.id = icp.conversation_id
		and con.conversation_part_id = icp.conversation_part_id
		left join staging.intercom_deleted_conversations  del on con.id = del.id
		where con.id is not null and del.id is null  
		and is_valid_json(assigned_to) or is_valid_json(assigned_to) is null
		)	
	select 
	conversation_id,
	conversation_created_at,
	conversation_updated_at,
	state,
	conversation_part_created_at,
	conversation_part_updated_at,
	conversation_part_notified_at,
	conversation_part_id,
	conversation_part_type,
	conversation_part_body,
	conversation_part_assigned_to,
	conversation_part_assigned_to_type,
	COALESCE (a.conversation_part_assigned_to_id_current,conversation_part_assigned_to_id_lag) as conversation_part_assigned_to_id,
	conversation_part_author_id,
	conversation_part_author_type,
	conversation_part_author_name,
	conversation_part_author_email,
	conversation_part_attachments,
	conversation_part_external_id,
	total_count,
CASE
		WHEN 
		 conversation_part_assigned_to_id = t."team assigned id" then true  
		WHEN conversation_part_assigned_to_id=admin_id THEN TRUE  
		ELSE FALSE 
		END  as is_team_cs
    from a 
	left join stg_external_apis.teams_for_assignment_intercom t
	on COALESCE (a.conversation_part_assigned_to_id_current,conversation_part_assigned_to_id_lag)=t."team assigned id" 
	LEFT JOIN admins a1
		ON COALESCE (a.conversation_part_assigned_to_id_current,conversation_part_assigned_to_id_lag)= a1.admin_id 
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
	order by conversation_part_created_at;
