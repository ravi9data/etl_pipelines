drop table if exists ods_operations.conversation_rating_intercom;
create table ods_operations.conversation_rating_intercom as
with last_reply_admin as (
select distinct conversation_id, 
	conversation_part_created_at, 
	conversation_part_author_id, 
	conversation_part_author_name, 
	case when ait.team_participated ='' then NULL else ait.team_participated end as team_participated,
	row_number() over (partition by conversation_id order by conversation_part_created_at desc) as idx
from ods_data_sensitive.intercom_conversation_parts icp 
inner join ods_data_sensitive.intercom_first_conversation ifc 
	on icp.conversation_id = ifc.id
left join ods_data_sensitive.v_intercom_admin_id_teams ait
	on ait.admin_id = icp.conversation_part_author_id 
where  conversation_part_created_at <= ifc.conversation_rating_created_at 
and (conversation_part_type ='comment' and conversation_part_author_type = 'admin'
or (conversation_part_type ='assignment' and conversation_part_author_type = 'admin')
or (conversation_part_type ='close' and conversation_part_author_type = 'admin')
or (conversation_part_type ='open' and conversation_part_author_type = 'admin')
))
--uses the source above to check the conversations that were rated for the last time
select 
	icp.conversation_part_created_at::timestamp as rating_timestamp_,
	convert_timezone ('Europe/Berlin',icp.conversation_part_created_at::timestamp) AS rating_timestamp_berlin,
	rating_timestamp_berlin::date as rating_day, --based on berlin timestamp
	DATE_TRUNC('week', rating_timestamp_berlin::timestamp)::date as rating_week, --based on berlin timestamp
	ifc.id as conversation_id,  
	ifc.market,
	lra.team_participated,
	ifc.conversation_rating,
	ifc.card_contact_reason ,
	ifc.b2b_contact_reason,
	ifc.time_to_admin_reply,
	ifc.time_to_last_close,
	ifc.contact_reason,
	ifc.resolution_path,
	ifc.customer_type,
	ifc.contact_item_1,
	ifc.contact_item_2,
	case when ifc.tags like '%B2B%' or ifc.b2b_contact_reason<>'' then 'B2B' else 'B2C' end as component,
	ifc."language",	
	lra.conversation_part_author_id,
	row_number() over (partition by ifc.id order by rating_timestamp_ desc) as desc_idx
from ods_data_sensitive.intercom_first_conversation ifc
left join ods_data_sensitive.intercom_conversation_parts icp
		on icp.conversation_id = ifc.id
		and conversation_part_type in ('conversation_rating_changed', 'conversation_rating_remark_added')
left join last_reply_admin lra
	on lra.conversation_id = ifc.id 
	and idx = 1
where  lra.team_participated is not null
and conversation_rating_created_at is not null
and conversation_rating is not null
and conversation_part_type is not null ;
grant select on ods_operations.conversation_rating_intercom to group customer_support;
