drop table if exists ods_operations.intercom_first_conversation_snapshot;
create table ods_operations.intercom_first_conversation_snapshot
as
with

intercom_contacts_dedup as (
select 
(timestamptz 'epoch' + updated_at ::int * interval '1 second') as updated_at
,row_number()over(partition by id order by updated_at desc) as rr
,* from staging.intercom_contacts),

intercom_contacts as (
select external_id,id from intercom_contacts_dedup
where rr=1
),

cte_conversations as
	(
	select
		contact_.id,
		created_at,
		updated_at,
		waiting_since,
		snoozed_until,
		intercom_customer_id,
		delivered_as,
		source_id,
		redacted,
		author_type,
		author_id,
		fc_reply_at,
		fc_reply_type,
		fc_url,
		"open",
		state,
		read,
		priority, market,
		team_participated,
		teams_count,
		case when admins_list = '[]' then false else true end as has_admin_replies,
		admins_list,
		admin_assignee_id,
		admin_assignee_name,
		team_assignee_id,
		team_assignee_name,
		time_to_assignment,
		time_to_admin_reply,
		time_to_first_close,
		time_to_last_close,
		median_time_to_reply,
		first_assignment_at,
		first_admin_reply_at,
		first_close_at,
		last_assignment_at,
		last_assignment_admin_reply_at,
		last_contact_reply_at,
		last_admin_reply_at,
		last_close_at,
		last_closed_by_id,
		count_reopens,
		count_assignments,
		count_conversation_parts,
		total_count,
		conversation_rating,
		conversation_rating_created_at,
		contact_reason,
		Duplicate,
		B2B_contact_reason,
		card_contact_reason,
		resolution_path,
		customer_type,
		contact_item_1,
		contact_item_2,
		dc_resolution_path,
		"language"	
		from ods_data_sensitive.intercom_first_conversation contact_
	where  date_trunc('month',created_at::date)>='2022-01-01'
		or  date_trunc('month',updated_at::date)>='2022-01-01'
	),
	cte_tags as
	(
	select
		conversation_id ,
		listagg(tag_name,', ') list_tags
	from stg_external_apis.intercom_tag_info iti
	group by 1
	)
	select
	convs.*,
	list_tags tags,
	c.external_id as customer_id
	from cte_conversations convs
	left join cte_tags tags
	on convs.id = tags.conversation_id
	left join intercom_contacts c 
	on convs.intercom_customer_id = c.id;

drop table if exists ods_operations.intercom_conversation_parts_snapshot;
create table ods_operations.intercom_conversation_parts_snapshot
as
select
parts_.conversation_id,
conversation_created_at,
conversation_updated_at,
state,
conversation_part_created_at,
conversation_part_updated_at,
conversation_part_notified_at,
conversation_part_id,
conversation_part_type,
conversation_part_author_id,
conversation_part_author_type,
conversation_part_external_id,
total_count,
conversation_part_assigned_to_id,
conversation_part_assigned_to_type,
is_team_cs
from ods_data_sensitive.intercom_conversation_parts parts_
where  date_trunc('month',conversation_created_at::date)>='2022-01-01'
		or  date_trunc('month',conversation_updated_at::date)>='2022-01-01'


;
GRANT SELECT ON ods_operations.intercom_first_conversation_snapshot TO group customer_support;
GRANT SELECT ON ods_operations.intercom_first_conversation_snapshot TO tableau;
