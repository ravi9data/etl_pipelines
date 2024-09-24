	drop table if exists ods_data_sensitive.intercom;
	create table ods_data_sensitive.intercom as 
 with json_time_extracts as (
     select id as conversation_id,
	     case when "source" is not null then JSON_EXTRACT_PATH_text("first_contact_reply" ,'created_at') else NULL end as fc_reply_created_at,
	      case when statistics is not null then 
		     case when JSON_EXTRACT_PATH_text("statistics" ,'first_assignment_at') = '' then NULL 
			      else JSON_EXTRACT_PATH_text("statistics" ,'first_assignment_at') end 
			 else NULL end as stat_first_assignment_at,
	     case when statistics is not null then 
		     case when JSON_EXTRACT_PATH_text("statistics" ,'first_admin_reply_at') = '' then NULL 
			      else JSON_EXTRACT_PATH_text("statistics" ,'first_admin_reply_at') end 
			 else NULL end as stat_first_admin_reply_at,
	     case when conversation_rating is not null then JSON_EXTRACT_PATH_text("conversation_rating" ,'created_at') else NULL end as rating_created_at,
	     count(*) over (PARTITION by id ) as total
     from stg_external_apis.intercom_conversations_list)
	select 
		id as conversation_id,
		"type",
		date_trunc('week',(timestamptz 'epoch' + created_at::int * interval '1 second')) as created_at ,
		date_trunc('week',(timestamptz 'epoch' + updated_at ::int * interval '1 second')) as updated_at ,
		date_trunc('week',(timestamptz 'epoch' + waiting_since ::int * interval '1 second')) as waiting_since ,
		date_trunc('week',(timestamptz 'epoch' + snoozed_until ::int * interval '1 second')) as snoozed_until,
		date_trunc('week',(timestamptz 'epoch' + fc_reply_created_at ::int * interval '1 second')) as fc_reply_created_at,
		case when "source" is not null then JSON_EXTRACT_PATH_text("source" ,'type' ) else NULL end as source_type, 
		case when "source" is not null then JSON_EXTRACT_PATH_text("source" ,'delivered_as' ) else NULL end as delivered_as, 
		case when "source" is not null then JSON_EXTRACT_PATH_text("source" ,'author','name' ) else NULL end as customer_name, 
		case when "source" is not null then JSON_EXTRACT_PATH_text("source" ,'author','email' ) else NULL end as customer_email, 
		case when "assignee" is not null then JSON_EXTRACT_PATH_text("assignee" ,'type') else NULL end as assignee_type,
		case when "assignee" is not null then JSON_EXTRACT_PATH_text("assignee" ,'id') else NULL end as assignee_id,
		case when "source" is not null then JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text(contacts ,'contacts'),0),id) else null end as intercom_customer_id, 
		state,
		"read",
		tags,
		json_extract_path_text(tags, 'tags') as tags_info,
		statistics,
		case when j.stat_first_assignment_at is not null then date_trunc('week',(timestamptz 'epoch' + stat_first_assignment_at ::int * interval '1 second')) else null end as stat_first_assignment_at,
		case when j.stat_first_admin_reply_at is not null then date_trunc('week',(timestamptz 'epoch' + stat_first_admin_reply_at ::int * interval '1 second')) else null end as stat_first_admin_reply_at,
		case when statistics is not null then JSON_EXTRACT_PATH_text("statistics" ,'count_assignments') else NULL end as count_assignments,
		case when statistics is not null then JSON_EXTRACT_PATH_text("statistics" ,'count_conversation_parts') else NULL end as count_conversation_parts,
		priority ,
		conversation_rating,
		date_trunc('week',(timestamptz 'epoch' + rating_created_at ::int * interval '1 second')) as rating_created_at,
		case when conversation_rating is not null then
			  case when JSON_EXTRACT_PATH_text("conversation_rating" ,'rating') = '' then NULL 
				  else JSON_EXTRACT_PATH_text("conversation_rating" ,'rating') end 
			else NULL end as rating,
		case when conversation_rating is not null then 
			 case when JSON_EXTRACT_PATH_text("conversation_rating" ,'remark') = '' then NULL 
				 else JSON_EXTRACT_PATH_text("conversation_rating" ,'remark') end 
			else NULL end as customer_remark
	from stg_external_apis.intercom_conversations_list s
	left join json_time_extracts j on ((s.id = j.conversation_id) and j.total = 1);


--- TABLE FOR TAG INFO --
	drop table if exists ods_data_sensitive.intercom_tag_info;
	create table ods_data_sensitive.intercom_tag_info as 
	with a as (
	select date_trunc('week',(timestamptz 'epoch' + created_at::int * interval '1 second')) as date_convert,
	id as conversation_id,
	json_extract_path_text(tags, 'tags') as tags_info,
	json_array_length(json_extract_path_text(tags, 'tags')) as total_items
	from stg_external_apis.intercom_conversations_list 
	order by total_items desc),
	numbers as (
	select * from public.numbers
	where ordinal < 20)	
	select a.*,
	json_extract_array_element_text(tags_info, pn.ordinal::int,true) as tag_split,
	json_extract_path_text(tag_split, 'id') as tag_id,
	json_extract_path_text(tag_split, 'name') as tag_name,
	json_extract_path_text(tag_split, 'applied_at') as applied_at,
	date_trunc('week',(timestamptz 'epoch' + applied_at::int * interval '1 second')) as tag_applied_at
	from a 
	cross join public.numbers pn 
	where pn.ordinal < (a.total_items);
