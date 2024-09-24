drop table if exists ods_data_sensitive.intercom_first_conversation  cascade;
create table ods_data_sensitive.intercom_first_conversation as 
with a as (
	select 
		distinct  id, 
		teammates,
		len(teammates),
		(JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),0),'id')) as admin_participated_one,
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),1),'id') as admin_participated_two,
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),2),'id') as admin_participated_three,
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),3),'id') as admin_participated_four,
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),4),'id') as admin_participated_five,
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),5),'id') as admin_participated_six,
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),6),'id') as admin_participated_seven,
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),7),'id') as admin_participated_eight,
		(JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),0),'id') + ', ' +
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),1),'id') +', ' + 
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),2),'id') +', ' + 
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),3),'id') +', ' + 
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),4),'id') +', ' + 
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),5),'id') +', ' + 
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),6),'id') +', ' +
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),7),'id') +', ' + 
		JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text (teammates, 'admins'),8),'id')) as admin_ids
	from stg_external_apis.intercom_conversations
	
	)	
	
,teams as(
	select 
		distinct a.id, 
		listagg(distinct case when it.name = 'USA - Chat &amp; Email' then 'USA - Chat & Email' else it.name end, ', ') as team_participated,
		count (*) as teams_count 
	from a 
	left join s3_spectrum_intercom.teams it 
	on it.admin_ids ilike (case when admin_participated_one != '' then '%'+a.admin_participated_one+'%' end)
	or (it.admin_ids ilike (case when admin_participated_two != '' then '%'+a.admin_participated_two+'%' end)
	or it.admin_ids ilike (case when admin_participated_three != '' then '%'+a.admin_participated_three+'%' end)
	or it.admin_ids ilike (case when admin_participated_four != '' then '%'+a.admin_participated_four+'%' end)
	or it.admin_ids ilike (case when admin_participated_five != '' then '%'+a.admin_participated_five+'%' end)
	or it.admin_ids ilike (case when admin_participated_six != '' then '%'+a.admin_participated_six+'%' end)
	or it.admin_ids ilike (case when admin_participated_seven != '' then '%'+a.admin_participated_seven+'%' end)
	or it.admin_ids ilike (case when admin_participated_eight != '' then '%'+a.admin_participated_eight+'%' end))
	where it.id in (Select distinct "team assigned id" from stg_external_apis.teams_for_assignment_intercom tfai where "team participated"='Yes')--5163706, 4516337, 4481975,4559118,5578958)
	group by 1 order by 3 desc
	)	,
 t1 AS (
	SELECT
		id,json_parse(tags) AS tags_v1
	FROM
		stg_external_apis.intercom_conversations)
,t2 AS (
	SELECT
		id,tags_v1.tags AS tag_array
		FROM t1
		),
t3 AS (
	SELECT ned.id, CAST(t1.name AS text) tag_name
  		FROM t2 AS ned,
  		ned.tag_array AS t1 ),
tag_name as (
 		SELECT id,listagg(tag_name,' ,') as "name"
 		FROM t3
 		GROUP BY id)
,format as (
	  select 
	    fc.id,
	    (timestamptz 'epoch' + created_at::int * interval '1 second') as created_at,
	    (timestamptz 'epoch' + updated_at ::int * interval '1 second') as updated_at,
	    JSON_EXTRACT_PATH_text(json_extract_array_element_text(attachments, 0), 'type') as attachment_type,
	    JSON_EXTRACT_PATH_text(json_extract_array_element_text(attachments, 0), 'name') as attachment_name,
	    JSON_EXTRACT_PATH_text(json_extract_array_element_text(attachments, 0), 'url') as attachment_url,
	    JSON_EXTRACT_PATH_text(json_extract_array_element_text(attachments, 0), 'content_type') as attachment_content_type,
	    JSON_EXTRACT_PATH_text(json_extract_array_element_text(attachments, 0), 'filesize') as attachment_filesize,
	    (timestamptz 'epoch' + waiting_since ::int * interval '1 second') as waiting_since,
	    (timestamptz 'epoch' + snoozed_until ::int * interval '1 second') as snoozed_until,  
	    JSON_EXTRACT_PATH_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text(contacts,'contacts'),0),'id') as intercom_customer_id, 
	    delivered_as,
	    source_id, 
	    redacted,
	  	 title,
	  	 subject,
		 body,
	    JSON_EXTRACT_PATH_text("author", 'type') as author_type,
	    JSON_EXTRACT_PATH_text("author", 'id') as author_id,
	    JSON_EXTRACT_PATH_text("author", 'name') as author_name,
	    JSON_EXTRACT_PATH_text("author", 'email') as author_email,
	    (timestamptz 'epoch' + (JSON_EXTRACT_PATH_text("first_contact_reply", 'created_at'))::int * interval '1 second') as fc_reply_at,
	    JSON_EXTRACT_PATH_text("first_contact_reply" ,'type') as fc_reply_type,
	    JSON_EXTRACT_PATH_text("first_contact_reply" ,'url') as fc_url,
	    fc."open",
	    state,
	    read,
	    priority,
	    JSON_EXTRACT_PATH_text(sla_applied,'sla_name') as sla_name, 
		 JSON_EXTRACT_PATH_text(sla_applied,'sla_status') as sla_status,
	    case 
		   when JSON_EXTRACT_PATH_text(custom_attributes,'🌍 Market') ilike '%Germany%' then 'Germany'
			when JSON_EXTRACT_PATH_text(custom_attributes,'🌍 Market') ilike '%Netherlands%' then 'Netherlands'
			when JSON_EXTRACT_PATH_text(custom_attributes,'🌍 Market') ilike '%Austria%' then 'Austria'
			when JSON_EXTRACT_PATH_text(custom_attributes,'🌍 Market') ilike '%Spain%' then 'Spain'
	      when JSON_EXTRACT_PATH_text(custom_attributes,'🌍 Market') ilike '%USA%' then 'USA'
			when JSON_EXTRACT_PATH_text(custom_attributes,'🌍 Market') ='' then 'n/a'
			else JSON_EXTRACT_PATH_text(custom_attributes,'🌍 Market') 
		end as market,
		case 
			when JSON_EXTRACT_PATH_text(custom_attributes,'1️⃣st Level') ilike '%General Questions%' then 'General Questions' 
			when JSON_EXTRACT_PATH_text(custom_attributes,'1️⃣st Level') ilike '%Declined Orders%' then 'Declined Orders'
			when JSON_EXTRACT_PATH_text(custom_attributes,'1️⃣st Level') ilike '%Damage Claims%' then 'Damage Claims'
			when JSON_EXTRACT_PATH_text(custom_attributes,'1️⃣st Level') ='' then 'n/a'
			else JSON_EXTRACT_PATH_text(custom_attributes,'1️⃣st Level')
		end as first_level,
		case 
			when JSON_EXTRACT_PATH_text(custom_attributes,'2️⃣nd Level') ilike '%Cancellation%' then 'Cancellation'
			when JSON_EXTRACT_PATH_text(custom_attributes,'2️⃣nd Level') ilike '%Order Delay%' then 'Order Delay'
			when JSON_EXTRACT_PATH_text(custom_attributes,'2️⃣nd Level') ilike '%Personal Data%' then 'Personal Data'
			when JSON_EXTRACT_PATH_text(custom_attributes,'2️⃣nd Level') ='' then 'n/a'
			else JSON_EXTRACT_PATH_text(custom_attributes,'2️⃣nd Level')
		end as second_level,
		case 
			when JSON_EXTRACT_PATH_text(custom_attributes,'3️⃣rd Level') ilike '%Payments%' then 'Payments'
			when JSON_EXTRACT_PATH_text(custom_attributes,'3️⃣rd Level') ilike '%GDPR%' then 'GDPR'
			when JSON_EXTRACT_PATH_text(custom_attributes,'3️⃣rd Level') = '' then 'n/a'
			else JSON_EXTRACT_PATH_text(custom_attributes,'3️⃣rd Level') 
		end as third_level,
		case 
			when JSON_EXTRACT_PATH_text(custom_attributes,'4️⃣th Level') ilike '%Return Issues%' then 'Return Issues'
			when JSON_EXTRACT_PATH_text(custom_attributes,'4️⃣th Level') = '' then 'n/a'
			else JSON_EXTRACT_PATH_text(custom_attributes,'4️⃣th Level')
		end as fourth_level,
		t.team_participated,
		t.teams_count,
		admin_assignee_id,
		iac.name as admin_assignee_name,
		team_assignee_id, 
		it.name as team_assignee_name,
	   nullif(JSON_EXTRACT_PATH_text("statistics" ,'time_to_assignment'), '')::int  as time_to_assignment,
	   nullif(JSON_EXTRACT_PATH_text("statistics" ,'time_to_admin_reply'), '')::int/ 60  as time_to_admin_reply,
	   nullif(JSON_EXTRACT_PATH_text("statistics" ,'time_to_first_close'), '')::int/ 60 as time_to_first_close,
	   nullif(JSON_EXTRACT_PATH_text("statistics" ,'time_to_last_close'), '')::int/ 60 as time_to_last_close,
	   nullif(JSON_EXTRACT_PATH_text("statistics" ,'median_time_to_reply'), '')::int/ 60 as median_time_to_reply,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("statistics", 'first_assignment_at'), '')::int * interval '1 second') as first_assignment_at,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("statistics", 'first_admin_reply_at'), '')::int * interval '1 second') as first_admin_reply_at,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("statistics", 'first_close_at'), '')::int * interval '1 second') as first_close_at,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("statistics", 'last_assignment_at'), '')::int * interval '1 second') as last_assignment_at,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("statistics", 'last_assignment_admin_reply_at'), '')::int * interval '1 second') as last_assignment_admin_reply_at,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("statistics", 'last_contact_reply_at'), '')::int * interval '1 second') as last_contact_reply_at,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("statistics", 'last_admin_reply_at'), '')::int * interval '1 second') as last_admin_reply_at,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("statistics", 'last_close_at'), '')::int * interval '1 second') as last_close_at,
	   JSON_EXTRACT_PATH_text("statistics" ,'last_closed_by_id') as last_closed_by_id,
	   JSON_EXTRACT_PATH_text("statistics" ,'count_reopens') as count_reopens,
	   JSON_EXTRACT_PATH_text("statistics" ,'count_assignments') as count_assignments,
	   JSON_EXTRACT_PATH_text("statistics" ,'count_conversation_parts') as count_conversation_parts,
	   total_count,
	   nullif(JSON_EXTRACT_PATH_text("conversation_rating" ,'rating'), '')::int as conversation_rating,
	   JSON_EXTRACT_PATH_text("conversation_rating" ,'remark') as conversation_remark,
	--    JSON_EXTRACT_PATH_text(json_extract_array_element_text(conversation_rating, 0), 'name') as rating_teammate_id,
	   (timestamptz 'epoch' + nullif(JSON_EXTRACT_PATH_text("conversation_rating", 'created_at'), ' ')::int * interval '1 second') as conversation_rating_created_at,
	   JSON_EXTRACT_PATH_text (fc.teammates, 'admins') admins_list , 
	   json_array_length(JSON_EXTRACT_PATH_text (fc.teammates, 'admins'))  admins_list_length,
		custom_attributes, --to be used by snapshots table 
		JSON_EXTRACT_PATH_text(custom_attributes,'📄 Contact Reason') contact_reason,
		JSON_EXTRACT_PATH_text(custom_attributes,'📖 Duplicate') as Duplicate,
		JSON_EXTRACT_PATH_text(custom_attributes,'📄 B2B Contact Reason') as B2B_contact_reason,
		JSON_EXTRACT_PATH_text(custom_attributes,'📄 Card Contact Reason') as card_contact_reason,
		JSON_EXTRACT_PATH_text(custom_attributes,'✔️ Resolution Path') as resolution_path,
		JSON_EXTRACT_PATH_text(custom_attributes,'👤 Customer Type') as customer_type,		
		case when CHARINDEX('/',JSON_EXTRACT_PATH_text(custom_attributes,'📊 Contact Item'))>0 
         then SUBSTRING(JSON_EXTRACT_PATH_text(custom_attributes,'📊 Contact Item'),1,CHARINDEX('/',JSON_EXTRACT_PATH_text(custom_attributes,'📊 Contact Item'))-1) 
         else JSON_EXTRACT_PATH_text(custom_attributes,'📊 Contact Item') end contact_item_1, 
        CASE WHEN CHARINDEX('/',JSON_EXTRACT_PATH_text(custom_attributes,'📊 Contact Item'))>0 
         THEN SUBSTRING(JSON_EXTRACT_PATH_text(custom_attributes,'📊 Contact Item'),CHARINDEX('/',JSON_EXTRACT_PATH_text(custom_attributes,'📊 Contact Item'))+1,len(JSON_EXTRACT_PATH_text(custom_attributes,'📊 Contact Item')))  
         ELSE NULL END as contact_item_2,
		JSON_EXTRACT_PATH_text(custom_attributes,'✔️ DC Resolution Path') as dc_resolution_path,
		JSON_EXTRACT_PATH_text(custom_attributes,'🌐 Language') as "language",		
		tg."name" as tags
	from stg_external_apis.intercom_conversations fc 
	left join staging.intercom_deleted_conversations  del
		on fc.id  = del.id	
	left join s3_spectrum_intercom.teams it 
		on fc.team_assignee_id = it.id 
	left join s3_spectrum_intercom.admin_count iac 
		on fc.admin_assignee_id = iac.id
	left join tag_name tg 
		on fc.id = tg.id
	left join teams t 
		on fc.id  = t.id
	where fc.id  is not null and created_at is not null and del.id is null
	order by 3 desc)
select *
from format;

GRANT SELECT ON ods_data_sensitive.intercom_first_conversation TO tableau;
