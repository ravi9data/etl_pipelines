--delete from ods_data_sensitive.intercom_performance_track  where conversation_updated_at >= (CURRENT_DATE - interval '1 day');
--insert into ods_data_sensitive.intercom_performance_track 

drop table if exists ods_data_sensitive.intercom_performance_track;
create table ods_data_sensitive.intercom_performance_track as 

with conversation_rating_for as (
		select distinct icp.conversation_id, 
		icp.conversation_part_body as rating_for, 
		ifc.conversation_rating
from ods_data_sensitive.intercom_conversation_parts icp  
left join ods_data_sensitive.intercom_first_conversation ifc 
on icp.conversation_id = ifc.id
where conversation_created_at >= '2021-01-01'
and conversation_part_body ilike ('<p>Bist du zufrieden mit%')
		),  b as(
select distinct 
date_trunc('hour',conversation_part_updated_at) as date, 
conversation_part_author_name as agent_name, 
conversation_part_author_email as agent_email,
ifc.delivered_as,
ifc.market,
count (*) as total_interactions,
count (distinct icp.conversation_id) as conversations_participated,
count(distinct case when conversation_part_type ='close' then icp.conversation_id end) as closed_conversations,
count(distinct case when conversation_part_type ='snoozed' then icp.conversation_id end) as conversations_snoozed,
count(distinct case when conversation_part_type ='note' then icp.conversation_id end) as note_on_conversations,
count(distinct case when conversation_part_type ='comment' then icp.conversation_id end) as comment_on_conversations,
count (distinct case when rf.rating_for is not null then rf.conversation_id end ) as conversation_rating_request,
count (distinct case when rf.rating_for is not null and rf.conversation_rating is not null then rf.conversation_id end ) as conversation_rating_filled,
count(distinct case when rf.rating_for is not null and rf.conversation_rating >= 4 then rf.conversation_id end ) as good_conversation_rating,
count(distinct case when rf.rating_for is not null and rf.conversation_rating < 4 then rf.conversation_id end ) as bad_conversation_rating
	from ods_data_sensitive.intercom_conversation_parts icp   
	left join ods_data_sensitive.intercom_first_conversation ifc 
	on ifc.id = icp.conversation_id 
	left join conversation_rating_for rf
	on rf.conversation_id = icp.conversation_id	and rf.rating_for = '<p>Bist du zufrieden mit ' + conversation_part_author_name+ '?</p>'
		where conversation_part_author_type = 'admin' 
          and conversation_part_updated_at::date >= '2021-01-01'
		and icp.conversation_part_type in ('close', 'comment', 'snoozed', 'note')
		and conversation_part_author_name != 'GroverBot'
		group by 1, 2, 3, 4, 5 order by 6 desc)
		select b.*,
		(case when conversation_rating_filled != 0 then (good_conversation_rating::float / conversation_rating_filled::float) end) as csat
		from b 
		--order by 5 desc
        ;

GRANT SELECT ON ods_data_sensitive.intercom_performance_track TO tableau;
