drop table if exists stg_external_apis.intercom_tag_info;

create table stg_external_apis.intercom_tag_info as
	with a as (
	select date_trunc('week',(timestamptz 'epoch' + created_at::int * interval '1 second')) as date_convert,
	id as conversation_id,
	json_extract_path_text(tags, 'tags') as tags_info,
	json_array_length(json_extract_path_text(tags, 'tags')) as total_items
	from stg_external_apis.intercom_conversations 
	order by total_items desc),
	numbers as (
	select * from public.numbers
	where ordinal < 20)
	select a.*,
	json_extract_array_element_text(tags_info, pn.ordinal::int,true) as tag_split,
	json_extract_path_text(tag_split, 'id') as tag_id,
	json_extract_path_text(tag_split, 'name') as tag_name
	--json_extract_path_text(tag_split, 'applied_at') as applied_at,
	--(timestamptz 'epoch' + applied_at::int * interval '1 second') as tag_applied_at
	from a
	cross join public.numbers pn
	where pn.ordinal < (a.total_items);