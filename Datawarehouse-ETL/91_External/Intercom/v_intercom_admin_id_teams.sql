create or replace view ods_data_sensitive.v_intercom_admin_id_teams
as
with numbers as 
	(
	select 
	* 
	from public.numbers
	where ordinal < 70
	)
	,team_with_admins as
	(
	select 
	 name team_name
	,id team_id 
	--,  * 
	, json_array_length(admin_ids) length_admins
	, JSON_EXTRACT_ARRAY_ELEMENT_TEXT(admin_ids,pn.ordinal::integer) admin_id
	from s3_spectrum_intercom.teams
	cross join numbers pn
	where pn.ordinal < length_admins
	)
select 
admin_id, 
listagg(team_name, ', ') teams_names, 
listagg(team_id, ', ') teams_ids,
listagg(case 	when team_name = 'USA - Chat &amp; Email' then 'USA - Chat & Email' 
				then team_name
				else '' 
		end, ', ') as team_participated --same as first conversation
from team_with_admins
group by 1
WITH NO SCHEMA BINDING;
