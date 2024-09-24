drop table if exists dm_operations.intercom_assignments_month_market;
create table dm_operations.intercom_assignments_month_market as
with aa as(
select 
distinct 
	date_trunc('day',conversation_part_created_at::timestamp)::date as fact_day,
	date_trunc('week',conversation_part_created_at::timestamp)::date as fact_week,
	conversation_id, 
	ifc.market, 
	conversation_part_created_at as created_at,
	ifc.updated_at,
	conversation_part_author_name,
	conversation_part_type, 
	conversation_part_assigned_to_type,
	icp.conversation_part_assigned_to_id,
	case when "team assigned id" is not null then "team assigned name"
	else null end as is_selected_team,
	it."name" as team_name,
	ac."name" as admin_name,
	case 
		when conversation_part_assigned_to_type = 'admin' and ac."open" = 0 and ac.closed = 0 then 'inactive admin'
		when conversation_part_assigned_to_type = 'admin' and (ac."open" != 0 or ac.closed != 0) then 'active admin'
		when conversation_part_assigned_to_type = 'admin' and ac.id is null then 'inactive admin'
	else null	
	end as is_active_admin,
	case when conversation_part_assigned_to_type != 'admin' or viait.team_participated = '' then null else viait.team_participated
	end as is_team_participated,
	last_close_at::date as last_close_at
from ods_data_sensitive.intercom_conversation_parts icp 
	left join ods_data_sensitive.intercom_first_conversation ifc 
		on icp.conversation_id = ifc.id 
	left join s3_spectrum_intercom.teams it 
		on it.id = conversation_part_assigned_to_id
	left join stg_external_apis.teams_for_assignment_intercom i
		on conversation_part_assigned_to_id = "team assigned id"
	left join s3_spectrum_intercom.admin_count ac
		on  conversation_part_assigned_to_id = ac.id
	left join ods_data_sensitive.v_intercom_admin_id_teams viait
		on viait.admin_id = conversation_part_assigned_to_id
where conversation_part_assigned_to is not null 
and conversation_part_assigned_to_type not in ('bot', 'nobody_admin')
and date_trunc('day', conversation_part_created_at::timestamp)::date >= '2020-01-01'
order by updated_At desc ),
	cte_tags as
	(
	select
		conversation_id ,
		listagg(tag_name,', ') list_tags
	from stg_external_apis.intercom_tag_info iti
	group by 1
	)
	, a as (
	select
	convs.*,
	list_tags tags
	from aa convs
	left join cte_tags tags
	on convs.conversation_id = tags.conversation_id
	)
,final_ as (
SELECT 
*
from a 
where  
 conversation_part_type !='bulk_reassignment' 
	and (is_selected_team is not null 
	or (is_active_admin = 'inactive admin' and is_team_participated is  null)
	or (is_active_admin = 'active admin' and is_team_participated is not null)))
	select 
 distinct date_trunc('month', fact_day)::date as fact_month, 
  market,
  case when tags like '%B2B%' then true else false end as is_b2b_tag,
  count(distinct conversation_id) as total_incoming
from final_ 
where fact_month::date >= '2020-01-01'
group by 1, 2,3;
grant select on dm_operations.intercom_assignments_month_market to cs_redash;