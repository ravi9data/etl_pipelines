
drop table if exists ods_operations.intercom_assignments;  
create table ods_operations.intercom_assignments as  
with a as(
select 
distinct 
	date_trunc('day',convert_timezone ('Europe/Berlin',conversation_part_created_at::timestamp)::timestamp)::date as fact_day,
	date_trunc('week',convert_timezone ('Europe/Berlin',conversation_part_created_at::timestamp)::timestamp)::date as fact_week,
	conversation_id, 
	ifc.market, 
	conversation_part_created_at as created_at,
	convert_timezone ('Europe/Berlin',conversation_part_created_at::timestamp) AS created_at_berlin_timestamp,
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
	last_close_at::date as last_close_at,
	ifc."language"
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
AND NOT (conversation_part_type='open' or conversation_part_type ='close')
and conversation_part_assigned_to_type not in ('bot', 'nobody_admin')
and date_trunc('day', conversation_part_created_at::timestamp)::date >= '2022-01-01'
order by updated_At desc )
SELECT 
*
from a 
where  
 conversation_part_type !='bulk_reassignment' 
	and (is_selected_team is not null 
	or (is_active_admin = 'inactive admin' and is_team_participated is  null)
	or (is_active_admin = 'active admin' and is_team_participated is not null));
	
	grant select on ods_operations.intercom_assignments to group customer_support;
