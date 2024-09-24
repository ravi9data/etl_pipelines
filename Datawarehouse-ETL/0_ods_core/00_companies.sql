drop table if exists ods_production.companies;
create table ods_production.companies as 
with a as (
select distinct 
	u.id as customer_id,
	c.id as company_id,
	c.name AS company_name,
	c.status,
	c.created_at,
	GREATEST(c.updated_at, t.updated_at) as updated_at,
	c.company_type_id, 
	v2.company_id as company_gbp_enabled,
	t.name as company_type_name, --regular company type name
	e.companytypeadd as new_company_type_name --new company type name from google sheet file
from stg_api_production.spree_users u --getting the customer_id to join on google sheet file
left join stg_api_production.companies c
	on u.company_id = c.id
left join stg_api_production.company_types t
	on t.id = c.company_type_id
left join stg_curated.b2b_eu_dashboard_access_activated_v2 v2
	on v2.company_id = c.id 
left join stg_external_apis.company_type_enhancement e
	on e.customerid = u.id
	where u.company_id is not null)
select distinct
		a.customer_id, 
		a.company_id,
		a.company_name,
		a.status,
		a.created_at,
		a.updated_at,
		case 
			when a.company_gbp_enabled is not null then true
			else false end as is_gbp_enabled,
		cp.employees_count as gpb_employees_submitted,
		a.company_type_id,
		(case 
			when a.new_company_type_name is null then a.company_type_name
			else a.new_company_type_name end) as company_type_name,
		ltc.lead_type,
		bbfm.is_freelancer 
from a
	left join stg_external_apis.lead_type_companies ltc 
		on a.company_id = ltc.company_id
	left join stg_external_apis.gbp_employees cp  
 		on cp.company_id = a.company_id
	left join dm_risk.b2b_freelancer_mapping bbfm 
		on bbfm.company_type_name = a.company_type_name 
;

GRANT SELECT ON ods_production.companies TO tableau;
GRANT SELECT ON ods_production.companies TO b2b_redash;
