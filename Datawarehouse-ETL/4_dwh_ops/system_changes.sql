drop table if exists dm_operations.system_changes;
create table dm_operations.system_changes as
	select 
		sc.entity,
		date_trunc('week', sc.created_date) created_date ,
		case 
			when u.id = '00507000000TeiUAAS' -- Bulk Edit user, NodeOps Manual Backend 
			then 'Bulk Edit'
			when u.id in (
						'0053W000000bKt1QAE', -- NodeOps Backed (Not the bulk Edit User, it has middlename)
						'00558000000FxVXAA0', -- DA & IT Backend Grover
						'0051t000003ZSbmAAG', -- Tech Backend
						'0051t000003vT1jAAE', -- Tech Script
						'00558000000FVaEAAW'  -- IT Grover
						)
			then 'System'
			when u.id in (
						'0053W000000nEShQAM', -- Elene T.
						'0051t000004dRrqAAE',  -- Fernandes F.
						'00507000000S8TVAA0' -- Fabio
					)
			then 'Treasury'
			else 'Operations'
			end as user_type,
		--sc.createdbyid,
		sc.field_type,
		count(1) total_changes
	from ods_production.system_changes sc
	left join stg_salesforce."user" u 
	on sc.createdbyid = u.id
	where created_date > current_date - 365
	group by 1,2,3,4;



drop table if exists dm_operations.system_changes_weekly_agg;
create table dm_operations.system_changes_weekly_agg
as
with
	select 
		date as fact_date,
		count(1) total_assets ,
		count(case when date - 548 < created_at then 1 end) total_assets_18m --created within last 18 months
	from master.asset_historical 
	where asset_status_original not in (
		'WRITTEN OFF DC',
		'WRITTEN OFF OPS',
		'SOLD',
		'REPORTED AS STOLEN',
		'LOST'
		) 
	and date > current_date - 365
	group by 1
)
select 'System Changes in Asset' as metric_name,
	created_date as fact_date,
	sum(total_changes) metric_value
from dm_operations.system_changes
where entity = 'Asset'
group by 1,	2
union
select 'System Changes in Allocation' as metric_name,
	created_date as fact_date,
	sum(total_changes) metric_value
from dm_operations.system_changes
where entity = 'Allocation'
group by 1,	2
union
select 'Assets' as metric_name,
	fact_date,
	total_assets as metric_value
union
select 'Asset Created in 18M' as metric_name,
	fact_date,
	total_assets_18m as metric_value
union
select 'Total Allocations' as metric_name,
	date_trunc('week', allocated_at) as fact_date,
	count(1) as metric_value
from ods_production.allocation
where allocated_at > current_date - 365
group by 1,	2;

GRANT SELECT ON dm_operations.system_changes_weekly_agg TO tableau;
GRANT SELECT ON dm_operations.system_changes TO tableau;
