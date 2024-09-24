drop table if exists dm_operations.jira_phase_mapping ;

SET enable_case_sensitive_identifier TO true;

create table dm_operations.jira_phase_mapping as 
with level_one as(
SELECT
	i._airbyte_data.id::int AS issueid,
	i._airbyte_data."key"::text AS issuekey,
	convert_to_utc_timestamp(issue_log.created::text) AS event_created_at,
	convert_to_utc_timestamp(i._airbyte_data.created::text) AS issuecreateddate,
	issue_log.author."displayName"::text authordisplayname,
	COALESCE(i._airbyte_data.fields.assignee."displayName"::text,'Not Assigned') AS assignedname ,
	items.field::text AS itemfield,
	items."fromString"::text AS itemfromstring,
	items."toString"::text AS itemtostring,
	i._airbyte_data.fields.project.name::text AS projectname,
	convert_to_utc_timestamp(i._airbyte_data.fields.resolutiondate::text) AS resolutiondate,
	i._airbyte_data.fields.priority."name"::text AS priorityname,
	i._airbyte_data."key"::text,
	i._airbyte_data.fields.status."name"::text AS issue_status,
	count(case when itemfield = 'status' then issueid else null end) over (partition by "key") as total_Status,
	rank() over (partition by issuekey order by event_created_at asc  ) as event_idx
FROM
	stg_external_apis._airbyte_raw_issues i,
	i._airbyte_data.changelog.histories issue_log,
	issue_log.items items
)
,no_change as (
select * from 
level_one
where total_Status = 0
)
,tbl_new as (
	select 
	issuecreateddate::date as start_date,
	current_date::date as end_date,
	issueid,
	issuekey,
	event_Created_at,
	authordisplayname,
	assignedname,
    issue_status as status_bod,
	issue_status as status_eod,
	projectname,
	resolutiondate,
	priorityname,
	1 as day_idx,
	0 as total_events,
	1 as idx
	from no_change 
	where event_idx = 1
	and issuekey is not null )
,with_change as (
select *,
rank() over (partition by issuekey,event_created_at::date order by event_created_at desc ) as day_idx
from 
level_one
where total_Status > 0 
and itemfield = 'status'
)
, idx as (
select *,
count(*) over (partition by issuekey) as total_events,
row_number() over (partition by issuekey order by event_created_at asc ) as idx
from with_change
where day_idx = 1
order by event_created_at desc
)
,mapping as (
select 
case when idx = 1 then issuecreateddate::date
else (lag(event_created_at::date) over (partition by issueid order by idx asc))+1
		end as start_date,
case when resolutiondate is null and (idx = total_events) then current_date::date
when resolutiondate is not null and (idx = total_events) then current_date::date
	else Date_trunc('day',event_created_at) end as end_date,		
		issueid,
		issuekey,
		event_Created_at,
		authordisplayname,
	    assignedname,
  	    itemfromstring as status_bod,
		itemtostring as status_eod,
		projectname,
		resolutiondate,
		priorityname,
		day_idx,
		total_events,
		idx
from idx 
order by idx )
,union_tables as (
select *  from mapping 
union all 
select * from tbl_new
)
,
components as (
select i._airbyte_data."key"::text AS issuekey,
listagg(c.name::Text, ' | ') as components_agg
from stg_external_apis._airbyte_raw_issues i, i._airbyte_data.fields.components AS c
group by 1) 
select ut.*, components_agg from union_tables ut 
left join components c on ut.issuekey = c.issuekey
;


drop table if exists dm_operations.jira_historical;
create table dm_operations.jira_historical as 
with dates as (
			select
			datum,
			day_is_weekday,
			week_day_number,
			day_is_first_of_month,
			day_is_last_of_month
			from 
			public.dim_dates f 
			where datum > '2022-03-28'
			and datum <= current_date 
			)
		select 
		f.datum as fact_day,
		case when resolutiondate is not null and (idx = total_events) then  
		     case when datum < resolutiondate::date then status_bod 
		       else status_eod end 
		   when datum = end_date then status_eod 
		   else status_bod 
		     end   as
		status_eod,
		projectname,
		priorityname,
		assignedname,
		components_agg,
		count(issuekey) as issues
		from dates f 
		left join dm_operations.jira_phase_mapping ja
		   on f.datum::date >= ja.start_date
		   and F.datum::date <= coalesce(ja.end_date::date, f.datum::date+1)
           where status_eod is not null --Removing all the tickets with No status
		   group by 1,2,3,4,5,6
		   order by 1 desc;


/*Historical snapshot table on the ticket level*/
            drop table if exists dm_operations.jira_historical_full;
           create table dm_operations.jira_historical_full as
        with dates as (
			select
			datum,
			day_is_weekday,
			week_day_number,
			day_is_first_of_month,
			day_is_last_of_month
			from 
			public.dim_dates f 
			where datum > '2022-03-28'
			and datum <= current_date 
			)
		select 
		f.datum as fact_day,
		issuekey,
		case when resolutiondate is not null and (idx = total_events) then  
		     case when datum < resolutiondate::date then status_bod 
		       else status_eod end 
		   when datum = end_date then status_eod 
		   else status_bod 
		     end   as
		status_eod,
		projectname,
		priorityname,
		assignedname,
		components_agg,
		resolutiondate,
		day_is_weekday,
		week_day_number,
		day_is_first_of_month,
		day_is_last_of_month
		from dates f 
		left join dm_operations.jira_phase_mapping ja
		   on f.datum::date >= ja.start_date
		   and F.datum::date <= coalesce(ja.end_date::date, f.datum::date+1)
		   order by 1 desc;
		   
		   
TRUNCATE TABLE stg_external_apis.jira_issues;


INSERT
	INTO
	stg_external_apis.jira_issues (id,
	"key",
	issuetypeid,
	issuetypename,
	projectid,
	projectname,
	projectkey,
	parentid,
	parentkey,
	resolutionid,
	resolutionname,
	resolutiondescription,
	resolutiondate,
	workratio,
	lastviewed,
	watchcount,
	iswatching,
	created,
	priorityid,
	priorityname,
	timespentseconds,
	timespent,
	assigneedisplayname,
	assigneekey,
	assigneeaccountid,
	assigneeemail,
	updated,
	statusid,
	statusname,
	description,
	summary,
	creatordisplayname,
	creatorname,
	creatorkey,
	creatoremail,
	reporterdisplayname,
	reportername,
	reporterkey,
	reporteremail,
	aggregateprogress,
	totalprogress,
	votes,
	hasvotes,
	duedate,
	labels,
	environment,
	componentsaggregate,
	issuelinksaggregate,
	"subscription id",
	"is b2b customer?",
	country,
	"shipment tracking number _",
	"delivered to warehouse?",
	label,
	"order no.",
	"unique identifier _",
	"allocation date",
	"linked intercom conversation ids")
SELECT
	i._airbyte_data.id::int AS id,
	i._airbyte_data."key"::text AS "key",
	i._airbyte_data.fields.issuetype.id::text AS "issuetypeid",
	i._airbyte_data.fields.issuetype.name::text AS "issuetypename",
	i._airbyte_data.fields.project.id::text AS "projectid",
	i._airbyte_data.fields.project."name"::text AS "projectname",
	i._airbyte_data.fields.project."key"::text AS "projectkey",
	i._airbyte_data.fields.parent."id"::bigint AS "parentid",
	i._airbyte_data.fields.parent."key"::text AS "parentkey",
	i._airbyte_data.fields.resolution."id"::text AS "resoultionid",
	i._airbyte_data.fields.resolution."name"::text AS "resoultionname",
	i._airbyte_data.fields.resolution."description"::text AS "resoultiondescription",
	convert_to_utc_timestamp(i._airbyte_data.fields."resolutiondate"::text) AS "resolutiondate",
	i._airbyte_data.fields.workratio::int AS "workratio",
	i._airbyte_data.fields."lastViewed"::timestamp AS "lastViewed",
	i._airbyte_data.fields.watches."watchCount"::int AS "watchCount",
	i._airbyte_data.fields.watches."isWatching"::bool AS "isWatching",
	i._airbyte_data.fields."created"::timestamp AS "created",
	i._airbyte_data.fields.priority."id"::text AS "priorityid",
	i._airbyte_data.fields.priority."name"::text AS "priorityname",
	i._airbyte_data.fields.timespent."seconds"::text AS "timespentseconds",
	i._airbyte_data.fields.timespent::text AS timespent,
	i._airbyte_data.fields.assignee."displayName"::text AS assingeedisplayName,
	i._airbyte_data.fields.assignee."key"::text AS assingeekey,
	i._airbyte_data.fields.assignee."accountId"::text AS assingeeaccountId,
	i._airbyte_data.fields.assignee."emailAddress"::text AS assingeeemailAddress,
	i."_airbyte_data".updated::timestamp AS updated,
	i._airbyte_data.fields.status."id"::text AS statusid,
	i._airbyte_data.fields.status."name"::text AS statusname,
	i._airbyte_data."renderedFields"."description"::text AS description,
	i._airbyte_data.fields.summary::text AS summary,
	i._airbyte_data.fields.creator."displayName"::text AS creatordisplayName,
	i._airbyte_data.fields.creator.name::text AS creatorname,
	i._airbyte_data.fields.creator."key"::text AS creatorkey,
	i._airbyte_data.fields.creator."emailAddress"::text AS createremail,
	i._airbyte_data.fields.reporter."displayName"::text AS reporterdisplayName,
	i._airbyte_data.fields.reporter.name::text AS reportername,
	i._airbyte_data.fields.reporter."key"::text AS reporterkey,
	i._airbyte_data.fields.reporter."emailAddress"::text AS reporteremail,
	i._airbyte_data.fields.aggregateprogress.progress::text AS aggregateprogress,
	i._airbyte_data.fields.aggregateprogress.total::text AS totalprogress,
	i._airbyte_data.fields.votes.votes::int AS votes,
	i._airbyte_data.fields.votes.hasVoted::bool AS hasvotes,
	i._airbyte_data.fields.duedate::timestamp AS duedate,
	i._airbyte_data.fields.labels::text AS labels,
	i._airbyte_data.fields.environment::Text AS environment,
	i._airbyte_data.fields.components::text AS componentsaggregate,
	i._airbyte_data.fields.issuelinks::text AS issuelinksaggregate,
	i._airbyte_data.fields.customfield_10660::text AS "subscription id",
	i._airbyte_data.fields.customfield_10718[0].value::text AS "is b2b customer?",
	i._airbyte_data.fields.customfield_10708.value::text AS "country",
	i._airbyte_data.fields.customfield_10712::text AS "shipment tracking number _",
	i._airbyte_data.fields.customfield_10717[0].value::text AS "delivered to warehouse?",
	i._airbyte_data.fields.labels::text AS label,
	i._airbyte_data.fields.customfield_10658::text AS "order no.",
	i._airbyte_data.fields.customfield_10711::text AS "unique identifier _",
	i._airbyte_data.fields.customfield_10710::timestamp AS "allocation date",
	i._airbyte_data.fields.customfield_10657::text AS "linked intercom conversation ids"
FROM
	stg_external_apis._airbyte_raw_issues i;
	
TRUNCATE TABLE stg_external_apis.jira_components;

INSERT INTO stg_external_apis.jira_components(id,"name",issueid,issuekey,issuecreateddate,issueupdateddate,description)
SELECT
	ac.id::int,
	ac.name::text,
	i._airbyte_data.id::int AS issueid,
	i._airbyte_data."key"::text AS issuekey,
	convert_to_utc_timestamp(i._airbyte_data.created::text) AS issuecreateddate,
	convert_to_utc_timestamp(i._airbyte_data.updated::text) AS issueupdateddate,
	ac.description::text
FROM
	stg_external_apis."_airbyte_raw_issues" i,
	i."_airbyte_data".fields.components AS ac ;
	
RESET enable_case_sensitive_identifier;

GRANT SELECT ON dm_operations.jira_historical TO tableau;
GRANT SELECT ON dm_operations.jira_historical_full TO tableau;
