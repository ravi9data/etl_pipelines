drop table if exists monitoring.snowplow_monitoring;
create table monitoring.snowplow_monitoring as(
select cast(page_view_date as date) as date,count(DISTINCT(session_id))as Sessions,count(DISTINCT(page_view_id)) as ApproxPageviews,marketing_medium,
  case
  when device_type = 'Computer' then 'desktop'
  when device_type = 'Mobile' then 'mobile'
  when device_type = 'Other' then 'Others'
  end as device_type,'snowplow' as Analytics
from web.page_views_snowplow
  --and cast(dvce_created_tstamp as date)between cast(getdate() as date) -19 and cast(getdate() as date)-1 
group by 1,4,5);



insert into monitoring.snowplow_monitoring
select *,'ga' from stg_external_apis.session_pageview_info;
--where cast("date" as date)between cast(getdate() as date) -19 and cast(getdate() as date)-1;




insert into monitoring.snowplow_monitoring
select cast(creation_time as date),count (DISTINCT(session_id)) ,0,'','','kenesis'
from stg_events.first_request
--where cast(creation_time as date)between cast(getdate() as date) -19 and cast(getdate() as date)-1
group by 1;

GRANT SELECT ON monitoring.snowplow_monitoring TO tableau;
