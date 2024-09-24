drop table if exists scratch.se_events;
create table scratch.se_events as
select
event_id,
collector_tstamp,
platform,
domain_userid,user_id,se_action,se_category,se_label,se_property,se_value,domain_sessionid, s.is_qa_url,
 case when dvce_type in ('Computer','Mobile') then dvce_type else 'Other' end as dvce_type,
 case when os_family in ('Windows','Android','iOS','Mac OS X','Linux') then os_family else 'Others' end as os_family,
 case when br_family in (
'Chrome',
'Safari',
'Firefox',
'Microsoft Edge',
'Opera',
'Apple WebKit',
'Internet Explorer'
) then br_family else 'Others' end as br_family,
trim(case
 when IS_VALID_JSON(se_property)
  then trim(json_extract_path_text(se_property,'current_flow')::varchar(666))
   else null
  end) as se_current_flow
from atomic.events e
left join web.sessions_snowplow s on e.domain_sessionid=s.session_id
where se_action not in ('heartbeat')
and se_action is not null
and e.useragent not like '%Datadog%';


grant select on 	scratch.se_events	 to shikhar_srivastava;
