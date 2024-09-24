create temp table web_events_time_dl (like scratch.web_events_time); 

insert into web_events_time_dl 
SELECT
    TRIM(wp.page_view_id) AS page_view_id,
    MIN(ev.derived_tstamp) AS min_tstamp, -- requires the derived timestamp (JS tracker 2.6.0+ and Snowplow 71+)
    MAX(ev.derived_tstamp) AS max_tstamp, -- requires the derived timestamp (JS tracker 2.6.0+ and Snowplow 71+)
    SUM(CASE WHEN ev.event_name = 'page_view' THEN 1 ELSE 0 END) AS pv_count, -- for debugging
    SUM(CASE WHEN ev.event_name = 'page_ping' THEN 1 ELSE 0 END) AS pp_count, -- for debugging
    10 * COUNT(DISTINCT(FLOOR(EXTRACT(EPOCH FROM ev.derived_tstamp)/10))) - 10 AS time_engaged_in_s -- assumes 10 seconds between subsequent page pings
  FROM atomic.events AS ev
  INNER JOIN scratch.web_page_context AS wp
    ON ev.event_id = wp.root_id
  WHERE ev.event_name IN ('page_view', 'page_ping','action','event')
	and ev.etl_tstamp ::date> DATEADD(week, -1, CURRENT_DATE)
  and ev.useragent not like '%Datadog%' 
	GROUP BY 1;

begin transaction;

delete from scratch.web_events_time 
using web_events_time_dl s
where web_events_time.page_view_id = s.page_view_id; 


insert into scratch.web_events_time 
select * from web_events_time_dl;

end transaction;


drop table web_events_time_dl;
