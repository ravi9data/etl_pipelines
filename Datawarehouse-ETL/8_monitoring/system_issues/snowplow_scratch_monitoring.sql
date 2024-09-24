insert into monitoring.snowplow_scratch  
WITH prep AS (
    SELECT
      TRIM(wp.page_view_id) AS page_view_id,
      ev.dvce_created_tstamp -- included to sort on
    FROM atomic.events AS ev
    INNER JOIN scratch.web_page_context AS wp -- an INNER JOIN guarantees that all rows have a page view ID
      ON ev.event_id = wp.root_id
    WHERE ev.platform = 'web' AND (ev.event_name = 'page_view')-- filtering on page view events removes the need for a FIRST_VALUE function
    and ev.name_tracker not in ('snplow1')
    and etl_tstamp::date<CURRENT_DATE::date 
  ) 
   SELECT 
   	'atomic' as schema_name,
   	'scratch_events' as table_name,
   	CURRENT_DATE::DATE-1 ,
   	count(*) 
   FROM (SELECT *, ROW_NUMBER () OVER (PARTITION BY page_view_id ORDER BY dvce_created_tstamp) AS n FROM prep) WHERE n = 1;

insert into monitoring.snowplow_scratch
 SELECT 
   	'scratch' as schema_name,
   	'scratch_events' as table_name,
   	CURRENT_DATE::DATE-1 as reporting_date,
   	count(*) 
   FROM scratch.web_events 
where etl_tstamp::date<CURRENT_DATE::date ;