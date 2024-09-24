DROP TABLE IF EXISTS scratch.web_events_time;
CREATE TABLE scratch.web_events_time
  DISTKEY(page_view_id)
AS (

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
    and ev.useragent not like '%Datadog%' 

  GROUP BY 1

);