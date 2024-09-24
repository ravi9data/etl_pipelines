DROP TABLE IF EXISTS scratch.session_order_event_mapping;
CREATE TABLE scratch.session_order_event_mapping AS 
WITH events AS (
SELECT 
  TRIM(domain_sessionid) AS session_id
 ,TRIM(order_id) as order_id  
 ,COUNT(*) AS event_count
 ,MIN(collector_tstamp) AS collector_tstamp
FROM scratch.se_events_flat
WHERE order_id IS NOT NULL
GROUP BY 1,2
)
SELECT 
  e.session_id 
 ,e.order_id  AS order_id
 ,e.collector_tstamp
 ,SUM(e.event_count) AS event_count
FROM events e 
  INNER JOIN ods_production.order o 
    ON e.order_id = o.order_id
GROUP BY 1,2,3;