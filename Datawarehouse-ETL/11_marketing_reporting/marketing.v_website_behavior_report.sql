DROP VIEW IF EXISTS marketing.v_website_behavior_report;
CREATE VIEW marketing.v_website_behavior_report AS
SELECT 
  se_category
 ,se_label
 ,dvce_type
 ,os_family
 ,page_urlpath
 ,CASE 
  WHEN user_id IS NOT NULL 
   THEN 'Logged In' 
  WHEN user_id IS NULL 
   THEN 'Logged Out'
  END AS login_status
 ,collector_tstamp::DATE AS event_date
 ,SUM(CASE 
   WHEN se_action = 'widgetImpression' 
    THEN 1 
   ELSE 0 
  END) AS impressions
 ,SUM(CASE 
   WHEN se_action = 'widgetClick' 
    THEN 1 
   ELSE 0 
  END) AS clicks 
-- ,MIN(derived_tstamp) AS min_tmp
-- ,MAX(derived_tstamp) AS max_tmp
FROM "atomic".events
WHERE se_action in ('widgetImpression','widgetClick')
GROUP BY 1,2,3,4,5,6,7
WITH NO SCHEMA BINDING
;