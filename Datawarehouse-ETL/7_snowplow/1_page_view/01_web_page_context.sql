DROP TABLE IF EXISTS scratch.web_page_context;
CREATE TABLE scratch.web_page_context
  DISTKEY(page_view_id)
  SORTKEY(page_view_id)
AS (

  -- deduplicate the web page context in 2 steps

  WITH prep AS (
    SELECT
	  root_id,
      id AS page_view_id
   FROM atomic.com_snowplowanalytics_snowplow_web_page_1
   GROUP BY 1,2
  )

  SELECT 
  	* 
  FROM prep 
 WHERE root_id NOT IN (SELECT root_id FROM prep GROUP BY 1 HAVING COUNT(*) > 1) 
  -- exclude all root ID with more than one page view ID

);
