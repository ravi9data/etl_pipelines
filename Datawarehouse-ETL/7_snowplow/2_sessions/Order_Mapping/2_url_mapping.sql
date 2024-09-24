DROP TABLE IF EXISTS scratch.session_order_url_mapping;
CREATE TABLE scratch.session_order_url_mapping AS 
SELECT DISTINCT 
  pv.domain_sessionid AS session_id
 ,NULLIF(REGEXP_SUBSTR(page_urlpath, '([A-Z]{1}\\d{9,}\)'), '') AS order_id
 ,MIN(etl_tstamp) AS etl_tstamp
FROM scratch.web_events pv 
WHERE REGEXP_INSTR(page_urlpath, '([A-Z]{1}\\d{9,}\)') > 0
GROUP BY 1,2

UNION 

SELECT DISTINCT 
  pv.domain_sessionid AS session_id 
 ,NULLIF(REGEXP_SUBSTR(refr_urlquery, '([A-Z]{1}\\d{9,}\)'), '') AS order_id
 ,MIN(etl_tstamp) AS etl_tstamp
FROM scratch.web_events pv 
WHERE LOWER(refr_urlquery) like '%orderid%'
  AND order_id IS NOT NULL
GROUP BY 1,2
 ; 