DROP TABLE IF EXISTS scratch.session_order_sp_user_mapping;
CREATE TABLE scratch.session_order_sp_user_mapping AS 
SELECT DISTINCT 
  pv.session_id
 ,o.order_id
 ,MIN(page_view_start) AS page_view_start
FROM web.page_views_snowplow pv 
  INNER JOIN ods_production.order o 
    ON o.customer_id = pv.customer_id
    AND 
      CASE 
        WHEN o.created_date::DATE < '2022-06-21' 
          THEN pv.page_view_start::DATE BETWEEN DATE_ADD('day', -90, o.created_date::DATE) 
             AND COALESCE(o.submitted_date, o.created_date::DATE + 7)
        ELSE pv.page_view_start::DATE BETWEEN DATE_ADD('day', -30, o.created_date::DATE) 
             AND COALESCE(o.submitted_date, o.created_date::DATE + 7)
        END
GROUP BY 1,2
;

DROP TABLE IF EXISTS scratch.session_order_snowplow_user_mapping;
CREATE TABLE scratch.session_order_snowplow_user_mapping AS
WITH order_customer AS (
SELECT DISTINCT
  order_id,
  submitted_date,
  created_date,
  snowplow_user_id
FROM ods_production.order o
  INNER JOIN web.page_views_snowplow p ON o.customer_id = p.customer_id
),

snowplow_user_sessions AS (
SELECT DISTINCT
  session_id,
  page_view_start::DATE AS session_date,
  snowplow_user_id
FROM web.page_views_snowplow)

SELECT DISTINCT
  su.session_id,
  o.order_id,
  MIN(su.session_date) AS session_date
FROM snowplow_user_sessions su
inner join order_customer o on o.snowplow_user_id=su.snowplow_user_id
    AND
      CASE
        WHEN o.created_date::DATE < '2022-06-21'
          THEN su.session_date::DATE BETWEEN DATE_ADD('day', -90, o.created_date::DATE)
             AND COALESCE(o.submitted_date, o.created_date::DATE + 7)
        ELSE su.session_date::DATE BETWEEN DATE_ADD('day', -30, o.created_date::DATE)
             AND COALESCE(o.submitted_date, o.created_date::DATE + 7)
        END
GROUP BY 1,2;