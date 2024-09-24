DROP VIEW IF EXISTS marketing.v_organic_search_traffic;
CREATE VIEW marketing.v_organic_search_traffic AS
SELECT 
  s.session_start::DATE
 ,s.store_label 
 ,s.first_page_type 
 ,s.user_bounced
 ,s.first_page_url
 ,AVG(s.time_engaged_in_s) AS session_length 
 ,COUNT(DISTINCT s.session_id) AS count_of_session_ids
 ,AVG(page_views) AS count_of_page_views
 ,COUNT(DISTINCT s.anonymous_id) AS total_users 
 ,COUNT(DISTINCT s.customer_id) AS total_registered_users
 ,SUM(sc.is_paid) AS paid_orders
 ,SUM(sc.is_submitted) AS submitted_orders
FROM traffic.sessions s 
  LEFT JOIN web.session_conversions AS sc
    ON s.session_id = sc.session_id
WHERE marketing_channel='Organic Search' 
  AND first_page_url NOT ILIKE '%staging%'
GROUP BY 1,2,3,4,5
WITH NO SCHEMA BINDING
;