DROP VIEW IF EXISTS marketing.v_page_scroll_report;
CREATE VIEW marketing.v_page_scroll_report AS 
SELECT 
  DATE_TRUNC('week', page_view_date::DATE) AS page_view_start
 ,store_name
 ,vertical_percentage_scrolled_tier 
 ,user_bounced
 ,user_engaged
 ,CASE 
   WHEN page_url LIKE '%your-tech/%'
    THEN LEFT(page_url, POSITION('your-tech/' IN page_url) -1)  || 'your-tech/'
   WHEN page_url LIKE '%/confirmation/%'
    THEN LEFT(page_url, POSITION('/confirmation/' IN page_url) +13) 
   WHEN page_url LIKE '%payment-methods%'
    THEN LEFT(page_url, POSITION('payment-methods' IN page_url) +15)    
   WHEN page_url LIKE '%account-security/devices-browsers%'
    THEN LEFT(page_url, POSITION('account-security/devices-browsers' IN page_url) +33)  
   ELSE page_url 
  END AS page_url 
 ,page_title 
 ,page_type
 ,COUNT(DISTINCT anonymous_id) unique_users
FROM traffic.page_views pv 
WHERE TRUE
  AND page_view_start::DATE >= CURRENT_DATE - 120 
GROUP BY 1,2,3,4,5,6,7,8
WITH NO SCHEMA BINDING
;