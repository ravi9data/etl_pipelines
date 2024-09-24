DROP VIEW IF EXISTS marketing.v_order_status_report;
CREATE VIEW marketing.v_order_status_report AS
WITH a AS (
SELECT DISTINCT 
  order_id AS order_id_other
 ,AVG(plan_duration) AS plan_duration
 ,COUNT(CASE 
   WHEN category_name = 'Phones & Tablets' 
    THEN order_item_id 
  END)  AS Phones_and_Tablets
 ,COUNT(CASE 
   WHEN category_name = 'Gaming & VR' 
    THEN order_item_id 
  END) AS Gaming_and_VR
 ,COUNT(CASE 
   WHEN category_name = 'Computers' 
    THEN order_item_id 
  END) AS Computers
 ,COUNT(CASE 
   WHEN category_name = 'Cameras' 
    THEN order_item_id 
  END) AS Cameras
 ,COUNT(CASE 
   WHEN category_name = 'Audio & Music' 
    THEN order_item_id 
  END) AS Audio_and_Music
 ,COUNT(CASE 
   WHEN category_name = 'Wearables' 
    THEN order_item_id 
  END) AS Wearables
 ,COUNT(CASE 
   WHEN category_name = 'Home Entertainment' 
    THEN order_item_id 
  END) AS Home_Entertainment
 ,COUNT(CASE 
   WHEN category_name = 'Drones' 
    THEN order_item_id 
  END) AS Drones
 ,COUNT(CASE 
   WHEN category_name = 'Smart Home' 
    THEN order_item_id 
  END) AS Smart_Home 
 ,COUNT(CASE 
   WHEN category_name = 'eMobility' 
    THEN order_item_id 
  END) AS eMobility
 ,COUNT(CASE 
   WHEN category_name = 'POS' 
    THEN order_item_id 
  END) AS POS
FROM ods_production.order_item
GROUP BY 1
)
SELECT 
  o.*
 ,a.*
FROM master."order" o
  LEFT JOIN a 
    ON a.order_id_other = o.order_id
WITH NO SCHEMA BINDING
;    