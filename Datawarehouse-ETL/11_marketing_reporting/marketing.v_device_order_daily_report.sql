DROP VIEW IF EXISTS marketing.v_device_order_daily_report;
CREATE VIEW marketing.v_device_order_daily_report AS
SELECT 
    oc.os ,
    oc.browser ,
    o.device ,
    o.new_recurring ,
    o.store_country AS country ,
    o.created_date::date AS created_date ,
    COUNT(DISTINCT oc.order_id) AS num_orders ,
    SUM(CASE WHEN o.submitted_date IS NOT NULL THEN 1 ELSE 0 END) AS num_submitted ,
    SUM(CASE WHEN o.paid_date IS NOT NULL THEN 1 ELSE 0 END) AS num_paid
FROM traffic.order_conversions oc
INNER JOIN master."order" o
  ON o.order_id = oc.order_id
WHERE o.created_date BETWEEN DATEADD('year', -2, CURRENT_DATE) AND DATEADD('day', -1, CURRENT_DATE)
GROUP BY 1,2,3,4,5,o.created_date::date
WITH NO SCHEMA BINDING;  