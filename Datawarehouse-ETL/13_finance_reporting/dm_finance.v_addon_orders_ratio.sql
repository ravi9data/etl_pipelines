-- dm_finance.v_addon_orders_ratio source

CREATE VIEW dm_finance.v_addon_orders_ratio AS
WITH addon_paid_orders AS (  
    SELECT 
       date_trunc('week',paid_date)::date AS paid_date
       ,count (DISTINCT order_id) AS paid_order_with_addons
       ,country
    FROM  ods_production.addon a
    WHERE TRUE 
          AND add_on_status = 'paid'
    GROUP BY 1,3     
    ORDER BY 1
) 
,total_paid_orders AS (
    SELECT 
        date_trunc('week',o.paid_date)::date AS paid_date
       ,count (DISTINCT order_id) AS paid_order
       ,shippingcountry
    FROM ods_production.ORDER o
    WHERE TRUE 
          AND status = 'PAID'
    GROUP BY 1,3
    ORDER BY 1 
)
,total_submitted_orders AS (
    SELECT
        date_trunc('week',o.submitted_date)::date AS submitted_date
       ,count(DISTINCT o.order_id) AS submitted_order
       ,shippingcountry
    FROM ods_production.ORDER o
    WHERE TRUE 
         AND o.submitted_date IS NOT NULL
    GROUP BY 1,3     
    ORDER BY 1
)
, submitted_orders_addons AS (
    SELECT 
        date_trunc('week',submitted_date)::date AS submitted_date
       ,count(distinct order_id) AS submitted_order_with_addons
       ,country
    FROM ods_production.addon
    WHERE TRUE 
    GROUP BY 1,3      
    ORDER BY 1
 )  
SELECT 
    DISTINCT date_trunc('week',d.datum)::date AS date_
    ,sa.country AS country
    ,COALESCE(s.submitted_order,0) AS total_submitted_orders
    ,COALESCE(t.paid_order,0) AS total_paid_orders
    ,COALESCE(sa.submitted_order_with_addons,0) AS submitted_order_with_addons
    ,COALESCE(addon.paid_order_with_addons,0) AS paid_order_with_addons
FROM public.dim_dates d
LEFT JOIN submitted_orders_addons sa 
   ON sa.submitted_date = d.datum
LEFT JOIN total_submitted_orders s 
   ON d.datum = s.submitted_date
   AND sa.country =s.shippingcountry
LEFT JOIN addon_paid_orders addon
   ON addon.paid_date = d.datum
   AND addon.country=sa.country
LEFT JOIN total_paid_orders t 
   ON d.datum = t.paid_date
   AND sa.country=t.shippingcountry
WHERE d.datum::date between dateadd(week,-12,date_trunc ('week',current_date))::date AND current_date
AND week_day_number=1
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA BINDING;
