DROP TABLE IF EXISTS staging.global_order_alerts;
CREATE TABLE staging.global_order_alerts AS
WITH global AS (
    SELECT submitted_date::DATE,
           day_is_weekday,
           COUNT(CASE WHEN completed_orders >= 1 THEN order_id END) AS daily_submitted,
           COUNT(CASE WHEN paid_orders >= 1 THEN order_id END) AS daily_paid,
           daily_paid/daily_submitted::FLOAT AS conversion_rate,
           AVG(daily_submitted) OVER(PARTITION BY  day_is_weekday) AS avg_submitted_orders,
           AVG(daily_paid) OVER(PARTITION BY  day_is_weekday) AS avg_paid_orders,
           daily_submitted / avg_submitted_orders::FLOAT AS ratio_submitted,
           daily_paid / avg_paid_orders::FLOAT AS ratio_paid
    FROM master."order" a 
             LEFT JOIN public.dim_dates b ON submitted_date::DATE = datum
    WHERE completed_orders >=1
      AND submitted_date::DATE >= CURRENT_DATE - 14
    GROUP BY 1,2
    ORDER BY 1)

SELECT *,
       CASE WHEN ratio_paid < 0.8 THEN TRUE ELSE FALSE END AS is_low_paid_number,
       CASE WHEN ratio_submitted < 0.8 THEN TRUE ELSE FALSE END AS is_low_submitted_number
FROM global;
