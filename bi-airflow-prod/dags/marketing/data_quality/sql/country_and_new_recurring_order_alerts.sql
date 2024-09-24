DROP TABLE IF EXISTS staging.country_and_new_recurring_order_alerts;
CREATE TABLE staging.country_and_new_recurring_order_alerts AS
WITH country_new_recurring as (
SELECT submitted_date::DATE,
       store_country,
       new_recurring,
       day_is_weekday,
       COUNT(CASE WHEN completed_orders >= 1 THEN order_id END) AS daily_submitted,
       COUNT(CASE WHEN paid_orders >= 1 THEN order_id END) AS daily_paid,
       daily_paid/daily_submitted::float AS conversion_rate,
       AVG(daily_submitted) OVER(PARTITION BY store_country,new_recurring, day_is_weekday) AS avg_submitted_orders,
       AVG(daily_paid) OVER(PARTITION BY store_country,new_recurring, day_is_weekday) as AVG_PAID_ORDERS,
       daily_submitted / avg_submitted_orders::float AS ratio_submitted,
       daily_paid / avg_paid_orders::float AS ratio_paid
FROM master."order" a
         LEFT JOIN public.dim_dates b ON submitted_date::DATE = datum
WHERE completed_orders >=1
  AND submitted_date::DATE >= current_date - 14
GROUP BY 1,2,3,4
ORDER BY 2,3,1)

SELECT *, 
       CASE WHEN daily_submitted BETWEEN 0 AND 50 THEN 0.6
       ELSE 0.7 END AS submitted_threshold,
       CASE WHEN daily_paid BETWEEN 0 AND 10 THEN 0.5
            WHEN daily_paid BETWEEN 11 AND 20 THEN 0.6
       ELSE 0.7 END AS paid_threshold,
       CASE WHEN ratio_paid < paid_threshold THEN TRUE ELSE FALSE END AS is_low_paid_number,
       CASE WHEN ratio_submitted < submitted_threshold THEN TRUE ELSE FALSE END AS is_low_submitted_number
FROM country_new_recurring;