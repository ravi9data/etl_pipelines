SELECT order_id       AS "order id",
       'de_sales_mcc' AS "conversion name",
       'RETRACT'      AS "adjustment type",
       CAST(DATE_ADD('hour', +3, submitted_date::timestamp) AS VARCHAR) +
       CASE
           WHEN store_country = 'Spain'
               THEN ' Europe/Madrid'
           WHEN store_country = 'Germany'
               THEN ' Europe/Berlin'
           WHEN store_country = 'Netherlands'
               THEN ' Europe/Amsterdam'
           WHEN store_country = 'Austria'
               THEN ' Europe/Vienna'
           ELSE ' Europe/Berlin'
           END
                      AS "adjustment time"
FROM master.order
WHERE "status" in ('DECLINED', 'CANCELLED', 'FAILED FIRST PAYMENT')
  AND submitted_date::DATE >= DATE_ADD('day', - 30, GETDATE())
  AND submitted_date::DATE < DATE_ADD('day', - 1, GETDATE())
  AND store_id != 621;
