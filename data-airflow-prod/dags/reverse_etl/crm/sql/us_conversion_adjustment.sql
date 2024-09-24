SELECT order_id        AS "order id",
       'Purchase - US' AS "conversion name",
       'RETRACT'       AS "adjustment type",
       CAST(DATE_ADD('hour', +12, submitted_date::timestamp) AS VARCHAR) + ' America/Los_Angeles'
                       AS "adjustment time"
FROM master.order
WHERE "status" IN ('DECLINED', 'CANCELLED', 'FAILED FIRST PAYMENT')
  AND submitted_date::DATE >= DATE_ADD('day', - 30, GETDATE())
  AND submitted_date::DATE < DATE_ADD('day', - 1, GETDATE())
  AND store_id = 621;
