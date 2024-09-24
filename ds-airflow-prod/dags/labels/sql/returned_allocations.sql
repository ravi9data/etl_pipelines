SELECT a.customer_id::int,
       sum(CASE
               WHEN a.allocation_status_original = 'RETURNED' THEN 1
               ELSE 0
           END) = count(*) AS is_returned
FROM ods_production.allocation a
LEFT JOIN ods_production.subscription s ON a.subscription_id = s.subscription_id
WHERE allocation_status_original IN ('RETURNED',
                                     'DELIVERED')
    AND s.status = 'ACTIVE'
GROUP BY 1;
