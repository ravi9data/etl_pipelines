DROP TABLE IF EXISTS skyvia.marketing_member_get_member;
CREATE TABLE skyvia.marketing_member_get_member AS

SELECT DISTINCT
  c.customer_id AS "referrer_user_id"
  ,CASE
    WHEN MIN(s2.status) = 'ACTIVE'
     THEN 'TRUE'
    ELSE 'FALSE'
   END AS has_referrer_an_active_subscription
  ,DECODE(MAX(CASE
    WHEN pay.paid_date IS NULL AND pay.due_date::DATE < CURRENT_DATE
     THEN 1
    ELSE 0
   END), 1, 'TRUE', 0, 'FALSE' ) AS has_open_payment
  ,o.customer_id AS "referee_user_id"
  ,c.referral_code
  ,o.order_id
  ,o.status AS order_status
  ,TO_CHAR(a.delivered_at::timestamp, 'yyyy-mm-dd HH24:MI:SS') AS delivered_at
   --revocation period expires 14 days after delivery of product at midnight
  ,TO_CHAR(DATEADD(SECOND, -1, DATEADD(DAY, 15, a.delivered_at::DATE))::timestamp, 'yyyy-mm-dd HH24:MI:SS') AS revocation_period_end
  ,DECODE(revocation_period_end < CURRENT_TIMESTAMP, TRUE, 'TRUE', FALSE, 'FALSE')  AS revocation_period_expired
FROM ods_production.customer c
  LEFT JOIN ods_production.order o
    ON c.referral_code = o.voucher_code
  LEFT JOIN ods_production.allocation a
    ON o.order_id = a.order_id
  LEFT JOIN ods_production.subscription s
    ON a.subscription_id = s.subscription_id
  LEFT JOIN ods_production.subscription s2
    ON c.customer_id = s2.customer_id
  LEFT JOIN ods_production.order o2
    ON c.customer_id = o2.customer_id
  LEFT JOIN master.subscription_payment pay
    ON o2.order_id = pay.order_id
    AND pay.status IN ('FAILED', 'FAILED FULLY')
WHERE TRUE
  AND o.is_special_voucher = 'Referals: Refer a friend Voucher'
  AND a.delivered_at IS NOT NULL
--THIS IS THE CAMPAIGN START DATE
  AND o.created_date >= '2021-09-15 13:00:00'
GROUP BY 1,4,5,6,7,8,9,10
ORDER BY delivered_at DESC
;

GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO skyvia;
