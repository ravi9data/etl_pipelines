CREATE VIEW dm_marketing.v_referral_voucher AS
SELECT DISTINCT
  c.customer_id AS "referrer_user_id"
  ,o.customer_id AS "referee_user_id"
  ,c.referral_code
  ,o.order_id
  ,o.created_date AS order_created_date
  ,o.submitted_date AS order_submitted_date
  ,o.paid_date AS order_paid_date
  ,o.status AS order_status
  ,s.subscription_id
  ,a.delivered_at
FROM ods_production.customer c
  LEFT JOIN ods_production.order o
    ON c.referral_code = o.voucher_code
  LEFT JOIN ods_production.allocation a
    ON o.order_id = a.order_id
  LEFT JOIN ods_production.subscription s
    ON a.subscription_id = s.subscription_id
WHERE TRUE
  AND o.is_special_voucher = 'Referals: Refer a friend Voucher'
WITH NO SCHEMA BINDING;
;