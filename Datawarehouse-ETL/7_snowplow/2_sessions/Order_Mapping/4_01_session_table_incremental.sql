CREATE TEMP TABLE tmp_session_conversions AS
WITH 

new_sessions AS (
  SELECT DISTINCT
  session_id,
  customer_id
  FROM web.sessions_snowplow
  WHERE DATE(session_start) >= DATEADD(week, -1, CURRENT_DATE)    
  )

  SELECT 
    s.session_id,
    MAX(CASE WHEN ISNUMERIC(s.customer_id::TEXT) THEN s.customer_id::INTEGER
         WHEN ISNUMERIC(om.customer_id_web::TEXT) THEN om.customer_id_web::INTEGER
         WHEN om.customer_id_order THEN om.customer_id_order
         ELSE NULL
        END) AS customer_id,
    MAX(CASE WHEN cart_date IS NOT NULL AND last_touchpoint_before_submitted THEN 1 ELSE 0 END) AS is_cart,
    MAX(CASE WHEN address_orders>=1 AND last_touchpoint_before_submitted THEN 1 ELSE 0 END) AS is_address,
    MAX(CASE WHEN payment_orders>=1 AND last_touchpoint_before_submitted THEN 1 ELSE 0 END) AS is_payment,
    MAX(CASE WHEN submitted_date IS NOT NULL AND last_touchpoint_before_submitted THEN 1 ELSE 0 END) AS is_submitted,
    MAX(CASE WHEN submitted_date IS not NULL AND first_touchpoint THEN 1 ELSE 0 END) as is_submitted_first_touchpoint,
    MAX(CASE WHEN submitted_date IS NOT NULL THEN 1 ELSE 0 END) as is_submitted_any_touchpoint,
    MAX(CASE WHEN paid_date IS NOT NULL AND last_touchpoint_before_submitted THEN 1 ELSE 0 END) AS is_paid,
    MAX(CASE WHEN om.new_recurring='RECURRING' AND last_touchpoint_before_submitted THEN 1 ELSE 0 END) AS is_recurring_customer,
    LISTAGG(voucher_code, ' | ' ) AS vouchers_used
  FROM new_sessions s 
    LEFT JOIN web.session_order_mapping om 
      ON s.session_id=om.session_id 
  GROUP BY 1;

BEGIN TRANSACTION;

DELETE FROM web.session_conversions
USING tmp_session_conversions b
WHERE session_conversions.session_id = b.session_id 
;

INSERT INTO web.session_conversions
SELECT * 
FROM tmp_session_conversions
;

END TRANSACTION;

DROP TABLE tmp_session_conversions;
