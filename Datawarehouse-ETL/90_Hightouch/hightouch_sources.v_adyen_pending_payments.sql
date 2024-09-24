CREATE OR REPLACE VIEW hightouch_sources.v_adyen_pending_payments AS
SELECT 
  customer_id
 ,order_id
 ,psp_reference
 ,due_date::date
 ,payment_method_detailed
 ,status
 ,amount_du
FROM master.subscription_payment 
WHERE payment_method_detailed='Adyen, 1-click'
  AND status = 'PENDING'
WITH NO SCHEMA BINDING;