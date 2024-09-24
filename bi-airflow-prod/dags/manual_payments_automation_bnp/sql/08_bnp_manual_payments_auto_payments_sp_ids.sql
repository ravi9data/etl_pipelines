/*
 * SUMMERIZES ALL THE PAYMENTS 
 * */
DROP TABLE IF EXISTS payment.bnp_manual_payments_auto_payments_sp_ids;
CREATE TABLE payment.bnp_manual_payments_auto_payments_sp_ids AS
SELECT DISTINCT 
  CURRENT_DATE AS report_date
 ,COALESCE(sp.order_id, sf.order_id) AS order_id
 ,COALESCE(sf.customer_id, sp.customer_id) AS customer_id
 ,sf.remi_remittance_information
 ,sf.bank_reference
 ,sf.country
 ,sp.payment_sfid
 ,sf.paid_amount 
 ,sp.amount_due 
 ,sf.customer_adjusted_amount
 ,'Auto' AS booking_method
FROM payment.bnp_manual_payments_booking_on_sf sf
  LEFT JOIN master.subscription_payment sp
    ON sf.id = sp.payment_id 
;

-------------------------------------------------------------------------------
-------------------------HISTORIZATION-----------------------------------------
-------------------------------------------------------------------------------
DELETE FROM payment.bnp_manual_payments_auto_payments_sp_ids_historical
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_auto_payments_sp_ids_historical
SELECT 
  report_date
 ,order_id
 ,customer_id
 ,remi_remittance_information
 ,bank_reference
 ,country
 ,payment_sfid
 ,paid_amount
 ,amount_due
 ,customer_adjusted_amount
 ,booking_method
FROM payment.bnp_manual_payments_auto_payments_sp_ids
;

GRANT SELECT ON payment.bnp_manual_payments_auto_payments_sp_ids TO tableau;
GRANT SELECT ON payment.bnp_manual_payments_auto_payments_sp_ids_historical TO tableau;