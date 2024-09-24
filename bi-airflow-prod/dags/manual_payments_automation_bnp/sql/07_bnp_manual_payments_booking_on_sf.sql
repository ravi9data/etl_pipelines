/*
 * THIS TABLE IS THE SOURCE ALL AUTOMATED PAYMENTS ARE BEING PUSHED TO SF
 * */
DROP TABLE IF EXISTS payment.bnp_manual_payments_booking_on_sf;
CREATE TABLE payment.bnp_manual_payments_booking_on_sf AS
SELECT DISTINCT 
  report_date
 ,payment_id AS id
 ,CURRENT_DATE AS paid_date
 ,'PAID' AS status
 ,amount_due AS paid_amount
 ,customer_adjusted_amount
 ,order_id
 ,customer_id
 ,remi_remittance_information
 ,bank_reference
 ,country
 ,'Manual Transfer' AS paid_reason
 ,'Production' AS sf_environment
FROM payment.bnp_manual_payments_order_id_mapping 
;

-------------------------------------------------------------------------------
-------------------------HISTORIZATION-----------------------------------------
-------------------------------------------------------------------------------
DELETE FROM payment.bnp_manual_payments_booking_on_sf_historical 
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_booking_on_sf_historical 
SELECT 
  report_date
 ,id
 ,paid_date
 ,status
 ,paid_amount
 ,customer_adjusted_amount
 ,order_id
 ,customer_id
 ,remi_remittance_information
 ,bank_reference
 ,country
 ,paid_reason
 ,sf_environment
FROM payment.bnp_manual_payments_booking_on_sf
;

GRANT SELECT ON payment.bnp_manual_payments_booking_on_sf TO tableau;
GRANT SELECT ON payment.bnp_manual_payments_booking_on_sf_historical TO tableau;