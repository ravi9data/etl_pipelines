/*
 * THIS TABLE FOCUSES ON ALL THE AUTOMATED PAYMENTS AND THEN DISTRIBUTES THE DATA INTO THE SUBSCRIPTION PAYMENTS.
 * NOTE THAT A USER WHO IS MAPPED AT THE CUSTOMER LEVEL COULD HAVE CHANGE IN ORDER ID HERE COMPARE TO DUE MATCH TABLE BECAUSE 
 IT COULD BE THAT USER HAVE PROVIDED THE WRONG ORDER ID IN THE REFERENCE OR CUSTOMER HAS GIVEN 1 ORDER ID IN REFERENCE AND AMOUNT IS ALLOCATED TO THE MULTIPLE ORDERS 
 * THE WHOLE TABLE LOGIC IS BUILT BY UNIONING 2 SCRIPTS WHICH ADDRESS DIFFERENT CASES 
         1. UNION 1 = ADDRESSES ALL THE ORDERS WITH CUSTOMER LEVEL MATCH
         2. UNION 2 = ADDRESSES ALL THE ORDERS WITH ORDER LEVEL MATCH
 */
DROP TABLE IF EXISTS payment.bnp_manual_payments_order_id_mapping;
CREATE TABLE payment.bnp_manual_payments_order_id_mapping AS
WITH match_customer_level AS (
/*PAYMENTS WHICH ARE AGGREGATED AT CUSTOMER LEVEL*/
SELECT DISTINCT customer_id
FROM payment.bnp_manual_payments_due_match_customer
WHERE amount_due_match_logic = 'CUSTOMER_LEVEL_MATCH'   
)
,combined_data AS (
/*PAYMENTS AGGREGATED AT CUSTOMER LEVEL*/
SELECT DISTINCT
  c.report_date
 ,LISTAGG(DISTINCT c.arrival_date, ',') WITHIN GROUP (ORDER BY c.customer_id) 
   OVER (PARTITION BY c.customer_id) AS arrival_date
 ,LISTAGG(DISTINCT c.booking_date, ',') WITHIN GROUP (ORDER BY c.customer_id) 
   OVER (PARTITION BY c.customer_id) AS booking_date 
 ,c.customer_id
 ,'CUSTOMER_LEVEL_MATCH' AS amount_due_match_logic
 ,'CUSTOMER_LEVEL_MATCH' AS reasons_booking_method
 ,sp.psp_reference
 ,sp.payment_sfid
 ,sp.order_id 
 ,LISTAGG(DISTINCT c.remi_remittance_information, ',') WITHIN GROUP (ORDER BY c.customer_id) 
   OVER (PARTITION BY c.customer_id) AS remi_remittance_information
 ,LISTAGG(DISTINCT c.info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_, ',') WITHIN GROUP (ORDER BY c.customer_id) 
   OVER (PARTITION BY c.customer_id) AS info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_ 
 ,LISTAGG(DISTINCT c.bank_reference, ',') WITHIN GROUP (ORDER BY c.customer_id) 
   OVER (PARTITION BY c.customer_id) AS bank_reference   
 ,c.country
 ,sp.payment_id
 ,sp.due_date
 ,c.t_manual_customer amount_transfered_customer
 ,c.t_manual_customer AS amount_transfered
 ,sp.amount_due
 ,c.customer_adjusted_amount_total
 ,CASE 
   WHEN amount_transfered_customer = sp.amount_due 
    THEN 0 
   ELSE 1 
  END AS cumulative_manual_transfer
FROM payment.bnp_manual_payments_due_match_customer c
  INNER JOIN match_customer_level mcl 
    ON c.customer_id = mcl.customer_id
  LEFT JOIN master.subscription_payment sp
    ON c.customer_id = sp.customer_id
WHERE TRUE 
  AND c.amount_due_match = 1
  AND sp.paid_date IS NULL
  AND sp.src_tbl = 'legacy'
  AND sp.amount_due <> 0
QUALIFY SUM(sp.amount_due) OVER (PARTITION BY sp.customer_id, c.remi_remittance_information ORDER BY sp.due_date 
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) <= (c.t_manual_customer + 1) 
UNION ALL
/*PAYMENTS AGGREGATED AT ORDER LEVEL*/
SELECT DISTINCT
  c.report_date
 ,LISTAGG(DISTINCT c.arrival_date, ',') WITHIN GROUP (ORDER BY c.customer_id) 
   OVER (PARTITION BY c.customer_id) AS arrival_date
 ,LISTAGG(DISTINCT c.booking_date, ',') WITHIN GROUP (ORDER BY c.customer_id) 
   OVER (PARTITION BY c.customer_id) AS booking_date   
 ,c.customer_id
 ,c.amount_due_match_logic
 ,c.reasons_booking_method
 ,sp.psp_reference 
 ,sp.payment_sfid
 ,sp.order_id 
 /*HERE REFERENCE IS NULL IN ORDER TO HAVE SINGLE RECORD PER CUSTOMER*/
 ,LISTAGG(DISTINCT c.remi_remittance_information, ',') WITHIN GROUP (ORDER BY c.order_id) 
   OVER (PARTITION BY c.order_id) AS remi_remittance_information
 ,LISTAGG(DISTINCT c.info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_, ',') WITHIN GROUP (ORDER BY c.order_id) 
   OVER (PARTITION BY c.order_id) AS info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_   
 ,LISTAGG(DISTINCT c.bank_reference, ',') WITHIN GROUP (ORDER BY c.order_id) 
   OVER (PARTITION BY c.order_id) AS bank_reference   
 ,c.country
 ,sp.payment_id
 ,sp.due_date
 ,c.t_manual_customer amount_transfered_customer
 ,c.t_manual_order AS amount_transfered
 ,sp.amount_due
 ,c.customer_adjusted_amount_total
 ,CASE 
    WHEN amount_transfered_customer = sp.amount_due 
     THEN 0 
    ELSE 1 
  END AS cumulative_manual_transfer
FROM payment.bnp_manual_payments_due_match_customer c
  LEFT JOIN master.subscription_payment sp 
    ON c.order_id = sp.order_id
  LEFT JOIN match_customer_level mcl 
    ON c.customer_id = mcl.customer_id
WHERE TRUE 
  AND c.amount_due_match = 1
  AND sp.paid_date IS NULL
  AND sp.src_tbl = 'legacy'
  AND c.amount_due_match_logic = 'ORDER_LEVEL_MATCH'
  AND mcl.customer_id IS NULL 
  AND sp.amount_due <> 0
QUALIFY SUM(sp.amount_due) OVER (PARTITION BY sp.order_id ORDER BY sp.due_date 
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) <= (c.order_amount + 1)
) 
SELECT
  report_date
 ,arrival_date
 ,booking_date 
 ,customer_id
 ,amount_due_match_logic
 ,reasons_booking_method
 ,psp_reference
 ,payment_sfid
 ,order_id
 ,remi_remittance_information
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,bank_reference
 ,country
 ,payment_id
 ,due_date
 ,customer_adjusted_amount_total::DECIMAL(10,2) / COUNT(payment_id) OVER (PARTITION BY CASE
   WHEN amount_due_match_logic = 'CUSTOMER_LEVEL_MATCH'
    THEN customer_id||remi_remittance_information
   WHEN  amount_due_match_logic = 'ORDER_LEVEL_MATCH'
    THEN order_id
  END) AS customer_adjusted_amount
 ,amount_transfered_customer 
 ,amount_transfered
 ,amount_due
 ,cumulative_manual_transfer
FROM combined_data
WHERE TRUE
QUALIFY COUNT(payment_id) OVER (PARTITION BY payment_id) = 1
;  
           
-------------------------------------------------------------------------------
-------------------------HISTORIZATION-----------------------------------------
-------------------------------------------------------------------------------
DELETE FROM payment.bnp_manual_payments_order_id_mapping_historical
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_order_id_mapping_historical
SELECT 
  report_date
 ,arrival_date
 ,booking_date   
 ,customer_id
 ,amount_due_match_logic
 ,reasons_booking_method
 ,psp_reference
 ,payment_sfid
 ,order_id
 ,remi_remittance_information
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,bank_reference
 ,country
 ,payment_id
 ,due_date
 ,customer_adjusted_amount
 ,amount_transfered_customer
 ,amount_transfered
 ,amount_due
 ,cumulative_manual_transfer
FROM payment.bnp_manual_payments_order_id_mapping
;

GRANT SELECT ON payment.bnp_manual_payments_order_id_mapping TO tableau;