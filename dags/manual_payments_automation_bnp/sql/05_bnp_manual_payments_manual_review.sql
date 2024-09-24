/*
 * THIS TABLE FOCUSES ON ALL THE MANUAL PAYMENTS, WHERE PAID AMOUNT EITHER 
 DOES NOT MATCH THE DUE AMOUNT OR CUSTOMER PROVIDED REFERENCE IS INVALID 
 */
DROP TABLE IF EXISTS payment.bnp_manual_payments_manual_review;
CREATE TABLE payment.bnp_manual_payments_manual_review AS
WITH exclusion_list_order_level AS (
SELECT DISTINCT order_id 
FROM payment.bnp_manual_payments_order_id_mapping
WHERE amount_due_match_logic = 'ORDER_LEVEL_MATCH'
)
,exclusion_list_customer_level AS (
SELECT DISTINCT customer_id 
FROM payment.bnp_manual_payments_order_id_mapping
WHERE amount_due_match_logic = 'CUSTOMER_LEVEL_MATCH'
)
SELECT
  a.report_date
 ,a.arrival_date
 ,a.booking_date
 ,a.customer_id
 ,a.order_id
 ,a.remi_remittance_information
 ,a.info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_ 
 ,a.bank_reference
 ,a.country
 ,COALESCE(due.amount_due_match_logic, CASE
   WHEN a.is_dc_case IS TRUE 
     THEN 'DC_PAYMENTS'
  END) AS amount_due_match_logic  
 ,a.confidence_on_order_id
 ,a.customer_name
 ,SUM(a.amount) AS amount
FROM payment.bnp_manual_payments_orders a
  LEFT JOIN payment.bnp_manual_payments_due_match_customer AS due
    ON a.remi_remittance_information = due.remi_remittance_information
  LEFT JOIN exclusion_list_order_level excl_ord
    ON a.order_id = excl_ord.order_id
  LEFT JOIN exclusion_list_customer_level excl_cus
    ON a.customer_id = excl_cus.customer_id  
WHERE TRUE
  AND (
       due.amount_due_match_logic = 'NO_LOGIC' 
   OR (excl_ord.order_id IS NULL AND excl_cus.customer_id IS NULL)
   OR  a.pass_through IN ('ERROR','NO_ORDER_ID_FOUND','CUSTOMER_ID_NOT FOUND')
      ) 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

-------------------------------------------------------------------------------
-------------------------HISTORIZATION-----------------------------------------
-------------------------------------------------------------------------------
DELETE FROM payment.bnp_manual_payments_manual_review_historical
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_manual_review_historical
SELECT 
  report_date
 ,arrival_date
 ,booking_date
 ,customer_id
 ,order_id
 ,remi_remittance_information
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_ 
 ,bank_reference
 ,country
 ,amount_due_match_logic
 ,confidence_on_order_id
 ,customer_name
 ,amount
FROM payment.bnp_manual_payments_manual_review
;

GRANT SELECT ON payment.bnp_manual_payments_manual_review TO tableau;
GRANT SELECT ON payment.bnp_manual_payments_manual_review_historical TO tableau;