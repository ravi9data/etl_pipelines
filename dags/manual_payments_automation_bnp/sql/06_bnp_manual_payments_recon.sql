/*
* THIS TABLE COMBINES BOTH THE ORDER ID MAPPING AND MANUAL REVIEW CASES 
AND PAYMENTS ARE SEGGREGATED INTO AUTOMATED AND MANUAL
*/
DROP TABLE IF EXISTS payment.bnp_manual_payments_recon;
CREATE TABLE payment.bnp_manual_payments_recon AS
SELECT DISTINCT 
  o.report_date
 ,o.arrival_date
 ,o.booking_date 
 ,o.customer_id  
 ,o.order_id
 ,o.amount_due_match_logic 
 ,o.country
 ,d.remi_remittance_information
 ,o.info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_ 
 ,o.bank_reference
 ,'AUTOMATED' AS booking_method
 ,CASE 
   WHEN d.amount_due_match_logic = 'CUSTOMER_LEVEL_MATCH'
    THEN d.t_manual_customer
   ELSE d.t_manual_order
  END AS transferred_amount
 ,o.customer_adjusted_amount
 ,SUM(o.amount_due) AS amount_due
FROM payment.bnp_manual_payments_order_id_mapping AS o
  LEFT JOIN payment.bnp_manual_payments_due_match_customer AS d
    ON CASE 
     WHEN d.amount_due_match_logic = 'CUSTOMER_LEVEL_MATCH' 
      THEN o.customer_id = d.customer_id
     ELSE o.order_id = d.order_id 
    END 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
UNION ALL
SELECT 
  report_date
 ,arrival_date::TEXT
 ,booking_date::TEXT
 ,customer_id  
 ,order_id   
 ,amount_due_match_logic 
 ,country
 ,remi_remittance_information
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,bank_reference
 ,CASE 
   WHEN amount_due_match_logic = 'DC_PAYMENTS' 
    THEN 'DC_PAYMENTS' 
   ELSE 'MANUAL'
  END AS booking_method
 ,SUM(amount) AS transfered_amount
 ,0 AS customer_adjusted_amount
 ,0 AS amount_due
FROM payment.bnp_manual_payments_manual_review 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
;
-------------------------------------------------------------------------------
-------------------------HISTORIZATION-----------------------------------------
-------------------------------------------------------------------------------
DELETE FROM payment.bnp_manual_payments_recon_historical
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_recon_historical
SELECT 
  report_date
 ,arrival_date
 ,booking_date 
 ,customer_id
 ,order_id
 ,amount_due_match_logic
 ,country
 ,remi_remittance_information
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_ 
 ,bank_reference
 ,booking_method
 ,transferred_amount
 ,customer_adjusted_amount
 ,amount_due
FROM payment.bnp_manual_payments_recon
; 

GRANT SELECT ON payment.bnp_manual_payments_recon TO tableau;
GRANT SELECT ON payment.bnp_manual_payments_recon_historical TO tableau;