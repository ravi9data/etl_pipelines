/*
 * THIS TABLE MAPS THE CUSTOMER NAME AND RECONCILES IT WITH THE SYSTEM NAME FROM CUSTOMER PII USING LEVENSHTEIN_DISTANCE.
 * IT ALSO CREATES A FLAG CALLED 'pass_through' WHICH CLASSIFIES THE ENTRIES BASED ON THE AVAILABLITY OF THE DATA  
 */
DROP TABLE IF EXISTS payment.bnp_manual_payments_orders;
CREATE TABLE payment.bnp_manual_payments_orders AS
SELECT
  a.report_date
/*THIS IS WHEN FUNDS ARE POSTED TO AN ACCOUNT AND AVAILABLE FOR IMMEDIATE USE*/ 
 ,a.value_date AS arrival_date  
 ,a.booking_date
 ,cp.customer_id
 ,a.order_id
 ,a.amount
 ,a.customer_name
 ,a.confidence_on_order_id
 ,is_dc_case
 ,a.remi_remittance_information
 ,a.bank_reference
 ,a.info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,a.country
 ,UPPER(cp.first_name || ' ' || cp.last_name) AS customer_name_system
 ,a.customer_name AS reported_customer_name
 ,CASE
   WHEN UPPER(cp.first_name) = SPLIT_PART(a.customer_name, ' ', 1)
    THEN 1
   ELSE 0
  END AS fl_first_name_match
 ,CASE
   WHEN UPPER(cp.last_name) = UPPER(SPLIT_PART(a.customer_name, ' ', REGEXP_COUNT(a.customer_name, ' ') + 1))
    THEN 1
   ELSE 0
  END AS fl_last_name_match
 ,CASE
   WHEN UPPER(reported_customer_name) = UPPER(customer_name_system)
    THEN 1
   ELSE 0
  END AS fl_name_match
 ,LEVENSHTEIN(reported_customer_name, customer_name_system) AS levenshtein_distance_name
 ,CASE
   WHEN a.customer_id IS NULL
    THEN 'CUSTOMER_ID_NOT FOUND'
   WHEN a.confidence_on_order_id = 'HIGH'
     AND levenshtein_distance_name = 0
    THEN 'CNF_HIGH_LEV0'
   WHEN a.confidence_on_order_id = 'HIGH'
     AND (fl_last_name_match = 1
       OR fl_first_name_match = 1) 
    THEN 'CNF_HIGH_NAME_PARTIAL_MATCH'
   WHEN a.confidence_on_order_id = 'HIGH' 
    THEN 'CNF_HIGH_NO_NAME_MATCH'
   WHEN a.confidence_on_order_id = 'MEDIUM'
     AND levenshtein_distance_name = 0
    THEN 'CNF_MEDIUM_NAME_MATCH'
   WHEN a.confidence_on_order_id = 'MEDIUM'
     AND (fl_last_name_match = 1
       OR fl_first_name_match = 1) 
    THEN 'CNF_MEDIUM_NAME_PARTIAL_MATCH'
   WHEN a.confidence_on_order_id = 'MEDIUM'
    THEN 'CNF_MEDIUM_NO_NAME_MATCH'
   WHEN a.confidence_on_order_id = 'LOW'
    THEN 'NO_ORDER_ID_FOUND'
   ELSE 'ERROR'
  END AS pass_through
/*NUMBER OF PAYMENTS PER ORDER ID*/  
 ,COUNT(a.remi_remittance_information) OVER (PARTITION BY a.order_id) AS no_of_payments_per_orders  
/*IDX OF PAYMENTS UNDER ONE ORDER_ID*/ 
 ,ROW_NUMBER() OVER (PARTITION BY a.order_id) AS idx_payments_per_orders
/*NUMBER OF PAYMENTS PER REFERENCE*/ 
 ,COUNT(a.remi_remittance_information) OVER (PARTITION BY a.remi_remittance_information) AS no_of_payments_per_reference
/*IDX OF PAYMENTS UNDER ONE ORDER_ID*/
 ,ROW_NUMBER() OVER (PARTITION BY a.order_id) AS idx_payments_per_reference
/*NUMBER OF PAYMENTS UNDER ONE CUSTOMER_ID*/ 
 ,COUNT(a.customer_id) OVER (PARTITION BY a.customer_id) AS no_of_payments_per_customer
FROM payment.bnp_manual_payments_raw a
  LEFT JOIN ods_data_sensitive.customer_pii cp 
    ON a.customer_id = cp.customer_id
;

-------------------------------------------------------------------------------
-------------------------HISTORIZATION-----------------------------------------
-------------------------------------------------------------------------------
DELETE FROM payment.bnp_manual_payments_orders_historical
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_orders_historical
SELECT 
  report_date
 ,arrival_date
 ,booking_date
 ,customer_id
 ,order_id
 ,amount
 ,customer_name
 ,confidence_on_order_id
 ,is_dc_case
 ,remi_remittance_information
 ,bank_reference
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,country
 ,customer_name_system
 ,reported_customer_name
 ,fl_first_name_match
 ,fl_last_name_match
 ,fl_name_match
 ,levenshtein_distance_name
 ,pass_through
 ,no_of_payments_per_orders
 ,idx_payments_per_orders
 ,no_of_payments_per_reference
 ,idx_payments_per_reference
 ,no_of_payments_per_customer
FROM payment.bnp_manual_payments_orders
;

GRANT SELECT ON payment.bnp_manual_payments_orders TO tableau;