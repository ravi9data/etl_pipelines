/*
 * THIS TABLE APPLIES A LOGIC BASED ON CONSIDERING THE PAYMENT MADE AND THE AMOUNT DUE IN THE PAYMENTS TABLE.
 * NOTE WE DO NOT TAKE 'ASSET' PAYMENTS INTO CONSIDERATION.
 * IF A PAYMENT IS DONE, FIRST IT IS ASSIGNED TO A FAILED PAYMENTS AND THEN TO A PLANNED PAYMENT
 * PASS_THROUGH WITH FOLLOWING CONDITIONS ARE EXCLUDED: ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')  
 *  IN CASE OF SUBSCRIPTION PAYMENTS 
         PAID DATE SHOULD BE NULL 
         EXCLUDE ORDERS AND CUSTOMERS WITH NEW INFRA PAYMENTS
*/
DROP TABLE IF EXISTS payment.bnp_manual_payments_due_match_customer;
CREATE TABLE payment.bnp_manual_payments_due_match_customer AS
WITH double_reference AS ( 
SELECT DISTINCT
/*SINGLE PAYMENT UNDER ONE REFERENCE / ONE ORDER*/
  MAX(report_date) OVER (PARTITION BY order_id, remi_remittance_information) AS report_date
 ,MAX(arrival_date) OVER (PARTITION BY order_id, remi_remittance_information) AS arrival_date
 ,MAX(booking_date) OVER (PARTITION BY order_id, remi_remittance_information) AS booking_date 
 ,customer_id
 ,order_id
 ,is_dc_case
 ,remi_remittance_information AS reference_new
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,country
 ,bank_reference
 ,no_of_payments_per_orders 
 ,idx_payments_per_orders
 ,no_of_payments_per_reference
 ,idx_payments_per_reference
 ,SUM(amount) OVER (PARTITION BY order_id, remi_remittance_information) AS t_manual_order
 ,SUM(amount) OVER (PARTITION BY customer_id) AS t_manual_customer
 ,SUM(amount) OVER (PARTITION BY order_id) AS order_amount
FROM payment.bnp_manual_payments_orders
WHERE TRUE
  AND no_of_payments_per_reference = 1
  AND no_of_payments_per_orders = 1
  AND pass_through NOT IN ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')
  AND order_id IS NOT NULL 
UNION ALL 
---ALL payments WHERE there ARE 2 UNIQUE reference 
SELECT DISTINCT
/*SINGLE PAYMENT UNDER ONE REFERENCE / MULTIPLE ORDERS*/
  MAX(report_date) OVER (PARTITION BY order_id, remi_remittance_information) AS report_date
 ,MAX(arrival_date) OVER (PARTITION BY order_id, remi_remittance_information) AS arrival_date
 ,MAX(booking_date) OVER (PARTITION BY order_id, remi_remittance_information) AS booking_date 
 ,customer_id
 ,order_id
 ,is_dc_case 
 ,LISTAGG(DISTINCT remi_remittance_information,',') WITHIN GROUP (ORDER BY order_id) OVER (PARTITION BY order_id) AS reference_new
 ,LISTAGG(DISTINCT info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_, ',') 
   WITHIN GROUP (ORDER BY order_id) OVER (PARTITION BY order_id) AS info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,country 
 ,LISTAGG(DISTINCT bank_reference,',') WITHIN GROUP (ORDER BY order_id) OVER (PARTITION BY order_id) AS bank_reference
 ,no_of_payments_per_orders
 ,1 AS idx_payments_per_orders
 ,no_of_payments_per_reference
 ,1 AS idx_payments_per_reference
 ,SUM(amount) OVER (PARTITION BY order_id) AS t_manual_order--since we ARE summing up 2 reference payments UNDER one orde
 ,SUM(amount) OVER (PARTITION BY customer_id) AS t_manual_customer
 ,SUM(amount) OVER (PARTITION BY order_id) AS order_amount
FROM payment.bnp_manual_payments_orders
WHERE TRUE
  AND no_of_payments_per_reference = 1
  AND no_of_payments_per_orders <> 1
  AND pass_through NOT IN ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')
  AND order_id IS NOT NULL 
UNION ALL 
SELECT DISTINCT
/*SINGLE PAYMENT UNDER ONE REFERENCE / NO ORDER MATCH*/
  MAX(report_date) OVER (PARTITION BY order_id, remi_remittance_information) AS report_date
 ,MAX(arrival_date) OVER (PARTITION BY order_id, remi_remittance_information) AS arrival_date
 ,MAX(booking_date) OVER (PARTITION BY order_id, remi_remittance_information) AS booking_date 
 ,customer_id
 ,order_id
 ,is_dc_case 
 ,remi_remittance_information AS reference_new
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,country
 ,bank_reference
 ,no_of_payments_per_orders
 ,idx_payments_per_orders
 ,no_of_payments_per_reference
 ,idx_payments_per_reference
 ,SUM(amount) OVER (PARTITION BY order_id, remi_remittance_information) AS t_manual_order
 ,SUM(amount) OVER (PARTITION BY customer_id) AS t_manual_customer
 ,SUM(amount) OVER (PARTITION BY order_id) AS order_amount
FROM payment.bnp_manual_payments_orders
WHERE TRUE
  AND no_of_payments_per_reference = 1
  AND pass_through NOT IN ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')
  AND order_id IS NULL 
UNION ALL
SELECT DISTINCT
/*MULTIPLE PAYMENTS UNDER SAME REFERENCE WITH ORDER MATCH*/
  MAX(report_date) OVER (PARTITION BY order_id, remi_remittance_information) AS report_date
 ,MAX(arrival_date) OVER (PARTITION BY order_id, remi_remittance_information) AS arrival_date
 ,MAX(booking_date) OVER (PARTITION BY order_id, remi_remittance_information) AS booking_date 
 ,customer_id
 ,order_id
 ,is_dc_case 
 ,remi_remittance_information AS reference_new
 ,LISTAGG(DISTINCT info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_, ',') WITHIN GROUP (ORDER BY order_id) 
   OVER (PARTITION BY order_id) AS info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,country
 ,LISTAGG(DISTINCT bank_reference,',') WITHIN GROUP (ORDER BY order_id) 
   OVER (PARTITION BY order_id) AS bank_reference
 ,1 AS no_of_payments_per_orders
 ,1 AS idx_payments_per_orders
 ,1 AS no_of_payments_per_reference
 ,1 AS idx_payments_per_reference
 ,SUM(amount) OVER (PARTITION BY order_id, remi_remittance_information) AS t_manual_order
 ,SUM(amount) OVER (PARTITION BY customer_id) AS t_manual_customer
 ,SUM(amount) OVER (PARTITION BY order_id) AS order_amount
FROM payment.bnp_manual_payments_orders
WHERE TRUE
  AND no_of_payments_per_reference <> 1
  AND pass_through NOT IN ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')
  AND order_id IS NOT NULL 
UNION ALL 
SELECT DISTINCT
/*MULTIPLE PAYMENTS UNDER SAME REFERENCE / NO ORDER MATCH */     
  MAX(report_date) OVER (PARTITION BY order_id, remi_remittance_information) AS report_date
 ,MAX(arrival_date) OVER (PARTITION BY order_id, remi_remittance_information) AS arrival_date
 ,MAX(booking_date) OVER (PARTITION BY order_id, remi_remittance_information) AS booking_date 
 ,customer_id
 ,order_id
 ,is_dc_case 
 ,remi_remittance_information AS reference_new
 ,LISTAGG(DISTINCT info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_, ',') WITHIN GROUP (ORDER BY order_id) 
   OVER (PARTITION BY order_id) AS info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_ 
 ,country
 ,LISTAGG(DISTINCT bank_reference,',') WITHIN GROUP (ORDER BY order_id) 
   OVER (PARTITION BY remi_remittance_information) AS bank_reference
 ,1 AS no_of_payments_per_orders
 ,1 AS idx_payments_per_orders
 ,1 AS no_of_payments_per_reference
 ,1 AS idx_payments_per_reference
 ,SUM(amount) OVER (PARTITION BY order_id, remi_remittance_information) AS t_manual_order
 ,SUM(amount) OVER (PARTITION BY customer_id) AS t_manual_customer
 ,SUM(amount) OVER (PARTITION BY order_id) AS order_amount
FROM payment.bnp_manual_payments_orders
WHERE TRUE
  AND no_of_payments_per_reference <> 1
  AND pass_through NOT IN ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')
  AND order_id IS NULL 
)
,in_dc_orders AS (
SELECT DISTINCT order_id
FROM master.subscription
WHERE TRUE 
  AND cancellation_reason_new IN ('DEBT COLLECTION')
  AND status = 'CANCELLED'
)
,in_dc_customer AS (
SELECT DISTINCT customer_id
FROM master.subscription
WHERE TRUE 
  AND cancellation_reason_new IN ('DEBT COLLECTION')
  AND status = 'CANCELLED'
)
,amount_due_orders_base AS (
/*FOR ALL ORDER PAYMENTS*/    
SELECT DISTINCT 
  sp.order_id
 ,dr.t_manual_order
 ,SUM(sp.amount_due) OVER (PARTITION BY sp.order_id ORDER BY sp.due_date 
   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amt_due_order
 ,SUM(sp.amount_due) OVER (PARTITION BY sp.order_id) AS full_order_amt
 ,COUNT(sp.payment_id) OVER (PARTITION BY sp.order_id) AS num_order_payments
 ,COUNT(sp.payment_id) OVER (PARTITION BY sp.customer_id) AS num_customer_payments
FROM master.subscription_payment AS sp
  INNER JOIN double_reference dr 
    ON sp.order_id = dr.order_id
WHERE TRUE 
  AND sp.paid_date IS NULL
  AND sp.src_tbl = 'legacy'
QUALIFY amt_due_order <= (dr.t_manual_order + 1) 
)
,amount_due_order AS (
SELECT 
  *
 ,ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amt_due_order DESC)
FROM amount_due_orders_base
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amt_due_order DESC ) = 1
)
,amount_due_customer_base AS (
/*FOR ALL CUSTOMER LEVEL PAYMENTS*/    
SELECT DISTINCT 
  sp.customer_id
 ,SUM(sp.amount_due) OVER (PARTITION BY sp.customer_id ORDER BY sp.due_date 
   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS amt_due_customer
 ,SUM(sp.amount_due) OVER (PARTITION BY sp.customer_id ) AS full_amt_due_customer
 ,COUNT(sp.payment_id) OVER (PARTITION BY sp.order_id ORDER BY sp.due_date 
   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS num_order_payments
 ,COUNT(sp.payment_id) OVER (PARTITION BY sp.customer_id ORDER BY sp.due_date 
   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS num_customer_payments
FROM master.subscription_payment AS sp
  INNER JOIN double_reference dr 
    ON sp.customer_id = dr.customer_id
WHERE TRUE
  AND sp.paid_date IS NULL
  AND sp.src_tbl = 'legacy'
QUALIFY amt_due_customer <= (dr.t_manual_customer + 1) 
)
,amount_due_customer AS (
SELECT *
FROM amount_due_customer_base
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amt_due_customer DESC) = 1
)
,new_infra_payments_orders AS (
/*TO ISOLATE ORDERS WITH NEW INFRA PAYMENTS*/    
SELECT DISTINCT order_id
FROM master.subscription_payment
WHERE TRUE 
  AND paid_date IS NULL
  AND src_tbl IN ('billing_service', 'ledger')
)
,new_infra_payments_customers AS (
/*TO ISOLATE CUSTOMERS WITH NEW INFRA PAYMENTS*/    
SELECT DISTINCT customer_id
FROM master.subscription_payment
WHERE TRUE
  AND paid_date IS NULL
  AND src_tbl IN ('billing_service', 'ledger') 
)
,asset_payment AS (
/*TO ISOLATE PAYMENTS WITH ASSET_PAYMENTS*/    
SELECT 
  order_id
 ,customer_id
 ,SUM(amount) OVER (PARTITION BY order_id) AS asset_amount
FROM master.asset_payment
WHERE TRUE
  AND paid_date IS NULL 
  AND payment_type = 'CUSTOMER BOUGHT'
)
,final AS (
SELECT DISTINCT
  mt.*
 ,ad.amt_due_order
 ,COALESCE(ad.full_order_amt) AS full_order_amt
 ,CASE 
   WHEN ido.order_id IS NOT NULL
     OR idc.customer_id IS NOT NULL
     OR mt.is_dc_case IS TRUE
    THEN TRUE 
   ELSE FALSE 
  END AS is_dc_case_
 ,COALESCE(adc.amt_due_customer, 0) AS amt_due_customer
 ,COALESCE(adc.full_amt_due_customer, 0) AS full_amt_due_customer
 ,COALESCE(ad.num_order_payments, 0) AS num_order_payments
 ,COALESCE(adc.num_customer_payments, 0) AS num_customer_payments
 ,COALESCE(ap.asset_amount, 0) AS asset_amount
 ,CASE
   WHEN nio.order_id IS NOT NULL 
     OR nic.customer_id IS NOT NULL 
    THEN TRUE 
   ELSE FALSE
  END is_new_infra_payments  
FROM double_reference mt
  LEFT JOIN amount_due_order ad 
    ON mt.order_id = ad.order_id
  LEFT JOIN amount_due_customer adc 
    ON mt.customer_id = adc.customer_id
  LEFT JOIN asset_payment ap 
    ON mt.order_id = ap.order_id
  LEFT JOIN new_infra_payments_orders nio 
    ON mt.order_id = nio.order_id
  LEFT JOIN new_infra_payments_customers nic
    ON mt.customer_id = nic.customer_id
  LEFT JOIN in_dc_orders ido 
    ON mt.order_id = ido.order_id 
  LEFT JOIN in_dc_customer idc
    ON mt.customer_id = idc.customer_id
)
,a AS (
SELECT 
 *
 /*ALL VALID PAYMENTS ARE FLAGGED AS 1 ELSE 0*/
 ,CASE
   WHEN is_dc_case_ IS TRUE 
    THEN '0'
   WHEN ABS(t_manual_customer::DECIMAL(30, 2) - amt_due_customer::DECIMAL(30, 2)) <= 1
     OR ABS(t_manual_order::DECIMAL(30, 2) - full_amt_due_customer::DECIMAL(30, 2)) <= 1
    THEN 1   
   WHEN ABS(t_manual_order::DECIMAL(30, 2) - amt_due_order::DECIMAL(30, 2)) <= 1 
    OR ABS(t_manual_order::DECIMAL(30, 2) - full_order_amt::DECIMAL(30, 2)) <= 1 
    THEN 1
   ELSE 0
  END AS amount_due_match
 /*REASONS FOR ALL VALID PAYMENTS*/
 ,CASE
   WHEN is_dc_case_ IS TRUE 
    THEN 'DC_PAYMENTS'
/*CUSTOMER PAID AMOUNT IS EQUAL TO SOME PAYMENTS ON CUSTOMER LEVEL DUE AMOUNT*/
  WHEN ABS(t_manual_customer::DECIMAL(30, 2) - amt_due_customer::DECIMAL(30, 2)) <= 1
     OR ABS(t_manual_order::DECIMAL(30, 2) - full_amt_due_customer::DECIMAL(30, 2)) <= 1
    THEN 'CUSTOMER_LEVEL_MATCH'    
/*CUSTOMER PAID AMOUNT WHICH IS EQUAL TO SUM OF SOME PAYMENTS FOR AN ORDER */
   WHEN ABS(t_manual_order::DECIMAL(30, 2) - amt_due_order::DECIMAL(30, 2)) <= 1 
     OR ABS(t_manual_order::DECIMAL(30, 2) - full_order_amt::DECIMAL(30, 2)) <= 1 
    THEN 'ORDER_LEVEL_MATCH' 
   ELSE 'NO_LOGIC' ----payment amount does not fit into any logic
  END AS amount_due_match_logic_pre
 /*CONSOLIATED PAYMENTS BASED ON ABOVE LOGIC*/ 
 ,CASE 
   WHEN amount_due_match_logic_pre = 'ORDER_LEVEL_MATCH'
    THEN amt_due_order
   WHEN amount_due_match_logic_pre = 'CUSTOMER_LEVEL_MATCH' 
    THEN amt_due_customer
  END AS amount_due
 ,CASE 
   WHEN amount_due_match_logic_pre = 'ORDER_LEVEL_MATCH'
    THEN amount_due - t_manual_order
   WHEN amount_due_match_logic_pre = 'CUSTOMER_LEVEL_MATCH' 
    THEN amount_due - t_manual_customer
  END AS customer_adjusted_amount_total
FROM final     
)
,order_level_match_customer AS (
SELECT DISTINCT customer_id 
FROM a 
WHERE amount_due_match_logic_pre = 'ORDER_LEVEL_MATCH'
)   
SELECT 
  a.report_date
 ,a.arrival_date
 ,a.booking_date
 ,a.customer_id
 ,a.order_id
 ,a.reference_new AS remi_remittance_information
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,a.bank_reference
 ,a.country
 ,a.no_of_payments_per_orders
 ,a.idx_payments_per_orders
 ,a.no_of_payments_per_reference
 ,a.idx_payments_per_reference
 ,a.t_manual_order
 ,a.order_amount
 ,a.t_manual_customer
 ,a.amount_due_match
 ,a.amt_due_order
 ,a.amount_due
 ,a.customer_adjusted_amount_total
 ,a.is_dc_case_ AS is_dc_case
 ,CASE 
   WHEN ord.customer_id IS NOT NULL 
     AND a.amount_due_match_logic_pre = 'CUSTOMER_LEVEL_MATCH' 
    THEN 'NO_LOGIC'
   ELSE a.amount_due_match_logic_pre
  END AS amount_due_match_logic
 ,CASE
   WHEN amount_due_match_logic <> 'NO_LOGIC' 
    THEN amount_due_match_logic 
   ELSE 
    CASE
     WHEN a.is_dc_case IS TRUE 
      THEN 'DC_PAYMENT'
     WHEN a.is_new_infra_payments IS TRUE 
      THEN 'ORDER_SHIFTED_TO_NEW_INFRA'
     WHEN a.t_manual_order <= 1 
      THEN 'AMOUNT_PAID <= 1_EUR'
     WHEN a.t_manual_order > a.amt_due_order 
       AND ABS(a.t_manual_order - a.asset_amount) > 1 
      THEN 'AMOUNT_PAID > ORDER_AMOUNT'
     WHEN a.t_manual_order < a.amt_due_order 
      THEN 'AMOUNT_PAID < ORDER_AMOUNT'
     WHEN a.no_of_payments_per_orders > 1 
      THEN 'PAYMENTS_WITH_MULTIPLE_ORDERS'
     WHEN ABS(a.t_manual_order - a.asset_amount) <= 1 
      THEN 'ASSET_PAYMENTS'
     WHEN a.amt_due_order IS NULL 
       AND a.t_manual_customer <> a.amt_due_customer 
      THEN 'NO_DUE_AMOUNT_FOR_ORDER'
    END
  END AS reasons_booking_method--Providing reasons FOR the NO_LOGIC payments, its gets easy for debugging AND DC agents 
FROM a
--making sure a customer does NOT have BOTH ORDER LEVEL AND customer LEVEL logic match
  LEFT JOIN order_level_match_customer ord
    ON a.customer_id = ord.customer_id
;

-------------------------------------------------------------------------------
-------------------------HISTORIZATION-----------------------------------------
-------------------------------------------------------------------------------
DELETE FROM payment.bnp_manual_payments_due_match_customer_historical
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_due_match_customer_historical
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
 ,no_of_payments_per_orders
 ,idx_payments_per_orders
 ,no_of_payments_per_reference
 ,idx_payments_per_reference
 ,t_manual_order
 ,order_amount
 ,t_manual_customer
 ,amount_due_match
 ,amt_due_order
 ,amount_due
 ,customer_adjusted_amount_total
 ,is_dc_case
 ,amount_due_match_logic
 ,reasons_booking_method
FROM payment.bnp_manual_payments_due_match_customer
;

GRANT SELECT ON payment.bnp_manual_payments_due_match_customer TO tableau;