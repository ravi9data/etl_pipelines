/*
 * THIS TABLE CREATING THE FUZZ MAPPING WHERE THE REFERENCES FROM BNP PAYMENTS ARE MAPPED EITHER USING ORDERID, SUBSCRIPTION_SFID,
   SUBSCRIPTION_PAYMENT_SFID, SUBSCRIPTION_BOID AND INVOICE_NUMBER. ONCE THESE ARE MAPPED IN THE MATCHING PARAMETER, 
   THEIR CORRESPONDING ORDER ID IS TAKEN INTO CONSIDERATION
 * EACH REGEX IS SPECIALLY WRITTEN TO ADDRESS A PATTERN AND THE TYPE OF THE PATTERN IS MENTIONED NEXT TO THE REGEX COMMAND
 *  */
DROP TABLE IF EXISTS payment.bnp_manual_payments_raw;
CREATE TABLE payment.bnp_manual_payments_raw AS
WITH invoice_order AS (
SELECT 
/*TO AVOID CONSOLIDATE PAYMENTS HAVING ONE INVOICE NUMBER FOR MULTIPLE PAYMENTS*/
  invoice_number
 ,order_id
 ,customer_id
 ,COUNT(subscription_payment_id) OVER (PARTITION BY invoice_number ) AS no_of_payments_invoice
FROM ods_production.payment_subscription 
WHERE TRUE
QUALIFY COUNT(subscription_payment_id) OVER (PARTITION BY invoice_number ) = 1
)
,invoice_customer AS (
SELECT  
  invoice_number
 ,customer_id
 ,ROUND(SUM(amount_due), 2) AS invoice_amount
FROM ods_production.payment_subscription
GROUP BY 1,2
)
,base AS (
SELECT 
  CURRENT_DATE AS report_date
 ,TO_DATE(REPLACE(r.book_date, '.', '/'), 'DD/MM/YYYY') AS booking_date
 ,TO_DATE(REPLACE(r.value_date, '.', '/'), 'DD/MM/YYYY') AS value_date
 ,r.company_name
 ,REPLACE(REPLACE(TRIM(r.amount),',',''),',','.')::FLOAT AS amount
 ,r.bank_reference
 ,r.account_number
 ,CASE 
   WHEN r.account_number ILIKE 'DE%'
    THEN 'Germany'
   WHEN r.account_number ILIKE 'ES%'
    THEN 'Spain'
   WHEN r.account_number ILIKE 'AT%'
    THEN 'Austria'
   WHEN r.account_number ILIKE 'NL%'
    THEN 'Netherlands'
  END AS country
 ,r.supplementary_details
 ,CASE
   WHEN r.remi_remittance_information ILIKE '%DCI%'
     OR r.remi_remittance_information ILIKE '%DCA%'
     OR r.remi_remittance_information ILIKE '%DCC%'
    THEN TRUE 
   ELSE FALSE
  END AS is_dc_case
 ,r.remi_remittance_information
 ,CASE 
   WHEN is_dc_case IS FALSE
    THEN UPPER(REPLACE(COALESCE(
      NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'R[0-9]{9,10}', 1,  1, 'i'), '')
     --R000000 00
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information, 'R[0-9]{7,8}[[:space:]][0-9]{1,2}',1,1, 'i'), '')
     --R0000000 00
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information, 'R[0-9]{3,5}[[:space:]][0-9]{5,7}',1,1, 'i'), '')
     --R0000 00000
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information, 'R[0-9]{2}[[:space:]][0-9]{2}[[:space:]][0-9]{2}[[:space:]][0-9]{3,4}',1,1, 'i'), '')
     --(;./)R0000000000
     ,SUBSTRING(NULLIF(REGEXP_SUBSTR(r.remi_remittance_information, '\\WR[[:space:]][0-9]{9,10}',1,1, 'i'), ''), 2)
     --(;./)R0000000000
     ,SUBSTRING(NULLIF(REGEXP_SUBSTR(r.remi_remittance_information, '\\W[F,f][0-9]{7,8}[ ]?[0-9]{2,3}',1,1, 'i'), ''),2)
     --F000000000
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'[F][0-9]{10}$|[F][0-9]{10}',1,1,'i'), '')
     --FX00X00X0
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'F-[(0-9A-Z)]{8}',1,1,'i'), '')
     --S-000000000
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'[S][-][0-9]{9,10}',1,1,'i'), '')
     --S000000000
     ,LEFT(NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'[S][0-9]{9,10}',1,1,'i'), ''),1)||'-'||
       RIGHT(NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'[S][0-9]{9,10}',1,1,'i'), ''),10)
     --G-AAAAAAAA
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'G-[^ ]{8}',1, 1,'i'),'')
     -- SP-000000000
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'SP-[0-9]{9}',1,1,'i'), '')
     --SP000000000
     ,LEFT(NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'SP[ ]?[0-9]{9}',1,1,'i'), ''),2)||'-'||
       RIGHT(NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'SP[ ]?[0-9]{9}',1,1,'i'), ''),9)
     --DE/ES/NL/AT-000000-0000000000  
     ,NULLIF(REPLACE(REGEXP_SUBSTR(r.remi_remittance_information,'DE[-.][0-9]{6}[-.][0-9]{10}|AT[-.][0-9]{6}[-.][0-9]{10}|NL[-.][0-9]{6}[-.][0-9]{10}|ES[-.][0-9]{6}[-.][0-9]{10}',1,1, 'i'),'.','-'), '')
     --DE/AT/NL/ES0000000000000000
     ,LEFT(NULLIF(REPLACE(REGEXP_SUBSTR(r.remi_remittance_information,'DE[0-9]{16}|AT[0-9]{16}|NL[0-9]{16}|ES[0-9]{16}',1,1, 'i'),'.','-'), ''),2)||'-'||
       LEFT(RIGHT(NULLIF(REPLACE(REGEXP_SUBSTR(r.remi_remittance_information,'DE[0-9]{16}|AT[0-9]{16}|NL[0-9]{16}|ES[0-9]{16}',1,1, 'i'),'.','-'), ''),16),6)||'-'||
       RIGHT(NULLIF(REPLACE(REGEXP_SUBSTR(r.remi_remittance_information,'DE[0-9]{16}|AT[0-9]{16}|NL[0-9]{16}|ES[0-9]{16}',1,1, 'i'),'.','-'), ''),10)
     --DE/AT/NL/ES 000000 0000000000
     ,LEFT(NULLIF(REPLACE(REGEXP_SUBSTR(r.remi_remittance_information,'DE[ ][0-9]{6}[ ][0-9]{10}|AT[ ][0-9]{6}[ ][0-9]{10}|NL[ ][0-9]{6}[ ][0-9]{10}|ES[ ][0-9]{6}[ ][0-9]{10}',1,1, 'i'),'.','-'), ''),2)||'-'||
       LEFT(RIGHT(NULLIF(REPLACE(REGEXP_SUBSTR(r.remi_remittance_information,'DE[ ][0-9]{6}[ ][0-9]{10}|AT[ ][0-9]{6}[ ][0-9]{10}|NL[ ][0-9]{6}[ ][0-9]{10}|ES[ ][0-9]{6}[ ][0-9]{10}',1,1, 'i'),'.','-'), ''),17),6)||'-'||
       RIGHT(NULLIF(REPLACE(REGEXP_SUBSTR(r.remi_remittance_information,'DE[ ][0-9]{6}[ ][0-9]{10}|AT[ ][0-9]{6}[ ][0-9]{10}|NL[ ][0-9]{6}[ ][0-9]{10}|ES[ ][0-9]{6}[ ][0-9]{10}',1,1, 'i'),'.','-'), ''),10)
     --00000000X DNI-1
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'[ ][0-9]{8}[A-Z]{1}[ ]?|^[0-9]{8}[A-Z]{1}[ ]?',1,1,'i'), '')
     --X0000000X DNI-2
     ,NULLIF(REGEXP_SUBSTR(r.remi_remittance_information,'[ ][A-Z]{1}[0-9]{7}[A-Z]{1}[ ]?|^[A-Z]{1}[0-9]{7}[A-Z]{1}[ ]',1,1,'i'), '')
     ,r.remi_remittance_information), ' ', ''))
   ELSE NULL       
  END AS matching_parameter
 ,UPPER(r.ordp_name_ordering_party_name) AS customer_name
 ,r.info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,r.obk_bic_or_local_code_ordering_bank
FROM payment.bnp_manual_payments_input_list r
  LEFT JOIN staging_airbyte_bi.bnp_exclusion_list exc
    ON r.supplementary_details ILIKE '%'||exc.exclusion_list||'%' 
WHERE exc.exclusion_list IS NULL
)
,personal_id_number AS (
/*LET'S USE LAST_VALUE() TO MAKE SURE WE HAVE RECORD PER CUSTOMER*/
SELECT DISTINCT
  user_id AS customer_id
 ,LAST_VALUE(identification_number) OVER (PARTITION BY user_ID 
   ORDER BY created_at, updated_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS personal_id_number
FROM s3_spectrum_rds_dwh_api_production_sensitive.personal_identifications 
)
,pre_final as(
SELECT
  b.*
/*ORDER_ID FROM VARIOUS SOURCES ARE CONSOLIDATED*/  
 ,COALESCE(o.order_id, s.order_id, s1.order_id, ps1.order_id, ps.order_id, s2.order_id) AS order_id
/*CUSTOMER_ID FROM VARIOUS SOURCES ARE CONSOLIDATED*/ 
 ,COALESCE(o.customer_id, s.customer_id, s1.customer_id, ps1.customer_id, ps.customer_id,
            s2.customer_id, dni.customer_id::BIGINT, ic.customer_id) AS customer_id
FROM base b 
  LEFT JOIN ods_production.order o
    ON b.matching_parameter = o.order_id
  LEFT JOIN ods_production.subscription s 
    ON b.matching_parameter = s.subscription_bo_id
  LEFT JOIN ods_production.subscription s1
    ON b.matching_parameter = s1.subscription_name
  LEFT JOIN ods_production.subscription s2
    ON b.matching_parameter = s2.subscription_id
  LEFT JOIN ods_production.payment_subscription ps 
    ON b.matching_parameter = ps.subscription_payment_name
  LEFT JOIN invoice_order ps1 
    ON b.matching_parameter = ps1.invoice_number
  LEFT JOIN personal_id_number dni
    ON b.matching_parameter = dni.personal_id_number
  LEFT JOIN invoice_customer ic 
    ON b.matching_parameter = ic.invoice_number
   AND b.amount = ic.invoice_amount
)
SELECT 
  *
/*ORDER_IDS MAPPED FROM REFERENCE ARE CONSIDERED HIGH CONFIDENCE 
 AND ORDER_IDS MAPPED FROM OTHER SOURCES ARE CONSIDERED MEDIUM AND UPON MISSING ITS LOW*/
 ,CASE 
   WHEN matching_parameter ILIKE 'R%'
     AND order_id IS NOT NULL
    THEN 'HIGH'
   WHEN (matching_parameter SIMILAR TO 'G-%|SP-%|S-%|DE-%|AT-%|NL-%|ES-%|F%'
      AND customer_id IS NOT NULL)
     OR customer_id IS NOT NULL 
    THEN 'MEDIUM'
   ELSE 'LOW' 
  END AS confidence_on_order_id
FROM pre_final;

-------------------------------------------------------------------------------
-------------------------HISTORIZATION-----------------------------------------
-------------------------------------------------------------------------------
DELETE FROM payment.bnp_manual_payments_raw_historical
WHERE report_date = CURRENT_DATE;

INSERT INTO payment.bnp_manual_payments_raw_historical
SELECT 
  report_date
 ,booking_date
 ,value_date
 ,company_name
 ,amount
 ,bank_reference
 ,account_number
 ,country
 ,supplementary_details
 ,is_dc_case
 ,remi_remittance_information
 ,matching_parameter
 ,customer_name
 ,info_account_info_debtor_account_for_crdt_or_creditor_account_for_dbt_
 ,obk_bic_or_local_code_ordering_bank
 ,order_id
 ,customer_id
 ,confidence_on_order_id
FROM payment.bnp_manual_payments_raw
;

GRANT SELECT ON payment.bnp_manual_payments_raw TO tableau;