/*
 * This table creating the Fuzz mapping , where the reference given from COBA payments are mapped either using orderid, subscription_sfid,
   Subscription_payment_sfid,Subscription_boid and invoice_number.Once these are mapped in the matching parameter. Its correspoinding Order Id is taken into consideration
 * Each Regex is specially written to address a pattern and the type of the pattern is mentioned next to the Regex command
 *  */
drop table if exists dm_debt_collection.coba_manual_payments_raw;
create table dm_debt_collection.coba_manual_payments_raw as 
WITH invoice_order AS (---To avoid consolidate payments having one invoice number FOR multiple payments.
   SELECT 
      invoice_number,
      order_id,
      customer_id,
      count(subscription_payment_id)OVER(PARTITION BY invoice_number ) AS no_of_payments_invoice
   FROM ods_production.payment_subscription ps 
   WHERE TRUE
   QUALIFY count(subscription_payment_id)OVER(PARTITION BY invoice_number )=1
)
,base AS (
SELECT  
        current_date as execution_date,
        extractnumber :: INTEGER as report_extract_number,
        current_date as report_date,
        case
            when arrival_date like '%.%' then (
                right(left(arrival_date, 10), 4) || '-' || left(right(left(arrival_date, 10), 7), 2) || '-' || left(left(arrival_date, 10), 2)
            ) :: date
            else arrival_date :: date
        end as arrival_date,
        case
            when booking_date like '%.%' then (
                right(left(booking_date, 10), 4) || '-' || left(right(left(booking_date, 10), 7), 2) || '-' || left(left(booking_date, 10), 2)
            ) :: date
            else booking_date :: date
        end as booking_date,
        REPLACE(REPLACE(trim(amount),',',''),',','.')::float as amount,
        currency,
        reference,
    UPPER(REPLACE(COALESCE(
    NULLIF(REGEXP_SUBSTR(co.reference,'R[0-9]{9,10}', 1,  1, 'i'), ''),
    NULLIF(REGEXP_SUBSTR(co.reference, 'R[0-9]{7,8}[[:space:]][0-9]{1,2}',1,1, 'i'), ''),--R00000000
    NULLIF(REGEXP_SUBSTR(co.reference, 'R[0-9]{3,5}[[:space:]][0-9]{5,7}',1,1, 'i'), ''),--R0000000 00
    NULLIF(REGEXP_SUBSTR(co.reference, 'R[0-9]{2}[[:space:]][0-9]{2}[[:space:]][0-9]{2}[[:space:]][0-9]{3,4}',1,1, 'i'), ''),--R0000 00000
    SUBSTRING(NULLIF(REGEXP_SUBSTR(co.reference, '\\WR[[:space:]][0-9]{9,10}',1,1, 'i'), ''), 2),--(;./)R0000000000
    SUBSTRING(NULLIF(REGEXP_SUBSTR(co.reference, '\\W[F,f][0-9]{7,8}[ ]?[0-9]{2,3}',1,1, 'i'), ''),2),--(;./)R0000000000
    NULLIF(REGEXP_SUBSTR(co.reference,'[F][0-9]{10}$',1,1,'i'), ''),-- F000000000
    NULLIF(REGEXP_SUBSTR(co.reference,'[S][-][0-9]{9,10}',1,1,'i'), ''),-- S000000000
    NULLIF(REGEXP_SUBSTR(co.reference,'G-[^ ]{8}',1, 1,'i'),''),-- G-AAAAAAAA
    NULLIF(REGEXP_SUBSTR(co.reference,'SP-[0-9]{9}',1,1,'i'), ''),-- SP-000000000
    NULLIF(REPLACE(REGEXP_SUBSTR(co.reference,'DE[-.( )][0-9]{6}[-.( )][0-9]{10}',1,1, 'i'),'.','-'), ''),-- DE-000000-0000000000
    NULLIF(REPLACE(REGEXP_SUBSTR(co.reference,'AT[-.( )][0-9]{6}[-.( )][0-9]{10}',1,1, 'i'),'.','-'), ''),-- AT-000000-0000000000
    NULLIF(REPLACE(REGEXP_SUBSTR(co.reference,'NL[-.( )][0-9]{6}[-.( )][0-9]{10}',1,1, 'i'),'.','-'), ''),-- NL-000000-0000000000
    NULLIF(REPLACE(REGEXP_SUBSTR(co.reference,'ES[-.( )][0-9]{6}[-.( )][0-9]{10}',1,1, 'i'),'.','-'), ''),-- ES-000000-0000000000
                        co.reference), ' ', '')) AS matching_parameter
    ,UPPER(customer_name) as customer_name
    ,customer_iban as payment_account_iban
  FROM debt_collection.manual_payments_coba co
      where report_date=current_date
    )
    SELECT
    b.*,
    COALESCE(o.order_id,s.order_id,s1.order_id,ps1.order_id,ps.order_id) AS order_id,---all order_id from various sources are consolidated
    COALESCE(o.customer_id,s.customer_id,s1.customer_id,ps1.customer_id,ps.customer_id) AS customer_id,---all customer_id from various sources are consolidated
    CASE 
       WHEN matching_parameter ILIKE 'R%' AND o.order_id IS NOT NULL  THEN 'HIGH'
       WHEN matching_parameter SIMILAR TO 'G-%|SP-%|S-%|DE-%|AT-%|NL-%|ES-%|F%' THEN 'MEDIUM'
       ELSE 'LOW' 
    end as confidence_on_order_id ---all direct order_id mapped from reference are considered HIGH confidence and order_id mapped from other sources are considered MEDIUM and upon missing its LOW
    from base b 
    LEFT JOIN ods_production.order o
      ON b.matching_parameter = o.order_id
   LEFT JOIN ods_production.subscription s 
      ON b.matching_parameter=s.subscription_bo_id
   LEFT JOIN ods_production.subscription s1
      ON b.matching_parameter=s1.subscription_name
   LEFT JOIN ods_production.payment_subscription ps 
      ON b.matching_parameter=ps.subscription_payment_name
   LEFT JOIN invoice_order ps1 
      ON b.matching_parameter=ps1.invoice_number;

GRANT SELECT ON dm_debt_collection.coba_manual_payments_raw TO tableau;
