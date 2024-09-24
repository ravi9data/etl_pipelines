/*
* This table combines both the order id mapping and manual review cases and payments are seggregated into AUTOMATED and MANUA
*/
drop table if exists dm_debt_collection.coba_manual_payment_recon;
create table dm_debt_collection.coba_manual_payment_recon AS
WITH base AS (
SELECT ---All order where there are multiple reference for 1 payment
    DISTINCT
    o.execution_date,
    o.report_extract_number,
    o.report_date,
    o.arrival_date,
    o.booking_date,
    o.order_id,
    o.customer_id,
    o.currency,
    o.customer_name,
    o.reference,
    om.amount_due_match_logic,
    om.idx_payments_per_reference,
    CASE WHEN amount_due_match_logic='No logic' THEN 'Payments With Multiple orders' ELSE amount_due_match_logic end AS reasons_booking_method,
    1 AS idx_payments_per_orders,
    sum(om.t_manual_order) AS amount
    FROM 
    dm_debt_collection.coba_manual_payments_orders o
    LEFT JOIN dm_debt_collection.coba_manual_payment_due_match_customer om
      ON o.reference=om.reference-- Entire Base
    WHERE o.no_of_payments_per_orders>1
    AND o.ORDER_ID IS NOT NULL 
    AND om.no_of_payments_per_orders<>om.no_of_payments_per_reference
    and om.amount_due_match_logic<>'0.5 logic customer'   
    --and o.customer_id='924556'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    UNION all
    SELECT -------all payments where there are one reference per payments 
    DISTINCT
    o.execution_date,
    o.report_extract_number,
    o.report_date,
    o.arrival_date,
    o.booking_date,
    o.order_id,
    o.customer_id,
    o.currency,
    o.customer_name,
    o.reference,
    om.amount_due_match_logic,
    om.idx_payments_per_reference,
    CASE WHEN amount_due_match_logic='No logic' THEN 'Payments With Multiple orders' ELSE amount_due_match_logic end AS reasons_booking_method,
    1 AS idx_payments_per_orders,
    sum(o.amount) AS amount
    FROM 
    dm_debt_collection.coba_manual_payments_orders o
    LEFT JOIN dm_debt_collection.coba_manual_payment_due_match_customer om
      ON o.reference=om.reference-- Entire Base
    WHERE o.no_of_payments_per_orders>1
    AND o.ORDER_ID IS NOT NULL 
    AND om.no_of_payments_per_orders=om.no_of_payments_per_reference
    and om.amount_due_match_logic<>'0.5 logic customer'   
    --and o.customer_id='924556'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
    UNION ALL 
    SELECT --------all other cases mapped at order level 
       DISTINCT
       o.execution_date,
       o.report_extract_number,
       o.report_date,
       o.arrival_date,
       o.booking_date,
       o.order_id,
       o.customer_id,
       o.currency,
       o.customer_name,
       o.reference,
       om.amount_due_match_logic,
       om.idx_payments_per_reference,
       Case 
         WHEN om.reasons_booking_method IS NULL AND o.no_of_payments_per_orders between 1 AND 5 THEN 'Payments With Multiple orders'-- ensuring NULL value FOR double orders ARE filled
         WHEN o.pass_through not in ('ERROR','NO_ORDER_ID_FOUND','CUSTOMER_ID_NOT FOUND') THEN om.reasons_booking_method
            ELSE 'Incorrect Reference'
       END as reasons_booking_method,
       o.idx_payments_per_orders,
       o.amount
       FROM    
       dm_debt_collection.coba_manual_payments_orders o
       LEFT JOIN dm_debt_collection.coba_manual_payment_due_match_customer om
         ON o.reference=om.reference-- Entire Base
         AND o.idx_payments_per_orders=om.idx_payments_per_orders
         WHERE TRUE 
         --and o.customer_id='924556'
         --AND o.customer_id NOT IN (SELECT DISTINCT customer_id FROM dm_debt_collection.coba_manual_payment_due_match_customer om WHERE amount_due_match_logic='0.5 logic customer' )
         AND (o.no_of_payments_per_orders=1 OR o.order_id IS NULL )
         ) 
,FINAL AS (
SELECT ----payments mapped at customer level
    DISTINCT
    o.execution_date,
    o.report_extract_number,
    o.report_date,
    o.arrival_date,
    o.booking_date,
    o.order_id,
    o.customer_id,
    o.currency,
    o.customer_name,
    o.reference,
    om.amount_due_match_logic,
    1::int AS idx_payments_per_reference,
    om.reasons_booking_method as reasons_booking_method,
    o.idx_payments_per_orders,
    o.amount,
    NULL::int AS automated_amount_due,
    c.amount_due,
    'Automated' as booking_method,
    0::decimal as fl
    FROM    
    dm_debt_collection.coba_manual_payments_orders o
    INNER JOIN dm_debt_collection.coba_manual_payment_order_id_mapping om 
      ON o.customer_id=om.customer_id-- Entire Base
         left join
            (select
               customer_id,
               sum(amount_due) as amount_due
            from
              dm_debt_collection.coba_manual_payment_order_id_mapping
              WHERE due_date<=current_date
            group by 1
            )
            c --Automated Applied Cases
         on o.customer_id = c.customer_id
         WHERE om.amount_due_match_logic='0.5 logic customer'       
UNION DISTINCT 
 SELECT 
   o.*,
   CASE 
       WHEN o.idx_payments_per_reference BETWEEN 2 AND 5 THEN d.amount_due 
       WHEN o.idx_payments_per_reference=1 THEN b.amount_due 
   END AS automated_amount_due,
    CASE 
      WHEN o.reasons_booking_method IN ('0.5 logic order','order 2 payments','order 3 payments') THEN COALESCE(automated_amount_due,a.amount_due)
      WHEN o.reasons_booking_method='planned payments' THEN COALESCE(p.planned_amount,0)
      WHEN o.reasons_booking_method IN ('1planned+1Failed','1planned+2Failed') THEN COALESCE(p.planned_amount,0)+COALESCE(automated_amount_due,0)
      WHEN o.amount_due_match_logic='No logic' OR o.amount_due_match_logic IS NULL THEN coalesce(a.amount_due)
    end
    as amount_due,
    case 
        when b.order_id is not null then 'Automated'
        WHEN p.order_id IS NOT NULL THEN 'Automated'
        when a.reference is not null then 'Manual'
        when o.reference is null then 'Manual'
    else 'Manual' end as booking_method,
    0::decimal as fl
    FROM    
    base o
        left join
        (select
            reference,
            order_id,
            amount_due_match_logic,
            sum(order_id_due_amount) as amount_due
        from
        dm_debt_collection.coba_manual_payment_manual_review
        --WHERE  customer_id='2286976'
        group by 1,2,3
        )
        a --Manual Review Cases
        on a.reference = o.reference 
        left join
            (select
               order_id,
               sum(amount_due) as amount_due
            from
              dm_debt_collection.coba_manual_payment_order_id_mapping
              WHERE due_date<=current_date
            group by 1
            )
            b --Automated Applied Cases REF 1
        on o.order_id = b.order_id
          left join
            (select
               order_id,
               amount_due as amount_due
            from
              dm_debt_collection.coba_manual_payment_order_id_mapping
              WHERE due_date<=current_date
            )
            d --Automated Applied Cases REF 2-5
        on o.order_id = d.order_id
          left join
            (select
               order_id,
               sum(amount_due) as planned_amount
            from
              dm_debt_collection.coba_manual_payment_order_id_mapping
              WHERE due_date>=current_date
            group by 1
            )
            p--Planned cases
            on o.order_id = p.order_id
            WHERE true
            AND (o.customer_id NOT IN (SELECT customer_id FROM dm_debt_collection.coba_manual_payment_due_match_customer d WHERE amount_due_match_logic='0.5 logic customer')
            OR o.customer_id IS NULL ))
            SELECT 
               execution_date 
               ,report_extract_number  
               ,report_date   
               ,arrival_date  
               ,booking_date  
               ,order_id   
               ,customer_id   
               ,currency   
               ,customer_name 
               ,reference       
               ,amount  
               ,amount_due 
               ,reasons_booking_method 
               ,booking_method   
            FROM FINAL;

-----Historize the table 
DELETE FROM dm_debt_collection.coba_manual_payment_recon_historical 
WHERE  execution_date = current_date;

INSERT INTO dm_debt_collection.coba_manual_payment_recon_historical 
SELECT 
  execution_date 
  ,report_extract_number  
  ,report_date   
  ,arrival_date  
  ,booking_date  
  ,order_id   
  ,customer_id   
  ,currency   
  ,customer_name 
  ,reference  
  ,amount  
  ,amount_due 
  ,reasons_booking_method 
  ,booking_method
FROM dm_debt_collection.coba_manual_payment_recon; 

GRANT SELECT ON dm_debt_collection.coba_manual_payment_recon TO tableau;

GRANT SELECT ON dm_debt_collection.coba_manual_payment_recon_historical TO tableau;
