/*
 * This table applies the logic based considering the payment made and the amount due in the payments table.
 * Note we do not take asset payments into consideration.
 * If a payment is done, first it is assigned to a failed payments and then to a planned payment
 * pass_through not in ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND') are directly considered to be manual payments and hence are exlcuded here 
 *  In case of Subscription payments 
         Paid date should be null 
         exclude migrated from old infra to new infra orders 
         Exclude HELD and NEW payments as these status are mainly present in new infra                       
 */
drop table if exists dm_debt_collection.coba_manual_payment_due_match_customer;
create table dm_debt_collection.coba_manual_payment_due_match_customer AS
WITH double_reference AS ( 
  SELECT ---------------multiple payments UNDER same reference
  DISTINCT 
       max(execution_date)over(PARTITION BY order_id,reference) AS execution_date,
       max(report_date)over(PARTITION BY order_id,reference) AS report_date,
       max(arrival_date)over(PARTITION BY order_id,reference) AS arrival_date,
       max(booking_date)over(PARTITION BY order_id,reference) AS booking_date ,
       customer_id,
       order_id,
       reference,
       LISTAGG(DISTINCT payment_account_iban,',')WITHIN GROUP (ORDER BY order_id)OVER(PARTITION BY order_id) AS payment_account_iban,
       1 AS no_of_payments_per_orders,
       1 AS idx_payments_per_orders,
       1 AS no_of_payments_per_reference,
       1 AS idx_payments_per_reference,
       sum(amount)over(PARTITION BY order_id,reference) AS t_manual_order,
       sum(amount)over(PARTITION BY customer_id) AS t_manual_customer,
       sum(amount) over(PARTITION BY order_id) AS order_amount
    FROM  dm_debt_collection.coba_manual_payments_orders
    WHERE no_of_payments_per_orders >1 AND no_of_payments_per_orders=no_of_payments_per_reference
    AND pass_through not in ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')
    UNION ALL 
   SELECT -------payments UNDER same ORDER Id but different reference 
   DISTINCT 
       max(execution_date)OVER(PARTITION BY order_id) AS execution_date ,
       max(report_date)OVER(PARTITION BY order_id) AS report_date,
       max(arrival_date)OVER(PARTITION BY order_id) AS arrival_date,
       max(booking_date)OVER(PARTITION BY order_id) AS booking_date,
       customer_id,
       order_id,
       last_VALUE(reference)OVER(PARTITION BY order_id) AS reference,
       LISTAGG(DISTINCT payment_account_iban,',')WITHIN GROUP (ORDER BY order_id)OVER(PARTITION BY order_id) AS payment_account_iban,
       no_of_payments_per_orders,
       1 AS idx_payments_per_orders,
       1 AS no_of_payments_per_reference,
       1 AS idx_payments_per_reference,
       sum(amount)over(PARTITION BY order_id) AS t_manual_order,
       sum(amount)over(PARTITION BY customer_id) AS t_manual_customer,
       sum(amount) over(PARTITION BY order_id) AS order_amount
    FROM  dm_debt_collection.coba_manual_payments_orders
    WHERE no_of_payments_per_orders >1 AND no_of_payments_per_orders<>no_of_payments_per_reference
    AND pass_through not in ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')
    )
,single_reference AS (
     SELECT 
      nw.execution_date,
      nw.report_date,
      nw.arrival_date,
      nw.booking_date,
      nw.customer_id,
      nw.order_id,
      nw.reference,
      LISTAGG(DISTINCT nw.payment_account_iban,',')WITHIN GROUP (ORDER BY nw.order_id)OVER(PARTITION BY nw.order_id) AS payment_account_iban,
      nw.no_of_payments_per_orders,
      nw.idx_payments_per_orders,
      nw.no_of_payments_per_reference,
      nw.idx_payments_per_reference,
      sum(nw.amount) over(PARTITION BY nw.reference,nw.customer_name) AS t_manual_ref,
      sum(nw.amount) OVER (PARTITION BY nw.customer_id,nw.customer_name) AS t_manual_customer,
      sum(nw.amount) over(PARTITION BY nw.order_id) AS order_amount
   FROM 
      dm_debt_collection.coba_manual_payments_orders nw
      LEFT JOIN double_reference dr 
      ON nw.order_id=dr.order_id
   where 
    pass_through not in ('ERROR','NO_ORDER_ID_FOUND', 'CUSTOMER_ID_NOT FOUND')
   AND
      (dr.order_id IS  NULL )
      )
,manual_transfer AS (
SELECT * FROM double_reference
UNION DISTINCT 
SELECT * FROM single_reference
)
,amount_due_order AS (-- for all order payments 
select 
DISTINCT 
      sp.order_id,
      sp.customer_id,
      sum(sp.amount_due) OVER (PARTITION BY order_id)  AS  amt_due_order,
      sum(sp.amount_due) OVER (PARTITION BY customer_id) AS amt_due_customer,
      count(sp.payment_id) OVER (PARTITION BY order_id) AS num_order_payments,
      count(sp.payment_id) OVER (PARTITION BY customer_id) AS num_customer_payments
   FROM 
       master.subscription_payment as sp
      where sp.paid_date is null -- not paid yet
      and sp.status not in('PLANNED', 'HELD','NEW') -- to remove planned and HELD payments
      and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
      )
,amount_due_2_orders AS (--- for all orders with 2 or more payments 
select 
      sp.order_id,
      sp.customer_id,
      sum(sp.amount_due) OVER (PARTITION BY payment_id ORDER BY due_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )  AS  amt_due_order
   FROM 
       master.subscription_payment as sp
      where sp.paid_date is null -- not paid yet
      and sp.status not in('PLANNED', 'HELD','NEW') -- to remove planned and HELD payments
      and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
      )
,amount_due_customer AS (--- for all customer level payments 
select 
DISTINCT 
      sp.customer_id,
      sum(sp.amount_due) OVER (PARTITION BY customer_id) AS amt_due_customer,
      count(sp.payment_id) OVER (PARTITION BY order_id) AS num_order_payments,
      count(sp.payment_id) OVER (PARTITION BY customer_id) AS num_customer_payments
   FROM 
       master.subscription_payment as sp
      where sp.paid_date is null -- not paid yet
      and sp.status not in('PLANNED', 'HELD','NEW') -- to remove planned and HELD payments
      and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
      )
,planned_amount_due as (---- for all planned payments 
SELECT 
      sp.order_id,
      sp.customer_id,
      sum(sp.amount_due) OVER (PARTITION BY order_id) AS planned_amount,
      count(sp.amount_due) OVER (PARTITION BY order_id) AS num_planned_payments
FROM master.subscription_payment as sp
where sp.paid_date is null -- not paid yet
      and sp.status ='PLANNED' 
      and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
      )
,new_infra_payments AS (---- to isolate orders with new infra payments
SELECT 
   order_id,
   count(CASE WHEN len(sp.payment_id) > 20 THEN payment_id END ) AS new_infra_payments
   FROM master.subscription_payment as sp
    where sp.paid_date is null -- not paid yet
    and sp.status not in('PLANNED', 'HELD','NEW') -- to remove planned and HELD payments
    --and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
    GROUP BY 1
)
,asset_payment AS (--- to isolate payments with asset payments 
SELECT 
   order_id,
   customer_id,
   sum(amount)OVER (PARTITION BY order_id) AS asset_amount
FROM master.asset_payment AS ap
WHERE TRUE
AND paid_date IS NULL 
AND payment_type ='CUSTOMER BOUGHT'
)
,FINAL AS (
SELECT 
distinct
   mt.*,
   ad.amt_due_order,
   --CASE WHEN no_of_payments_per_orders =1 THEN ad.amt_due_order ELSE ado.amt_due_order END amt_due_order,
   pa.planned_amount,
   COALESCE(ad.amt_due_customer,adc.amt_due_customer) AS amt_due_customer,
   COALESCE(pa.num_planned_payments,0) AS num_planned_payments,
   COALESCE(ad.num_order_payments,0) AS num_order_payments,
   COALESCE(adc.num_customer_payments,0) AS num_customer_payments,
   COALESCE(ap.asset_amount,0) AS asset_amount,
   COALESCE(nip.new_infra_payments,0) AS new_infra_payments
   FROM manual_transfer mt
   LEFT JOIN amount_due_order ad 
   ON mt.order_id=ad.order_id
   LEFT JOIN planned_amount_due pa 
      ON mt.order_id=pa.order_id
   LEFT JOIN amount_due_customer adc 
      ON mt.customer_id=adc.customer_id
   LEFT JOIN asset_payment ap 
      ON ap.order_id=mt.order_id
   LEFT JOIN new_infra_payments nip 
      ON nip.order_id=mt.order_id
   LEFT JOIN amount_due_2_orders ado
      ON ado.order_id=mt.order_id
      AND ado.amt_due_order=mt.t_manual_order
      )
      , a AS (
       select 
       *,
            CASE ---- ALL VALID payments are flagged as 1 else 0 
               WHEN num_order_payments%2=0 AND ABS(2*(t_manual_order::decimal(30, 2))-amt_due_order::decimal(30, 2))<=0.5 AND 2*idx_payments_per_orders=num_order_payments THEN 1  
               WHEN num_order_payments%3=0 AND ABS(3*(t_manual_order::decimal(30, 2))-amt_due_order::decimal(30, 2))<=0.5 AND 3*idx_payments_per_orders=num_order_payments THEN 1  
               WHEN ABS(t_manual_order::decimal(30, 2) - amt_due_order::decimal(30, 2))<=0.5  THEN 1
               WHEN ABS(t_manual_customer::decimal(30, 2) - amt_due_customer::decimal(30, 2))<=0.5 THEN 1
               WHEN amt_due_order IS NULL and ABS(t_manual_order::decimal(30, 2)- planned_amount::decimal(30, 2))<=0.5  THEN 1
               WHEN ABS(amt_due_order::decimal(30, 2)+planned_amount::decimal(30, 2)-t_manual_order::decimal(30, 2))<=0.5  AND num_order_payments=1 THEN 1
               WHEN ABS(amt_due_order::decimal(30, 2)+planned_amount::decimal(30, 2)-t_manual_order::decimal(30, 2))<=0.5 AND num_order_payments=2 THEN 1
               else 0
      END as amount_due_match, 
         CASE ---- Reasons for ALL VALID payments 
               WHEN num_order_payments%2=0 AND ABS(2*(t_manual_order::decimal(30, 2))-amt_due_order::decimal(30, 2))<=0.5 AND 2*idx_payments_per_orders=num_order_payments THEN 'order 2 payments' ---  customer payed amount which is equal to sum of 2 payments for a order 
               WHEN num_order_payments%3=0 AND ABS(3*(t_manual_order::decimal(30, 2))-amt_due_order::decimal(30, 2))<=0.5 AND 3*idx_payments_per_orders=num_order_payments THEN  'order 3 payments'---  customer payed amount which is equal to sum of 3 payments for a order (anything above 3 are put into manual)
               when ABS(t_manual_order::decimal(30, 2) - amt_due_order::decimal(30, 2))<=0.5 THEN '0.5 logic order' -----customer paid amount is equal to the order due amount 
               WHEN ABS(t_manual_customer::decimal(30, 2) - amt_due_customer::decimal(30, 2))<=0.5  THEN '0.5 logic customer' ---- customer paid amount is equal to the amount due by customer
               WHEN amt_due_order IS NULL and ABS(t_manual_order::decimal(30, 2)- planned_amount::decimal(30, 2))<=0.5  THEN 'planned payments' ----customer paid a planned payment in future. in this case there is no FAILED payments and has only PLANNED payments 
               WHEN ABS(amt_due_order::decimal(30, 2)+planned_amount::decimal(30, 2)-t_manual_order::decimal(30, 2))<=0.5 AND num_order_payments=1 THEN '1planned+1Failed' ---Customer payment a ORDER amount which equal to 1 FAILED AND 1 PLANNED 
               WHEN ABS(amt_due_order::decimal(30, 2)+planned_amount::decimal(30, 2)-t_manual_order::decimal(30, 2))<=0.5 AND num_order_payments=2 THEN '1planned+2Failed' ---Customer payment a ORDER amount which equal to 1 FAILED AND 2 PLANNED 
               else 'No logic' ----payment amount does not fit into any logic
      END as amount_due_match_logic_pre,
    CASE 
       WHEN amount_due_match_logic_pre IN ('0.5 logic order','order 2 payments','order 3 payments') THEN amt_due_order
       WHEN amount_due_match_logic_pre='0.5 logic customer' THEN amt_due_customer
       WHEN amount_due_match_logic_pre='planned payments' THEN planned_amount
       WHEN amount_due_match_logic_pre IN ('1planned+1Failed','1planned+2Failed')  THEN amt_due_order+planned_amount
    END AS amount_due, ---Consoliated payments based ON above logic.
    count(CASE WHEN amount_due_match_logic_pre='planned payments' then order_id end)over(PARTITION BY order_id) same_order_double_payments 
   from FINAL
   )
     SELECT 
   execution_date,
   report_date,
   arrival_date,
   booking_date,
   customer_id,
   order_id,
   reference,
   payment_account_iban,
   no_of_payments_per_orders,
   idx_payments_per_orders,
   no_of_payments_per_reference,
   idx_payments_per_reference,
   t_manual_order,
   order_amount,
   t_manual_customer,
   amount_due_match,
   amt_due_order,
   amount_due,
   CASE
      WHEN amount_due_match_logic_pre<>'planned payments' THEN amount_due_match_logic_pre
      WHEN amount_due_match_logic_pre='planned payments' AND same_order_double_payments IN (0,1) THEN 'planned payments'
      WHEN amount_due_match_logic_pre='planned payments' AND same_order_double_payments IN (2) THEN 'No logic'
      END AS  amount_due_match_logic,
       CASE
       WHEN amount_due_match_logic <>'No logic' THEN amount_due_match_logic 
       ELSE 
         CASE
            WHEN t_manual_order <=1 THEN 'Amount paid <= 1 Eur'
            WHEN t_manual_order>amt_due_order and ABS(t_manual_order-asset_amount)>0.5 THEN 'Amount Paid > Order Amount'
            WHEN t_manual_order<amt_due_order THEN 'Amount Paid < Order Amount'
            WHEN no_of_payments_per_orders>1 THEN 'Payments With Multiple orders'
            WHEN ABS(t_manual_order-asset_amount)<=0.5 THEN 'Asset Payments'
            WHEN amt_due_order IS NULL AND t_manual_customer<>amt_due_customer THEN 'No due amount for order'
            WHEN new_infra_payments<>0 THEN 'Order Shifted To New infra'
            WHEN amt_due_order IS NULL AND amt_due_customer IS NULL AND planned_amount IS NOT NULL THEN 'Higher Plannend Amount and No Failed Amount'
            WHEN amt_due_order IS NULL AND amt_due_customer IS NULL AND planned_amount IS NULL THEN 'No Future or Failed Payments'
         END
    END AS reasons_booking_method--Providing reasons FOR the NO LOGIC payments, its gets easy for debugging AND DC agents 
    FROM a;



GRANT SELECT ON  dm_debt_collection.coba_manual_payment_due_match_customer TO tableau;
