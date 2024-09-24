/*
 * This Table focuses on all the automated payments and then distributes the data into the subscription payments.
 * Note that a user whose is mapped at the customer level could have change in order Id here compare to due match table because
         It could be that user have provided the wrong order ID in the reference or customer has given 1 order id in reference and amount is allocated to the multiple orders 
 * The Whole table logic is built by unioning three scripts which address different cases 
         1. Union 1 = Addresses all the orders with 0.5 LOGIC CUSTOMER
         2. Union 2 = Addresses all the orders which has 1 payment per order 
         3. Union 3 = Addresses all the orders with has 2 payments per order 
         (Split between union 2 and union 3 is because summation of the amount is getting duplicated with same approach)
 */
drop table if exists dm_debt_collection.coba_manual_payment_order_id_mapping;
create table dm_debt_collection.coba_manual_payment_order_id_mapping AS
WITH multiple_payments_on_due_date_pre AS ( ---we have many orders where there is same due date for multiple payments and we need to exclude these to avoid confusion.
   SELECT DISTINCT 
      sp.order_id,
      count(order_id)over(PARTITION BY order_id,due_date) multiple_payments_on_due_date
   FROM master.subscription_payment sp 
   where TRUE 
       and sp.paid_date is null
       and sp.status not in('HELD','NEW')
       and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
)
,multiple_payments_on_due_date AS (
   SELECT
      order_id, 
      max(multiple_payments_on_due_date) multiple_payments_on_due_date
   FROM multiple_payments_on_due_date_pre
   GROUP BY 1
    )
,payments AS (
SELECT 
   sp.psp_reference ,
   sp.payment_sfid,
   sp.order_id,
   sp.payment_id ,
   sp.customer_id,
   sp.due_date ,
   sp.amount_due
FROM master.subscription_payment sp 
LEFT JOIN multiple_payments_on_due_date mp 
ON mp.order_id=sp.order_id
where TRUE
    and sp.paid_date is null
    and sp.status not in('HELD','NEW')
    and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
    AND mp.multiple_payments_on_due_date=1
    )
,customer_logic AS (---payments which are aggregated at customer level
SELECT 
DISTINCT customer_id
FROM dm_debt_collection.coba_manual_payment_due_match_customer c
where c.amount_due_match_logic='0.5 logic customer' 
)
,a AS (
/*all the 0.5 logic customer customers */
SELECT --------payments aggregated at customer level 
   DISTINCT
   c.execution_date,
   c.report_date,
   c.customer_id,
   '0.5 logic customer' AS amount_due_match_logic,
   '0.5 logic customer' AS reasons_booking_method,
   sp.psp_reference ,
   sp.payment_sfid,
   sp.order_id ,
   null as reference,--- here reference IS NULL IN ORDER TO have single record per customer 
   sp.payment_id ,
   sp.due_date ,
   c.t_manual_customer amount_transfered_customer,
   c.t_manual_customer as amount_transfered,
   sp.amount_due ,
   case when amount_transfered_customer= sp.amount_due then 0 else 1 end as cumulative_manual_transfer,
   c.payment_account_iban,
   1 as fl
FROM dm_debt_collection.coba_manual_payment_due_match_customer c
LEFT JOIN master.subscription_payment sp 
   ON c.customer_id=sp.customer_id
INNER JOIN customer_logic lc 
   ON c.customer_id=lc.customer_id
where TRUE 
    AND c.amount_due_match = 1
    and sp.paid_date is null
    and sp.status not in('PLANNED','HELD','NEW')
    and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
UNION ALL 
/*other orders than 0.5 logic customer customers */
SELECT -------------payments which are not aggreated at order level or customer level but has only payment per order 
   DISTINCT
   c.execution_date,
   c.report_date,
   c.customer_id,
   c.amount_due_match_logic,
   c.reasons_booking_method,
   sp.psp_reference ,
   sp.payment_sfid,
   sp.order_id ,
   c.reference,
   sp.payment_id ,
   sp.due_date ,
   c.t_manual_customer amount_transfered_customer,
   c.t_manual_order as amount_transfered,
   sp.amount_due ,
   case when amount_transfered_customer= sp.amount_due then 0 else 1 end as cumulative_manual_transfer,
   c.payment_account_iban,
   1 as fl
FROM dm_debt_collection.coba_manual_payment_due_match_customer c
LEFT JOIN payments sp 
   ON c.order_id=sp.order_id
LEFT JOIN customer_logic lc 
   ON c.customer_id=lc.customer_id
where TRUE 
    AND c.amount_due_match = 1 
    AND no_of_payments_per_orders=1
     AND c.amount_due_match_logic<>'0.5 logic order' 
     AND lc.customer_id IS NULL 
    QUALIFY sum(sp.amount_due)over(PARTITION BY sp.order_id,c.reference ORDER BY sp.due_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT row)<=(c.t_manual_order+0.5)---we are taking payments which is equal to or less than order amount paid
    UNION ALL  
 SELECT-------------payments which are not aggreated at order level or customer level but has  more than one payment per order 
   c.execution_date,
   c.report_date,
   c.customer_id,
   c.amount_due_match_logic,
   c.reasons_booking_method,
   sp.psp_reference ,
   sp.payment_sfid,
   c.order_id ,
   c.reference,
   sp.payment_id ,
   sp.due_date ,
   c.t_manual_customer amount_transfered_customer,
   c.t_manual_order as amount_transfered,
   sp.amount_due ,
   case when amount_transfered_customer= sp.amount_due then 0 else 1 end as cumulative_manual_transfer,
   c.payment_account_iban,
   1 as fl
FROM dm_debt_collection.coba_manual_payment_due_match_customer c
LEFT JOIN payments sp
    ON c.order_id=sp.order_id
LEFT JOIN customer_logic lc 
   ON c.customer_id=lc.customer_id
    WHERE c.amount_due_match = 1
    AND no_of_payments_per_orders>1
    AND lc.customer_id IS NULL 
    AND c.amount_due_match_logic<>'0.5 logic order' 
    QUALIFY sum(sp.amount_due)over(PARTITION BY sp.order_id ORDER BY sp.due_date ROWS BETWEEN UNBOUNDED PRECEDING AND current row )<=(c.order_amount+0.5) ---we are taking payments which is equal to or less than order amount paid
   UNION ALL 
   SELECT ------payments aggregated at order level 
   DISTINCT
   c.execution_date,
   c.report_date,
   c.customer_id,
   c.amount_due_match_logic,
   c.reasons_booking_method,
   sp.psp_reference ,
   sp.payment_sfid,
   sp.order_id ,
   null as reference,--- here reference IS NULL IN ORDER TO have single record per customer 
   sp.payment_id ,
   sp.due_date ,
   c.t_manual_customer amount_transfered_customer,
   c.t_manual_customer as amount_transfered,
   sp.amount_due ,
   case when amount_transfered_customer= sp.amount_due then 0 else 1 end as cumulative_manual_transfer,
   c.payment_account_iban,
   1 as fl_new
FROM dm_debt_collection.coba_manual_payment_due_match_customer c
LEFT JOIN master.subscription_payment sp 
   ON c.order_id=sp.order_id
LEFT JOIN customer_logic lc 
   ON c.customer_id=lc.customer_id
where TRUE 
    AND c.amount_due_match = 1
    and sp.paid_date is null
    and sp.status not in('PLANNED','HELD','NEW')
    and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
    AND c.amount_due_match_logic='0.5 logic order' 
    AND lc.customer_id IS NULL 
    )
    SELECT * FROM a
    WHERE TRUE
    QUALIFY count(payment_id)over(Partition BY payment_id)=1;-----ensuring unique payments to avoid duplicates
 

GRANT SELECT ON dm_debt_collection.coba_manual_payment_order_id_mapping TO tableau;






   
