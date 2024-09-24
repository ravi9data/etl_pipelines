/*
 * This Table focuses on all the manual payments, where then paid amount either does not match the due amount or customer provided reference is invalid 
 */
drop table if exists dm_debt_collection.coba_manual_payment_manual_review;
create table dm_debt_collection.coba_manual_payment_manual_review AS
with a as
    (
    select
        a.report_date,
        a.arrival_date,
        a.booking_date,
        a.customer_id,
        a.order_id,
        a.reference ,
        a.currency ,
        a.payment_account_iban,
        amount_due_match_logic,
        a.confidence_on_order_id,
        a.customer_name,
        sum(a.amount) AS amount
    from
        dm_debt_collection.coba_manual_payments_orders a
        LEFT JOIN dm_debt_collection.coba_manual_payment_due_match_customer AS due
        ON a.reference=due.reference
    where 
       ( amount_due_match_logic='No logic' 
        OR (a.order_id not in (select distinct order_id from dm_debt_collection.coba_manual_payment_order_id_mapping WHERE amount_due_match_logic<>'0.5 logic customer') ---Excluding orders which are present in order mapping (automated)
         ----Exlucding customers which are present in order mapping,sometimes the order id in reference and in payments can change while mapping,
        and a.customer_id NOT IN (select distinct customer_id from dm_debt_collection.coba_manual_payment_order_id_mapping WHERE amount_due_match_logic='0.5 logic customer')) 
        or a.pass_through in ('ERROR','NO_ORDER_ID_FOUND','CUSTOMER_ID_NOT FOUND'))  
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11
    ),
b as---customer level payments 
    (select 
        customer_id ,
        sum(amount_due) as due_amount
    from
        master.subscription_payment as sp
    where 
        paid_date is null
        and status not in('PLANNED','HELD','NEW')
        and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
    group by 
        1) ,
c as ---- order level payments 
    (select 
        order_id ,
        sum(amount_due) as due_amount
    from
        master.subscription_payment as sp
    where 
        paid_date is null
        and status not in('PLANNED', 'HELD','NEW')
        and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
    group by 
        1)
,d as ----order level planned payments
    (select 
        order_id ,
        sum(amount_due) as due_amount
    from
        master.subscription_payment as sp
    where 
        paid_date is null
        and status IN ('PLANNED')
        and len(sp.payment_id) <> 36 ---to exclude migrated sub payments 
       -- AND  customer_id='1519437'
    group by 
        1) 
select  
    current_date as execution_date,
    aa.*, 
    COALESCE(cc.due_amount,dd.due_amount,bb.due_amount) as order_id_due_amount,
    bb.due_amount as total_due,
    0 as fl
from 
    a aa
        left join
        b bb 
        on aa.customer_id = bb.customer_id
        left join
        c cc 
        on aa.order_id = cc.order_id
        LEFT JOIN 
        d dd 
        ON aa.order_id=dd.order_id;

-----Historize the table 
DELETE FROM dm_debt_collection.coba_manual_payment_manual_review_historical 
WHERE  execution_date = current_date;

INSERT INTO dm_debt_collection.coba_manual_payment_manual_review_historical 
SELECT 
    execution_date 
    ,report_date   
    ,arrival_date  
    ,booking_date  
    ,customer_id   
    ,order_id   
    ,reference  
    ,currency   
    ,amount_due_match_logic 
    ,customer_name 
    ,payment_account_iban 
    ,amount  
    ,order_id_due_amount 
    ,total_due  
FROM dm_debt_collection.coba_manual_payment_manual_review;

GRANT SELECT ON  dm_debt_collection.coba_manual_payment_manual_review TO tableau;

GRANT SELECT ON  dm_debt_collection.coba_manual_payment_manual_review_historical TO tableau;
