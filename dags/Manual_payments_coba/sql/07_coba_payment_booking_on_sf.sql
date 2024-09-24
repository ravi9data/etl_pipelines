/*
 * This table is where all automated payments are pushing to SF
 * */
drop table if exists dm_debt_collection.coba_payment_booking_on_sf;
create table dm_debt_collection.coba_payment_booking_on_sf AS
    select 
        distinct
        execution_date,
        report_date,
        payment_id as id,
        current_date as paid_date,
        'PAID' as status,
        amount_due as paid_amount,
        order_id,
        customer_id, 
        payment_account_iban,
        'Manual Transfer' as paid_reason,
        'Production' as sf_environment
    FROM  dm_debt_collection.coba_manual_payment_order_id_mapping a;

DELETE FROM dm_debt_collection.payment_booking_on_sf_historical 
WHERE  execution_date = current_date;

INSERT INTO dm_debt_collection.payment_booking_on_sf_historical 
SELECT 
    execution_date 
    ,report_date   
    ,id 
    ,paid_date  
    ,status  
    ,paid_amount   
    ,order_id   
    ,customer_id   
    ,payment_account_iban 
    ,paid_reason   
    ,sf_environment
FROM dm_debt_collection.coba_payment_booking_on_sf AS cpbost;

GRANT SELECT ON dm_debt_collection.coba_payment_booking_on_sf TO tableau;
GRANT SELECT ON dm_debt_collection.payment_booking_on_sf_historical TO tableau;
