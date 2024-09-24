/*
 * This Table creates Maps the customer name and reconciles it with the system name from customer PII using levenshtein_distance.
 * It also creates a FLAG passthrough which classifies the entried based on the availablity of the data  
 *  */
drop table if exists dm_debt_collection.coba_manual_payments_orders;
create table dm_debt_collection.coba_manual_payments_orders AS
SELECT
    a.execution_date,
    a.report_extract_number,
    a.report_date,
    a.arrival_date,
    a.booking_date,
    cp.customer_id,
    a.order_id,
    a.amount,
    a.currency,
    a.customer_name,
    a.confidence_on_order_id,
    a.reference,
    UPPER(cp.first_name || ' ' || cp.last_name) as customer_name_system,
    a.customer_name as reported_customer_name,
    CASE
        WHEN UPPER(cp.first_name) = split_part(customer_name, ' ', 1) then 1
        else 0
    end as fl_first_name_match,
    CASE
        WHEN UPPER(cp.last_name) = UPPER(
            split_part(
                customer_name,
                ' ',
                REGEXP_COUNT(customer_name, ' ') + 1
            )
        ) then 1
        else 0
    end as fl_last_name_match,
    CASE
        when UPPER(reported_customer_name) = UPPER(customer_name_system) then 1
        else 0
    end as fl_name_match,
    levenshtein(reported_customer_name, customer_name_system) as levenshtein_distance_name,
    payment_account_iban,
    case
        when a.customer_id IS NULL THEN 'CUSTOMER_ID_NOT FOUND'
        when confidence_on_order_id = 'HIGH'
        AND levenshtein_distance_name = 0 THEN 'CNF_HIGH_LEV0'
        when confidence_on_order_id = 'HIGH'
        AND (
            fl_last_name_match = 1
            or fl_first_name_match = 1
        ) THEN 'CNF_HIGH_NAME_PARTIAL_MATCH'
        when confidence_on_order_id = 'HIGH' THEN 'CNF_HIGH_NO_NAME_MATCH'
        WHEN confidence_on_order_id = 'MEDIUM'
        AND levenshtein_distance_name = 0 THEN 'CNF_MEDIUM_NAME_MATCH'
        when confidence_on_order_id = 'MEDIUM'
        AND (
            fl_last_name_match = 1
            or fl_first_name_match = 1
        ) THEN 'CNF_MEDIUM_NAME_PARTIAL_MATCH'
        when confidence_on_order_id = 'MEDIUM' THEN 'CNF_MEDIUM_NO_NAME_MATCH'
        when confidence_on_order_id = 'NO' THEN 'NO_ORDER_ID_FOUND'
        else 'ERROR'
    end as pass_through,
     count(reference) over(PARTITION BY order_id) AS no_of_payments_per_orders,--  number of payments per ORDER id 
     ROW_number()over(PARTITION BY order_id) AS idx_payments_per_orders, ---idx of payments under one order_id
     count(reference) over(PARTITION BY reference) AS no_of_payments_per_reference,--  number of payments per reference
     ROW_number()over(PARTITION BY order_id) AS idx_payments_per_reference, ---idx of payments under one order_id
     count(a.customer_id)over(PARTITION BY a.customer_id) AS no_of_payments_per_customer -- number of payments under one customer_id
FROM
    dm_debt_collection.coba_manual_payments_raw a
    left join ods_data_sensitive.customer_pii cp on cp.customer_id = a.customer_id;
 

-----Historize the table 
DELETE FROM dm_debt_collection.coba_manual_payments_order_confidence_historical 
WHERE  execution_date = current_date;

INSERT INTO dm_debt_collection.coba_manual_payments_order_confidence_historical 
SELECT 
    execution_date 
    ,report_extract_number  
    ,report_date   
    ,arrival_date  
    ,booking_date  
    ,order_id
    ,customer_id     
    ,amount  
    ,currency   
    ,customer_name 
    ,confidence_on_order_id 
    ,reference  
    ,payment_account_iban 
    ,pass_through  
FROM dm_debt_collection.coba_manual_payments_orders AS cmpot;

GRANT SELECT ON dm_debt_collection.coba_manual_payments_orders TO tableau;

GRANT SELECT ON dm_debt_collection.coba_manual_payments_order_confidence_historical TO tableau;
