/*
 * Summerizes all the payments 
 * */
DROP TABLE IF EXISTS dm_debt_collection.coba_auto_payments_sp_ids;
CREATE TABLE dm_debt_collection.coba_auto_payments_sp_ids AS
SELECT 
DISTINCT
current_date AS execution_date ,
coalesce(sp.order_id ,sf.order_id) AS order_id,
coalesce(sf.customer_id,sp.customer_id) AS customer_id,
sp.payment_sfid,
sf.paid_amount ,
sp.amount_due ,
sf.payment_account_iban,
'Auto' AS booking_method
FROM dm_debt_collection.coba_payment_booking_on_sf sf
LEFT JOIN master.subscription_payment sp 
ON sf.id = sp.payment_id 
;
;

-----Historize the table 
DELETE FROM dm_debt_collection.auto_payments_sp_ids_historical 
WHERE  execution_date = current_date;

INSERT INTO dm_debt_collection.auto_payments_sp_ids_historical 
SELECT 
execution_date 
,order_id   
,customer_id   
,payment_sfid  
,paid_amount   
,amount_due 
,booking_method
FROM dm_debt_collection.coba_auto_payments_sp_ids;

GRANT SELECT ON dm_debt_collection.coba_auto_payments_sp_ids TO tableau;

GRANT SELECT ON dm_debt_collection.auto_payments_sp_ids_historical TO tableau;


