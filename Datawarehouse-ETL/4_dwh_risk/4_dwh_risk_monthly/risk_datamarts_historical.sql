--Customer 
DELETE FROM dm_risk.customer_datamart_historical
WHERE date = current_date;

INSERT INTO dm_risk.customer_datamart_historical 
SELECT 
*,
'V1' as version,
current_date as date 
FROM dm_risk.customer_datamart_v1;



--Order
DELETE FROM dm_risk.order_datamart_historical
WHERE date = current_date;

INSERT INTO dm_risk.order_datamart_historical 
SELECT 
*,
'V1' as version,
current_date as date 
FROM dm_risk.order_datamart_v1;


--Subscription
DELETE FROM dm_risk.subscription_datamart_historical
WHERE date = current_date;

INSERT INTO dm_risk.subscription_datamart_historical
SELECT 
*,
'V1' as version,
current_date as date 
FROM dm_risk.subscription_datamart_v1;
