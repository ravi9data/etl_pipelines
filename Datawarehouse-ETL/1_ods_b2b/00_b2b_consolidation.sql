DROP TABLE IF EXISTS ods_b2b.consolidation_date;
CREATE TABLE ods_b2b.consolidation_date AS 
SELECT 
external_customer AS customer_id,
strategy AS stategy,
chosen_billing_day AS consolidation_day,
created_at AS createdat, 
id, 
chosen_billing_day AS chosenbillingday, 
strategy,
uuid, 
external_customer AS externalcustomer,
updated_at AS updatedat, 
1 AS idx
FROM oltp_billing.wallet 