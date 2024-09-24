
CREATE OR REPLACE VIEW dm_marketing.v_crm_email_subscribed_customers AS
SELECT 
	"date" AS reporting_date,
	COALESCE(shipping_country, billing_country) AS country,
	email_subscribe,	
	count(DISTINCT customer_id) AS customers
FROM master.customer_historical ch 
WHERE "date" > DATEADD('month',-13,date_trunc('month', current_date))
GROUP BY 1,2,3
WITH NO SCHEMA BINDING;