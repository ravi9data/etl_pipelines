WITH GP_cust AS (
SELECT 
DISTINCT customer_id,min(event_timestamp) AS first_google_pay_date
WHERE event_name ='payment-successful'
AND wallet_type='GOOGLE'
GROUP BY 1)
	SELECT
		gt.customer_id,
		payload_id ,
		first_google_pay_date,
		datediff('month',first_google_pay_date,event_timestamp) AS cohort,
		event_timestamp ,
		amount_transaction ,
		wallet_type,
		CASE WHEN event_timestamp >first_google_pay_date THEN 1 ELSE 0 END is_after_GP
	INNER JOIN GP_cust gp 
		ON gt.customer_id =gp.customer_id
	WHERE TRUE 
		and event_name ='payment-successful'
WITH NO SCHEMA binding;


