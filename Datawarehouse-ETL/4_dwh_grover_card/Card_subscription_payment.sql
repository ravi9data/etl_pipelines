WITH MAU AS (
	SELECT 
		distinct date_trunc('month',event_timestamp)::Date AS purchase_month,
		customer_id
	WHERE event_name ='payment-successful'
	AND transaction_type ='PURCHASE'
	ORDER BY 1)
,sub_payment AS (
    SELECT 	
    	*
SELECT 
	sp.*,m.purchase_month 
FROM sub_payment sp 
LEFT JOIN mau m 
ON sp.customer_id =m.customer_id 
AND date_trunc('month',due_date)::Date=m.purchase_month::date
WITH NO SCHEMA BINDING;

