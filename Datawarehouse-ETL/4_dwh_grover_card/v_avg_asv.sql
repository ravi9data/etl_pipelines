WITH ASV AS (
	SELECT 
		customer_id ,
		active_subscriptions,
		active_subscription_value ,
		fact_date  
	FROM ods_finance.active_subscriptions_overview aso 
		WHERE (fact_date =last_day(fact_date) or fact_date= current_date)
		AND fact_date >='2020-02-01'
		)
	SELECT 
		customer_id ,
		first_card_created_date
		,total_cards
		,cc.first_timestamp_payment_successful 
		WHERE first_card_created_date IS NOT NULL)
SELECT 
	date_trunc('month', first_card_created_date) AS card_created_month,
	fact_date,
	gc.first_timestamp_payment_successful,
	gc.customer_id ,
	sum(active_subscriptions) AS active_subscriptions ,
	sum(active_subscription_value) AS active_subscription_value ,
	CASE WHEN first_timestamp_payment_successful::Date<=fact_date::Date THEN TRUE ELSE FALSE END activated_user
Left JOIN ASV ch 
	ON ch.customer_id =gc.customer_id
GROUP BY 1,2,3,4
ORDER BY 1 DESC, 2 DESC,3
WITH NO SCHEMA BINDING;


