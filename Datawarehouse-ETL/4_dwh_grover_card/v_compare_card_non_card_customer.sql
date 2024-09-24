WITH ASV AS (
	SELECT 
		customer_id ,
		active_subscriptions,
		active_subscription_value ,
		active_committed_subscription_value ,
		fact_date  
	FROM ods_finance.active_subscriptions_overview aso 
		WHERE (fact_date =last_day(fact_date) or fact_date= current_date)
		AND fact_date >='2021-01-01'
		)
	SELECT 
		c.customer_id ,
		'card_customer' AS customer_classification,
		c.created_at,
		cp.age,
		date_trunc('MONTH',customer_acquisition_cohort) AS customer_acquisition_cohort  ,
		CASE WHEN cc.customer_id IS NOT NULL THEN 1 ELSE 0 END AS card_customer,
		date_trunc('MONTH',first_card_created_date) AS first_card_created_date,
		NULL AS indx
	FROM master.customer c 
	LEFT JOIN ods_data_sensitive.customer_pii cp
		ON c.customer_id =cp.customer_id
		ON c.customer_id=cc.customer_id
	WHERE c.customer_type ='normal_customer'
		AND c.shipping_country ='Germany'
		AND card_customer =1
		AND date_trunc('MONTH',customer_acquisition_cohort) ::date >= ('2021-01-01')
--		AND date_trunc('MONTH',customer_acquisition_cohort) ::date between '2021-06-01' and '2022-01-01'
--		AND date_trunc('MONTH',first_card_created_date)::Date IN ('2022-06-01')
	UNION 
	(
	WITH A AS (
	SELECT 
		c.customer_id ,
		'non_card_customer' AS customer_classification,
		c.created_at,
		cp.age,
		date_trunc('MONTH',customer_acquisition_cohort) AS customer_acquisition_cohort  ,
		CASE WHEN cc.customer_id IS NOT NULL THEN 1 ELSE 0 END AS card_customer,
		date_trunc('MONTH',first_card_created_date) AS first_card_created_date,
		ROW_NUMBER ()OVER (PARTITION BY date_trunc('MONTH',customer_acquisition_cohort)) indx
	FROM master.customer c 
	LEFT JOIN ods_data_sensitive.customer_pii cp
		ON c.customer_id =cp.customer_id
		ON c.customer_id=cc.customer_id
		WHERE c.customer_type ='normal_customer'
		AND c.shipping_country ='Germany'
		AND card_customer =0
		AND date_trunc('MONTH',customer_acquisition_cohort) ::date >= ('2021-01-01')
		--AND date_trunc('MONTH',customer_acquisition_cohort) ::date between '2021-06-01' and '2022-01-01'
		--AND crm_label ='Active'
		)
		SELECT * FROM a 
)
	)
SELECT 
	date_trunc('month', first_card_created_date) AS card_created_month,
	fact_date,
	customer_classification,
	customer_acquisition_cohort,
	card_customer,
	age,
	gc.customer_id ,
	indx,
	sum(active_subscriptions) AS active_subscriptions ,
	sum(active_subscription_value) AS active_subscription_value ,
	sum(active_committed_subscription_value) AS active_committed_subscription_value
	--CASE WHEN first_timestamp_payment_successful::Date<=fact_date::Date THEN TRUE ELSE FALSE END activated_user,
	--RANK()OVER (PARTITION BY date_trunc('month',first_timestamp_payment_successful),gc.customer_id ORDER BY fact_date) AS first_fact_date 
Left JOIN ASV ch 
	ON ch.customer_id =gc.customer_id
WHERE fact_date<=date_trunc('month',current_date)-1 
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 1 DESC, 2 DESC,3
WITH NO SCHEMA BINDING;


