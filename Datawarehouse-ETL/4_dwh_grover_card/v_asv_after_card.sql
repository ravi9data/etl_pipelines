WITH pre_asv as(
SELECT 
DISTINCT FACT_DATE,
gc2.customer_id ,
	gc2.first_timestamp_payment_successful,
	gc2.first_card_created_date ,
	aso.active_subscription_value,
	RANK() OVER ( PARTITION BY date_trunc('MONTH',FACT_DATE),gc2.customer_id  ORDER BY fact_date) AS date_rank, 
	CASE WHEN date_trunc('day',fact_date)>=date_trunc('month',first_timestamp_payment_successful) THEN TRUE ELSE FALSE END AS MAU
FROM (SELECT distinct customer_id,
			first_card_created_date,
			active_card_first_event,
			first_timestamp_payment_successful 
		WHERE active_card_first_event ='created')gc2
INNER JOIN (SELECT fact_date
			,customer_id
			,sum(active_subscriptions) AS active_subscriptions
			,sum(active_subscription_value ) AS active_subscription_value
			,sum(active_committed_subscription_value) AS active_committed_subscription_value
			FROM ods_finance.active_subscriptions_overview aso 
			--WHERE fact_date =date_trunc('month',fact_date) 
			--WHERE customer_id ='932342' and fact_month ='2021-05-01' 
			GROUP BY 1,2) aso
on aso.customer_id=gc2.customer_id
AND gc2.first_card_created_date::Date<=aso.fact_date::Date
AND fact_date<=current_date
--WHERE gc2.customer_id ='932342'
)
,ASV AS (
select 
DISTINCT 
	date_trunc('month',fact_date) AS sub_months, 
	fact_date,
	customer_id ,
	first_timestamp_payment_successful,
	first_card_created_date ,
	active_subscription_value AS  active_subscription_value_after_card,
	MAU
	FROM pre_asv
	WHERE date_rank =1
		)
,cash AS(
SELECT 
	distinct
		date_trunc('month',event_timestamp) AS cash_months,
		gc.customer_id,
		WHERE user_classification='Card_user'
		AND event_name='Redemption'
		GROUP BY 1,2)
, MAU AS (
	SELECT 
		distinct date_trunc('month',event_timestamp)::Date AS purchase_month,
		customer_id 
	WHERE event_name ='payment-successful'
	AND transaction_type ='PURCHASE'
	ORDER BY 1)
--,FINAL AS (
SELECT 
	distinct
	sub_months,
	fact_date,
	cash_months,
	first_card_created_date ,
	s.customer_id ,
	m.customer_id,
	first_timestamp_payment_successful ,
	active_subscription_value_after_card ,
	CASE WHEN m.customer_id IS NOT NULL THEN TRUE ELSE FALSE END AS MAU
FROM ASV s
LEFT JOIN cash c 
	ON s.sub_months=c.cash_months
	and s.customer_id =c.customer_id
LEFT JOIN MAU m 
	ON s.sub_months =m.purchase_month
	AND s.customer_id=m.customer_id 
ORDER BY 1,2
WITH NO SCHEMA BINDING;


