WITH 
 dates AS (
 SELECT datum,day_is_first_of_month ,week_day_number  FROM public.dim_dates dd 
 WHERE datum BETWEEN '2021-04-20' AND current_date -1)
,card_cust AS 
(
SELECT 
		customer_id,
		first_card_created_date,
		first_timestamp_payment_successful
FROM
WHERE
	first_card_created_date IS NOT NULL
)
,asv_pre AS (
	SELECT 
	fact_date,
	day_is_end_of_week,
	day_is_last_of_month,
	customer_id, 
	sum(active_subscriptions) AS active_subscriptions, 
	sum(active_subscription_value) AS active_subscription_value
	FROM ods_finance.active_subscriptions_overview aso
	GROUP BY 1,2,3,4
	ORDER BY 1)
,asv AS (
	SELECT 
		datum,
		day_is_end_of_week,
		day_is_last_of_month,
		first_card_created_date,
		first_timestamp_payment_successful,
		CASE WHEN datum>=first_timestamp_payment_successful::Date THEN TRUE ELSE FALSE END AS is_activated,
		CASE WHEN datum>=first_card_Created_date::Date AND cc.customer_id IS NULL THEN cc.customer_id ELSE cc.customer_id END AS customer_id,
		CASE WHEN datum>=first_card_Created_date::Date AND active_subscriptions IS NULL THEN 0 ELSE active_subscriptions END AS active_subscriptions,
		CASE WHEN datum>=first_card_Created_date::Date AND active_subscription_value IS NULL THEN 0 ELSE active_subscription_value END AS active_subscription_value,
		ROW_NUMBER()OVER (PARTITION BY cc.customer_id ORDER BY datum) idx
	FROM dates d 
	CROSS JOIN card_cust cc 
	LEFT JOIN asv_pre a 
		ON d.datum=a.fact_date 
		and a.customer_id=cc.customer_id
	WHERE true
	AND datum>=first_card_Created_date::Date 
	)
,before_card AS (
	SELECT datum,a.customer_id,first_card_Created_date,first_timestamp_payment_successful,active_subscriptions,active_subscription_value
	FROM dates d 
	CROSS JOIN (SELECT customer_id,first_card_Created_date,first_timestamp_payment_successful,active_subscriptions,active_subscription_value FROM asv WHERE idx=1) a 
	WHERE datum>=first_card_Created_date::Date)
SELECT 
	bc.datum AS fact_date,
	day_is_end_of_week,
	day_is_last_of_month,
	bc.customer_id AS customer_id_bc,
	--ac.customer_id AS customer_id_ac,
	bc.first_card_Created_date,
	bc.first_timestamp_payment_successful,
	ac.is_activated,
	bc.active_subscriptions AS active_subscriptions_before_card ,
	bc.active_subscription_value AS active_subscription_value_before_card,
	ac.active_subscriptions,
	ac.active_subscription_value ,
	(ac.active_subscriptions-bc.active_subscriptions) AS incremental_active_subscriptions,
	(ac.active_subscription_value-bc.active_subscription_value) AS incremental_active_subscription_value
	FROM before_card bc 
	LEFT JOIN asv ac 
		ON bc.datum=ac.datum
		AND bc.customer_id=ac.customer_id;

