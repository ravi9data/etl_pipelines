CREATE OR REPLACE VIEW dm_weekly_monthly.v_weekly_monthly_subscription_churn AS 
-- weekly and monthly - subscription churn
SELECT 
	'Weekly'::varchar AS report,
	date_trunc('week', s.cancellation_date)::date AS cancellation_date,
	s.country_name,
	s.cancellation_reason_churn AS cancellation_reason_churn_group,
	CASE WHEN cancellation_reason_churn = 'customer request' AND cancellation_reason_new = 'SOLD 1-EUR' THEN 'customer request - Sold 1 EUR'
		 WHEN cancellation_reason_churn = 'customer request' AND cancellation_reason_new = 'SOLD EARLY' THEN 'customer request - Sold Early'
		 WHEN cancellation_reason_churn = 'customer request' THEN 'customer request - Other'
		 WHEN cancellation_reason_churn = 'failed delivery' THEN 'no delivery'
		 ELSE cancellation_reason_churn END AS cancellation_reason_churn,
	CASE WHEN cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION', 'CANCELLED BEFORE ALLOCATION - OTHERS') THEN 'BEFORE ALLOCATION - OTHERS'
		 WHEN cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST') THEN 'BEFORE ALLOCATION - CUSTOMER REQUEST'
		 WHEN cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION - PROCUREMENT') THEN 'BEFORE ALLOCATION - PROCUREMENT'
		 ELSE cancellation_reason_new END AS cancellation_reason_new,
	count(DISTINCT s.subscription_id) AS cancelled_subscriptions,
	sum(s.subscription_value_euro) AS subscription_value_euro
FROM master.subscription s 
WHERE date_trunc('week', s.cancellation_date) >= date_trunc('week',dateadd('week',-6,current_date))
	AND s.cancellation_date::date <= current_date
GROUP BY 1,2,3,4,5,6
--
UNION 
--
SELECT 
	'Monthly'::varchar AS report,
	date_trunc('month', s.cancellation_date)::date AS cancellation_date,
	s.country_name,
	s.cancellation_reason_churn AS cancellation_reason_churn_group,
	CASE WHEN cancellation_reason_churn = 'customer request' AND cancellation_reason_new = 'SOLD 1-EUR' THEN 'customer request - Sold 1 EUR'
		 WHEN cancellation_reason_churn = 'customer request' AND cancellation_reason_new = 'SOLD EARLY' THEN 'customer request - Sold Early'
		 WHEN cancellation_reason_churn = 'customer request' THEN 'customer request - Other'
		 WHEN cancellation_reason_churn = 'failed delivery' THEN 'no delivery'
		 ELSE cancellation_reason_churn END AS cancellation_reason_churn,
	CASE WHEN cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION', 'CANCELLED BEFORE ALLOCATION - OTHERS') THEN 'BEFORE ALLOCATION - OTHERS'
		 WHEN cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST') THEN 'BEFORE ALLOCATION - CUSTOMER REQUEST'
		 WHEN cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION - PROCUREMENT') THEN 'BEFORE ALLOCATION - PROCUREMENT'
		 ELSE cancellation_reason_new END AS cancellation_reason_new,
	count(DISTINCT s.subscription_id) AS cancelled_subscriptions,
	sum(s.subscription_value_euro) AS subscription_value_euro
FROM master.subscription s 
WHERE date_trunc('month', s.cancellation_date) >= date_trunc('month',dateadd('month',-14,current_date))
	AND s.cancellation_date::date <= current_date
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_weekly_monthly.v_weekly_monthly_subscription_churn TO tableau;