DROP TABLE IF EXISTS tmp_weekly_monthly_subscription_churn;
CREATE TEMP TABLE tmp_weekly_monthly_subscription_churn
SORTKEY(report, cancellation_date, country_name, cancellation_reason_churn_group, cancellation_reason_churn, cancellation_reason_new)
DISTKEY(cancellation_date)
AS
-- weekly and monthly - subscription churn
SELECT 
	'Weekly'::varchar AS report,
	date_trunc('week', s.cancellation_date)::date AS cancellation_date,
	s.country_name,
	s.cancellation_reason_churn AS cancellation_reason_churn_group,
	CASE WHEN s.cancellation_reason_churn = 'customer request' AND s.cancellation_reason_new = 'SOLD 1-EUR' THEN 'customer request - Sold 1 EUR'
		 WHEN s.cancellation_reason_churn = 'customer request' AND s.cancellation_reason_new = 'SOLD EARLY' THEN 'customer request - Sold Early'
		 WHEN s.cancellation_reason_churn = 'customer request' THEN 'customer request - Other'
		 WHEN s.cancellation_reason_churn = 'failed delivery' THEN 'no delivery'
		 ELSE s.cancellation_reason_churn END AS cancellation_reason_churn,
	CASE WHEN s.cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION', 'CANCELLED BEFORE ALLOCATION - OTHERS') THEN 'BEFORE ALLOCATION - OTHERS'
		 WHEN s.cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST') THEN 'BEFORE ALLOCATION - CUSTOMER REQUEST'
		 WHEN s.cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION - PROCUREMENT') THEN 'BEFORE ALLOCATION - PROCUREMENT'
		 ELSE s.cancellation_reason_new END AS cancellation_reason_new,
	CASE WHEN COALESCE(c.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
	     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
	     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
	     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
	     ELSE 'n/a' END AS customer_type_freelancer_split,
	count(DISTINCT s.subscription_id) AS cancelled_subscriptions,
	sum(s.subscription_value_euro) AS subscription_value_euro
FROM master.subscription s 
LEFT JOIN master.customer c 
	ON s.customer_id=c.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name
WHERE date_trunc('week', s.cancellation_date) >= date_trunc('week',dateadd('week',-6,current_date))
	AND s.cancellation_date::date <= current_date
GROUP BY 1,2,3,4,5,6,7
--
UNION 
--
SELECT 
	'Monthly'::varchar AS report,
	date_trunc('month', s.cancellation_date)::date AS cancellation_date,
	s.country_name,
	s.cancellation_reason_churn AS cancellation_reason_churn_group,
	CASE WHEN s.cancellation_reason_churn = 'customer request' AND s.cancellation_reason_new = 'SOLD 1-EUR' THEN 'customer request - Sold 1 EUR'
		 WHEN s.cancellation_reason_churn = 'customer request' AND s.cancellation_reason_new = 'SOLD EARLY' THEN 'customer request - Sold Early'
		 WHEN s.cancellation_reason_churn = 'customer request' THEN 'customer request - Other'
		 WHEN s.cancellation_reason_churn = 'failed delivery' THEN 'no delivery'
		 ELSE s.cancellation_reason_churn END AS cancellation_reason_churn,
	CASE WHEN s.cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION', 'CANCELLED BEFORE ALLOCATION - OTHERS') THEN 'BEFORE ALLOCATION - OTHERS'
		 WHEN s.cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST') THEN 'BEFORE ALLOCATION - CUSTOMER REQUEST'
		 WHEN s.cancellation_reason_new IN ('CANCELLED BEFORE ALLOCATION - PROCUREMENT') THEN 'BEFORE ALLOCATION - PROCUREMENT'
		 ELSE s.cancellation_reason_new END AS cancellation_reason_new,
	CASE WHEN COALESCE(c.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
	     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
	     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
	     WHEN COALESCE(c.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
	     ELSE 'n/a' END AS customer_type_freelancer_split,
	count(DISTINCT s.subscription_id) AS cancelled_subscriptions,
	sum(s.subscription_value_euro) AS subscription_value_euro
FROM master.subscription s 
LEFT JOIN master.customer c 
	ON s.customer_id=c.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name
WHERE date_trunc('month', s.cancellation_date) >= date_trunc('month',dateadd('month',-14,current_date))
	AND s.cancellation_date::date <= current_date
GROUP BY 1,2,3,4,5,6,7;


BEGIN TRANSACTION;

DROP TABLE IF EXISTS dwh.weekly_monthly_subscription_churn;
CREATE TABLE dwh.weekly_monthly_subscription_churn AS
SELECT *
FROM tmp_weekly_monthly_subscription_churn;

END TRANSACTION;

GRANT SELECT ON dwh.weekly_monthly_subscription_churn TO tableau;
