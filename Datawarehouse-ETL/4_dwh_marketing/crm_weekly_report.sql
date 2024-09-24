DROP TABLE IF EXISTS dwh.crm_weekly_report;
CREATE TABLE dwh.crm_weekly_report AS 
WITH countries_sessions AS (
	SELECT  
		customer_id,
		CASE WHEN store_name IN ('Austria', 'Spain', 'Germany', 'Netherlands', 
								'United States' ) THEN store_name
			WHEN geo_country = 'AT' THEN 'Austria'
			WHEN geo_country = 'ES' THEN 'Spain'
			WHEN geo_country = 'DE' THEN 'Germany'
			WHEN geo_country = 'NL' THEN 'Netherlands'
			WHEN geo_country = 'US' THEN 'United States' END AS country,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY session_start) AS row_sessions 
	FROM traffic.sessions
	WHERE country IS NOT NULL
		AND customer_id IS NOT NULL
)
, a AS (
	SELECT DISTINCT  
		"date",
		DATE_TRUNC('week',"date") as week_date,
		c.customer_id,
		customer_type,
		created_at,
		customer_acquisition_cohort,
		start_date_of_first_subscription,
		crm_label,
		active_subscriptions,
		email_subscribe,
		CASE WHEN c.signup_country <> 'never_add_to_cart' THEN c.signup_country
			ELSE s.country END AS country
	FROM master.customer_historical c 
	LEFT JOIN public.dim_dates d 
		ON d.datum::date=c."date"::date
	LEFT JOIN countries_sessions s
		ON c.customer_id = s.customer_id
		AND row_sessions = 1
	WHERE day_name = 'Sunday'
		AND "date" > CURRENT_DATE - 60
)
,dc_churn_customers AS (
	SELECT 
		customer_id, 
		DATE_TRUNC('week',cancellation_date::date) AS cancellation_date,
		true AS is_dc
	FROM master.subscription
	WHERE cancellation_reason_new = 'DEBT COLLECTION'
	GROUP BY 1,2
	ORDER BY 2 desc 
)
,prep AS (
	SELECT 
		a.*, coalesce(is_dc,false) AS is_dc,
		lag(crm_label) over (partition by a.customer_id order by date) AS previous_month_status,
		lag(active_subscriptions) over (partition by a.customer_id order by date) AS previous_month_active_subscriptions
	FROM a
	LEFT JOIN dc_churn_customers dc 
		ON dc.customer_id = a.customer_id 
	     AND a.week_date = dc.cancellation_date
)
,agg AS (
	SELECT DISTINCT 
		date_trunc('week',date)::date AS bom,
		date::date as eom,
		customer_type,
		country,
		----
		COUNT(DISTINCT CASE WHEN previous_month_status in ('Registered') THEN customer_id END) AS registered_customers_bom,
		COUNT(DISTINCT CASE WHEN previous_month_status in ('Passive') THEN customer_id END) AS Passive_customers_bom,
		COUNT(DISTINCT CASE WHEN previous_month_status in ('Active') THEN customer_id END) AS Active_customers_bom,
		COUNT(DISTINCT CASE WHEN previous_month_status in ('Lapsed') THEN customer_id END) AS Lapsed_customers_bom,
		COUNT(DISTINCT CASE WHEN previous_month_status in ('Inactive') THEN customer_id END) AS Inactive_customers_bom,
		COUNT(DISTINCT CASE WHEN previous_month_status is null THEN customer_id END) AS crm_label_na_bom,
		----
		count(distinct customer_id) AS total_customers_eom,
		COUNT(DISTINCT CASE WHEN email_subscribe in ('Opted In') THEN customer_id END) AS total_customers_subscribed_to_newsletter_eom,
		COUNT(DISTINCT CASE WHEN crm_label in ('Registered') THEN customer_id END) AS registered_customers_eom,
		COUNT(DISTINCT CASE WHEN crm_label in ('Passive') THEN customer_id END) AS Passive_customers_eom,
		COUNT(DISTINCT CASE WHEN crm_label in ('Active') THEN customer_id END) AS Active_customers_eom,
		COUNT(DISTINCT CASE WHEN crm_label in ('Lapsed') THEN customer_id END) AS Lapsed_customers_eom,
		COUNT(DISTINCT CASE WHEN crm_label in ('Inactive') THEN customer_id END) AS Inactive_customers_eom,
		COUNT(DISTINCT CASE WHEN crm_label is null THEN customer_id END) AS crm_label_na,
		----
		COUNT(DISTINCT CASE WHEN active_subscriptions>1 
						 THEN customer_id END
				) AS Active_customers_w_multiple_subscriptions_eom,
		---
		COUNT(DISTINCT CASE WHEN active_subscriptions >previous_month_active_subscriptions 
						 AND previous_month_active_subscriptions>0
						 THEN customer_id END
				) AS upsell_customers,
		---
		---
		COUNT(DISTINCT CASE WHEN crm_label IN ('Active') 
						 AND coalesce(previous_month_status,'Registered') IN ('Registered') 
						THEN customer_id END
				) AS activated_customers,
		COUNT(DISTINCT CASE WHEN crm_label IN ('Active') 
						 AND previous_month_status IN ('Inactive') 
						THEN customer_id END
				) AS reactivated_customers,
		COUNT(DISTINCT CASE WHEN crm_label IN ('Active') 
						 AND previous_month_status IN ('Lapsed') 
						THEN customer_id END
				) AS Winback_customers,
		COUNT(DISTINCT CASE WHEN crm_label IN ('Inactive') 
						 AND previous_month_status IN ('Active') 
						THEN customer_id END
				) AS gross_churn_customers,
		COUNT(DISTINCT CASE WHEN crm_label IN ('Inactive') 
						 AND previous_month_status IN ('Active') 
						 AND is_dc is true
						THEN customer_id END
				) AS dc_customers
	FROM prep
	GROUP BY 1,2,3,4
)
--
,registered AS (
	SELECT 
		date_trunc('week',created_at)::date as fact_date,
		customer_type,
		CASE WHEN c.signup_country <> 'never_add_to_cart' THEN c.signup_country
			ELSE s.country END AS country,
		COUNT(DISTINCT c.customer_id) AS registered_customers,
		COUNT(DISTINCT CASE WHEN email_subscribe = 'Opted In' then c.customer_id end) AS subscribed_to_newsletter_at_signup,
		COUNT(DISTINCT CASE WHEN completed_orders>=1 then c.customer_id end) AS customers_with_submitted_orders,
		COUNT(DISTINCT CASE WHEN start_date_of_first_subscription is not null then c.customer_id end) AS customers_with_paid_orders
	FROM master.customer c
	LEFT JOIN countries_sessions s
		ON c.customer_id = s.customer_id
		AND row_sessions = 1
	GROUP BY 1,2,3
	ORDER BY 1 DESC 
)
,switch AS (
	SELECT 
		 date_trunc('week',Date)::Date AS fact_date, 
		 customer_type,
		 country_name AS country,
		 COUNT(DISTINCT s.customer_id) AS switch_customers
	FROM ods_production.subscription_plan_switching sw 
	LEFT JOIN master.subscription s 
		ON sw.subscription_id=s.subscription_id
	GROUP BY 1,2,3
)
SELECT 
	COALESCE(r.fact_date,agg.BOM) AS fact_date,
	COALESCE(r.customer_type,agg.customer_type) AS customer_type,
	r.registered_customers,
	r.subscribed_to_newsletter_at_signup,
	r.customers_with_submitted_orders,
	r.customers_with_paid_orders,
	CASE WHEN COALESCE(r.country,agg.country, 'No Info') IN ('Andorra', 'United Kingdom') THEN 'No Info'
		 ELSE COALESCE(r.country,agg.country,'No Info') END AS country,
	registered_customers_bom,
	Passive_customers_bom,
	Active_customers_bom,
	Lapsed_customers_bom,
	Inactive_customers_bom,
	crm_label_na_bom,
	----
	total_customers_eom,
	total_customers_subscribed_to_newsletter_eom,
	registered_customers_eom,
	Passive_customers_eom,
	Active_customers_eom,
	Lapsed_customers_eom,
	Inactive_customers_eom,
	crm_label_na,
	----
	Active_customers_w_multiple_subscriptions_eom,
	---
	upsell_customers,
	---
	---
	activated_customers,
	reactivated_customers,
	Winback_customers,
	gross_churn_customers,
	dc_customers,
	(gross_churn_customers - dc_customers) as customers_churn_without_dc,
	switch_customers
FROM registered r 
FULL OUTER JOIN agg 
	ON r.fact_date=agg.bom 
 	AND agg.customer_type = r.customer_type 
 	AND agg.country = r.country
LEFT JOIN switch 
	ON r.fact_date=switch.fact_Date 
	AND switch.customer_type = r.customer_type 
	AND switch.country = r.country
 ORDER BY r.fact_date DESC
 ;

GRANT SELECT ON dwh.crm_weekly_report TO tableau;
