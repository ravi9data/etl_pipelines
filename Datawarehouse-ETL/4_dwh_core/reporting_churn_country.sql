DROP TABLE IF EXISTS dwh.reporting_churn_country;
CREATE TABLE dwh.reporting_churn_country AS
WITH dates AS (
	SELECT DISTINCT  
		datum as fact_date, 
	 	date_trunc('month',datum)::DATE AS month_bom,
		LEAST(DATE_TRUNC('MONTH',DATEADD('MONTH',1,DATUM))::DATE-1,CURRENT_DATE) AS MONTH_EOM
	FROM public.dim_dates
	WHERE datum <= current_date
	ORDER BY 1 DESC 
),active_subs AS (
	SELECT 
		fact_date,
		s.country_name,
		month_bom,
		month_eom,
		COUNT(DISTINCT s.subscription_id) AS active_subscriptions,
	 	COUNT(DISTINCT 
	  		CASE 
	  			WHEN s.cancellation_reason_churn <> 'failed delivery' 
	    		 AND last_valid_payment_category LIKE ('%DEFAULT%') 
	   			 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
	   			 AND default_date < fact_date 
	  				THEN s.subscription_id 
	  		END) AS active_default_subscriptions,
	 	COUNT(DISTINCT s.customer_id) AS active_customers,
	 	COUNT(DISTINCT 
	  		CASE 
	    		WHEN s.cancellation_reason_churn <> 'failed delivery' 
	   			 AND last_valid_payment_category LIKE ('%DEFAULT%') 
	   			 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
	   			 AND default_date < fact_date 
	  				THEN customer_id 
	  		END) AS active_default_customers,
		SUM(s.subscription_value_eur) AS active_subscription_value,
		SUM(CASE 
				WHEN s.cancellation_reason_churn <> 'failed delivery' 
				 AND last_valid_payment_category LIKE ('%DEFAULT%') 
				 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
				 AND default_date < fact_date 
		  			THEN s.subscription_value_eur 
		  		END) AS active_default_subscription_value,
		COUNT(DISTINCT 
			CASE 
				WHEN s.cancellation_reason_churn = 'failed delivery' 
					THEN s.subscription_id END) AS active_subscriptions_failed_delivery,
		COUNT(DISTINCT 
			CASE 
				WHEN s.cancellation_reason_churn = 'failed delivery' 
					THEN s.customer_id END) AS active_customers_failed_delivery,
		SUM(CASE 
				WHEN s.cancellation_reason_churn = 'failed delivery' 
					THEN s.subscription_value_eur END) AS active_subscription_value_failed_delivery
	FROM dates d
	LEFT JOIN ods_production.subscription_phase_mapping s
		ON d.fact_date::DATE >= s.fact_day::DATE 
		AND  d.fact_date::DATE <= coalesce(s.end_date::DATE, d.fact_date::DATE+1)
	LEFT JOIN ods_production.subscription_cancellation_reason c 
		 	ON s.subscription_id=c.subscription_id
		 LEFT JOIN ods_production.subscription_cashflow cf 
		 	ON s.subscription_id=cf.subscription_id
 	WHERE s.store_label NOT ilike '%old%'
	GROUP BY 1,2,3,4
	ORDER BY 1 DESC
	)
,acquisition AS (
	SELECT 
		s.start_date::DATE AS start_date, 
		s.country_name,
		COUNT( s.subscription_id) AS new_subscriptions,
		COUNT(CASE 
				WHEN retention_group ='RECURRING, UPSELL' 
				THEN s.subscription_id END) AS upsell_subscriptions,
		COUNT(CASE 
				WHEN retention_group ='RECURRING, REACTIVATION'
				THEN s.subscription_id END) AS reactivation_subscriptions,
		SUM(CASE 
				WHEN new_recurring='NEW' 
				THEN s.subscription_value_euro END) AS new_subscription_value,
		SUM(CASE 
				WHEN retention_group ='RECURRING, UPSELL' 
				THEN s.subscription_value_euro END) AS upsell_subscription_value,
		SUM(CASE 
				WHEN retention_group ='RECURRING, REACTIVATION' 
				THEN s.subscription_value_euro END) as reactivation_subscription_value,
		COUNT(CASE 
				WHEN c.cancellation_reason_churn = 'failed delivery' 
				 AND new_recurring='NEW' 
				THEN s.subscription_id END) AS new_subscriptions_failed_delivery,
		COUNT(CASE 
				WHEN c.cancellation_reason_churn = 'failed delivery' 
				 AND retention_group ='RECURRING, UPSELL' 
				THEN s.subscription_id END) AS upsell_subscriptions_failed_delivery,
		COUNT(CASE
				WHEN c.cancellation_reason_churn = 'failed delivery' 
				 AND retention_group ='RECURRING, REACTIVATION' 
				THEN s.subscription_id END) AS reactivation_subscriptions_failed_delivery,
		SUM(CASE 
				WHEN c.cancellation_reason_churn = 'failed delivery' 
				 AND new_recurring='NEW' 
				THEN s.subscription_value_euro END) AS new_subscription_value_failed_delivery,
		SUM(CASE 
				WHEN c.cancellation_reason_churn = 'failed delivery' 
				 AND retention_group ='RECURRING, UPSELL' 
				THEN s.subscription_value_euro  END) AS upsell_subscription_value_failed_delivery,
		SUM(CASE 
				WHEN c.cancellation_reason_churn = 'failed delivery' 
				 AND retention_group ='RECURRING, REACTIVATION' 
				THEN s.subscription_value_euro END) AS reactivation_subscription_value_failed_delivery
	FROM ods_production.subscription s 
	LEFT JOIN ods_production.order_retention_group o 
		ON o.order_id=s.order_id
	LEFT JOIN ods_production.subscription_cancellation_reason c 
		ON s.subscription_id=c.subscription_id
	WHERE s.store_label NOT ilike '%old%' -------------------NEW
	GROUP BY 1,2
)
,churn AS (
	SELECT 
		s.cancellation_date::DATE AS cancellation_date,
		s.country_name,
		COUNT(s.subscription_id) AS cancelled_subscriptions,
		SUM(s.subscription_value_euro) AS cancelled_subscription_value,
		COUNT(CASE 
			WHEN c.cancellation_reason_churn = 'failed delivery' 
			THEN s.subscription_id END) AS cancelled_subscriptions_failed_delivery,
		SUM(CASE 
			WHEN c.cancellation_reason_churn = 'failed delivery' 
			THEN s.subscription_value_euro END) AS cancelled_subscription_value_failed_delivery,
		COUNT(CASE 
			WHEN c.cancellation_reason_churn = 'customer request' 
			THEN s.subscription_id END) AS cancelled_subscriptions_customer_request,
		SUM(CASE 
			WHEN c.cancellation_reason_churn = 'customer request' 
			THEN s.subscription_value_euro END) AS cancelled_subscription_value_customer_request
	FROM ods_production.subscription s
	LEFT JOIN ods_production.subscription_cancellation_reason c 
		ON s.subscription_id=c.subscription_id
	WHERE s.store_label NOT ilike '%old%' -------------------NEW
	GROUP BY 1,2
)
,targets AS (
	SELECT 
		*,
		CASE 
			WHEN country = 'DE' THEN 'Germany' 
			WHEN country ='EU' THEN 'International'
		 	WHEN country = 'US' THEN 'United States'
			 END AS country_name
	FROM dwh.commercial_targets_daily_country
)
SELECT DISTINCT 
	l.fact_date,
	l.country_name,
	l.month_bom,
	l.month_eom,
	--active customers
	COALESCE(active_customers,0) AS active_customers,
	COALESCE(active_default_customers,0) AS active_default_customers,
	COALESCE(active_customers_failed_delivery,0) AS active_customers_failed_delivery,
	--active subs
	COALESCE(active_subscriptions,0) AS active_subscriptions,
	COALESCE(active_default_subscriptions,0) AS active_default_subscriptions,
	COALESCE(active_subscriptions_failed_delivery,0) AS active_subscriptions_failed_delivery,
	COALESCE(active_subscription_value,0) AS active_subscription_value,
	COALESCE(active_default_subscription_value,0) AS active_default_subscription_value,
	COALESCE(active_subscription_value_failed_delivery,0) AS active_subscription_value_failed_delivery,
	--new subs
	COALESCE(new_subscriptions,0) AS new_subscriptions,
	COALESCE(new_subscriptions_failed_delivery,0) AS new_subscriptions_failed_delivery,
	COALESCE(new_subscription_value,0) AS new_subscription_value,
	COALESCE(new_subscription_value_failed_delivery,0) AS new_subscription_value_failed_delivery,
	--upsell
	COALESCE(upsell_subscriptions,0) AS upsell_subscriptions,
	COALESCE(upsell_subscriptions_failed_delivery,0) AS upsell_subscriptions_failed_delivery,
	COALESCE(upsell_subscription_value,0) AS upsell_subscription_value,
	COALESCE(upsell_subscription_value_failed_delivery,0) AS upsell_subscription_value_failed_delivery,
	-- reactivation
	COALESCE(reactivation_subscriptions,0) AS reactivation_subscriptions,
	COALESCE(reactivation_subscriptions_failed_delivery,0) AS reactivation_subscriptions_failed_delivery,
	COALESCE(reactivation_subscription_value,0) AS reactivation_subscription_value,
	COALESCE(reactivation_subscription_value_failed_delivery,0) AS reactivation_subscription_value_failed_delivery,
	-- cancelled subscriptions
	COALESCE(cancelled_subscriptions,0) AS cancelled_subscriptions,
	COALESCE(cancelled_subscriptions_failed_delivery,0) AS cancelled_subscriptions_failed_delivery,
	COALESCE(cancelled_subscriptions_customer_request,0) AS cancelled_subscriptions_customer_request,
	COALESCE(cancelled_subscription_value,0) AS cancelled_subscription_value,
	COALESCE(cancelled_subscription_value_failed_delivery,0) AS cancelled_subscription_value_failed_delivery,
	COALESCE(cancelled_subscription_value_customer_request,0) AS cancelled_subscription_value_customer_request,
	--sub switching
	t.active_sub_value_daily_target
FROM active_subs l
LEFT JOIN acquisition a 
	ON l.fact_date::DATE=a.start_date::DATE 
	AND l.country_name=a.country_name
LEFT JOIN churn c 
	ON l.fact_date::DATE=c.cancellation_date::DATE 
	AND l.country_name=c.country_name
LEFT JOIN targets t 
 	ON t.datum::DATE=l.fact_date::DATE 
 	AND  l.country_name=t.country_name
;

GRANT SELECT ON dwh.reporting_churn_country TO tableau;
