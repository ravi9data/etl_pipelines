DROP TABLE IF EXISTS dwh.reporting_churn_store_commercial;
CREATE TABLE dwh.reporting_churn_store_commercial AS
WITH dates AS (
	SELECT DISTINCT 
		 datum AS fact_date, 
		 date_trunc('month',datum)::DATE AS month_bom,
		 LEAST(DATE_TRUNC('MONTH',DATEADD('MONTH',1,DATUM))::DATE-1,CURRENT_DATE) AS MONTH_EOM
	FROM public.dim_dates
	WHERE datum <= current_date
	ORDER BY 1 DESC 
)
, s AS (
	SELECT 
		s.subscription_id,
		s.store_label, 
		s.store_commercial, 
		s.customer_id, 
		c2.company_type_name, 
		s.subscription_value_eur AS subscription_value_euro,
		s.end_date, 
		s.fact_day, 
		s.cancellation_date, 
		c.cancellation_reason_churn, 
		cf.last_valid_payment_category, 
		cf.default_date+31 AS default_date,
		s.country_name AS country,
		s.subscription_value_lc AS actual_subscription_value
 	FROM ods_production.subscription_phase_mapping s 
	LEFT JOIN ods_production.subscription_cancellation_reason c 
		ON s.subscription_id=c.subscription_id
	LEFT JOIN ods_production.subscription_cashflow cf 
		ON s.subscription_id=cf.subscription_id
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
) 
,active_subs AS (
	SELECT 
		fact_date,
		CASE 
		 	WHEN s.store_commercial like ('%B2B%') AND f.is_freelancer = 1
 		  		THEN 'B2B-Freelancers'
		 	WHEN  s.store_commercial like ('%B2B%')
		 		THEN 'B2B-Non Freelancers'
		 	WHEN s.store_commercial like ('%Partnerships%') 
		 		THEN 'Partnerships-Total'
		 	WHEN s.store_commercial = 'Grover Germany'
		 		THEN 'Grover-DE'
		 	WHEN s.store_label in ('Grover - UK online','Grover - Netherlands online','Grover - Austria online', 'Grover - Spain online')
		 		THEN 'Grover-EU'
		 	WHEN s.store_label in ('Grover - USA old online')
		 		THEN 'Grover-US old'
		 	WHEN s.store_label in ('Grover - United States online')
		 		THEN 'Grover-US'
		 	ELSE 'Grover-DE'
		 	END AS store_label,
		month_bom,
		month_eom,
		COALESCE(country, 'No Info') AS country,
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
					THEN s.customer_id 
			  		END) AS active_default_customers,
		SUM(s.subscription_value_euro) as active_subscription_value,
		SUM(s.actual_subscription_value) as actual_subscription_value,
		SUM(CASE 
			 	WHEN s.cancellation_reason_churn <> 'failed delivery' 
			    	 AND last_valid_payment_category LIKE ('%DEFAULT%') 
			    	 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
					 AND default_date < fact_date 
					THEN s.subscription_value_euro 
			  		END) AS active_default_subscription_value,
 		 COUNT(DISTINCT CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS active_subscriptions_failed_delivery,
		 COUNT(DISTINCT CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.customer_id END) AS active_customers_failed_delivery,
		 SUM(CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value_euro END) AS active_subscription_value_failed_delivery
	FROM dates d
	LEFT JOIN s 
		ON d.fact_date::date >= s.fact_Day::date 
		AND d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
	LEFT JOIN dm_risk.b2b_freelancer_mapping f 
		ON f.company_type_name = s.company_type_name
	WHERE country IS NOT NULL
		AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3,4,5
	ORDER BY 1 desc
)
, countries_data AS (
	SELECT DISTINCT
		 fact_date,
		 CASE 
		 	WHEN s.store_commercial like ('%B2B%') AND f.is_freelancer = 1
 		  		THEN 'B2B-Freelancers'
		 	WHEN  s.store_commercial like ('%B2B%')
		 		THEN 'B2B-Non Freelancers'
		 	WHEN s.store_commercial like ('%Partnerships%') 
		 		THEN 'Partnerships-Total'
		 	WHEN s.store_commercial = 'Grover Germany'
		 		THEN 'Grover-DE'
		 	WHEN s.store_label in ('Grover - UK online','Grover - Netherlands online','Grover - Austria online', 'Grover - Spain online')
		 		THEN 'Grover-EU'
		 	WHEN s.store_label in ('Grover - USA old online')
		 		THEN 'Grover-US old'
		 	WHEN s.store_label in ('Grover - United States online')
		 		THEN 'Grover-US'
		 	ELSE 'Grover-DE'
		 	END AS store_label,
		 month_bom,
		 month_eom,
		 'No Info' AS country, 
		 NULL::int8 AS active_subscriptions,
		 NULL::int8 AS active_default_subscriptions,
		 NULL::int8 AS active_customers,
		 NULL::int8 AS active_default_customers,
		 NULL::int8 AS active_subscription_value,
		 NULL::int8 AS actual_subscription_value,
		 NULL::int8 AS active_default_subscription_value,
		 NULL::int8 AS active_subscriptions_failed_delivery,
		 NULL::int8 AS active_customers_failed_delivery,
		 NULL::int8 AS active_subscription_value_failed_delivery
	FROM dates d
	LEFT JOIN s
		ON d.fact_date::date >= s.fact_Day::date 
		AND d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
	LEFT JOIN dm_risk.b2b_freelancer_mapping f 
		ON f.company_type_name = s.company_type_name
	WHERE s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3,4,5
)
, active_subs_final AS (
	SELECT * FROM active_subs
	UNION ALL
	SELECT * FROM countries_data
)
,payments as (
	SELECT 
		sp.paid_date::date AS paid_date,
		CASE 
		 	WHEN s.store_commercial like ('%B2B%') AND f.is_freelancer = 1
 		  		THEN 'B2B-Freelancers'
		 	WHEN  s.store_commercial like ('%B2B%')
		 		THEN 'B2B-Non Freelancers'
		 	WHEN s.store_commercial like ('%Partnerships%') 
		 		THEN 'Partnerships-Total'
		 	WHEN s.store_commercial = 'Grover Germany'
		 		THEN 'Grover-DE'
		 	WHEN s.store_label in ('Grover - UK online','Grover - Netherlands online','Grover - Austria online', 'Grover - Spain online')
		 		THEN 'Grover-EU'
		 	WHEN s.store_label in ('Grover - USA old online')
		 		THEN 'Grover-US old'
		 	WHEN s.store_label in ('Grover - United States online')
		 		THEN 'Grover-US'
		 	ELSE 'Grover-DE'
		 	END AS store_label,
		COALESCE(sp.country_name, 'No Info') AS country,
		SUM(sp.amount_paid) AS collected_subscription_revenue
	FROM master.subscription_payment sp 
	LEFT JOIN master.subscription s 
		ON sp.subscription_id=s.subscription_id 
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping f 
		ON f.company_type_name = c2.company_type_name
	WHERE s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3
	ORDER BY 1 desc
)
,acquisition as (
	SELECT 
		s.start_date::date as start_date, 
		CASE 
		 	WHEN s.store_commercial like ('%B2B%') AND f.is_freelancer = 1
 		  		THEN 'B2B-Freelancers'
		 	WHEN  s.store_commercial like ('%B2B%')
		 		THEN 'B2B-Non Freelancers'
		 	WHEN s.store_commercial like ('%Partnerships%') 
		 		THEN 'Partnerships-Total'
		 	WHEN s.store_commercial = 'Grover Germany'
		 		THEN 'Grover-DE'
		 	WHEN s.store_label in ('Grover - UK online','Grover - Netherlands online','Grover - Austria online', 'Grover - Spain online')
		 		THEN 'Grover-EU'
		 	WHEN s.store_label in ('Grover - USA old online')
		 		THEN 'Grover-US old'
		 	WHEN s.store_label in ('Grover - United States online')
		 		THEN 'Grover-US'
		 	ELSE 'Grover-DE'
		 	END AS store_label,
		COALESCE(s.country_name, 'No Info') AS country, 
		COUNT(s.subscription_id) AS new_subscriptions,
		COUNT(DISTINCT s.order_id) AS new_orders,
		SUM(s.subscription_value_euro) AS acquired_subscription_value,
		SUM(s.committed_sub_value) AS acquired_committed_sub_value,
		COUNT(CASE WHEN retention_group ='RECURRING, UPSELL' THEN s.subscription_id END) AS upsell_subscriptions,
		COUNT(case when retention_group ='RECURRING, REACTIVATION' THEN s.subscription_id END) AS reactivation_subscriptions,
		SUM(CASE WHEN new_recurring='NEW' THEN s.subscription_value_euro END) AS new_subscription_value,
		SUM(CASE WHEN retention_group ='RECURRING, UPSELL' THEN s.subscription_value_euro END) AS upsell_subscription_value,
		SUM(CASE WHEN retention_group ='RECURRING, REACTIVATION' THEN s.subscription_value_euro END) AS reactivation_subscription_value,
		COUNT(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND new_recurring='NEW' THEN s.subscription_id END) AS new_subscriptions_failed_delivery,
		COUNT(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND retention_group ='RECURRING, UPSELL' THEN s.subscription_id END) AS upsell_subscriptions_failed_delivery,
		COUNT(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND retention_group ='RECURRING, REACTIVATION' THEN s.subscription_id END) AS reactivation_subscriptions_failed_delivery,
		SUM(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND new_recurring='NEW' THEN s.subscription_value_euro END) AS new_subscription_value_failed_delivery,
		SUM(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND retention_group ='RECURRING, UPSELL' THEN s.subscription_value_euro END) AS upsell_subscription_value_failed_delivery,
		SUM(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND retention_group ='RECURRING, REACTIVATION' THEN s.subscription_value_euro END) AS reactivation_subscription_value_failed_delivery
	FROM ods_production.subscription s 
	LEFT JOIN ods_production.order_retention_group o 
		ON o.order_id=s.order_id
	LEFT JOIN ods_production.subscription_cancellation_reason c 
		ON s.subscription_id=c.subscription_id
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping f 
		ON f.company_type_name = c2.company_type_name
	WHERE s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3
)
,churn as (
	SELECT 
		s.cancellation_date::date as cancellation_date,
		CASE 
		 	WHEN s.store_commercial like ('%B2B%') AND f.is_freelancer = 1
 		  		THEN 'B2B-Freelancers'
		 	WHEN  s.store_commercial like ('%B2B%')
		 		THEN 'B2B-Non Freelancers'
		 	WHEN s.store_commercial like ('%Partnerships%') 
		 		THEN 'Partnerships-Total'
		 	WHEN s.store_commercial = 'Grover Germany'
		 		THEN 'Grover-DE'
		 	WHEN s.store_label in ('Grover - UK online','Grover - Netherlands online','Grover - Austria online', 'Grover - Spain online')
		 		THEN 'Grover-EU'
		 	WHEN s.store_label in ('Grover - USA old online')
		 		THEN 'Grover-US old'
		 	WHEN s.store_label in ('Grover - United States online')
		 		THEN 'Grover-US'
		 	ELSE 'Grover-DE'
		 	END AS store_label,
		COALESCE(s.country_name, 'No Info') AS country,
		COUNT(s.subscription_id) AS cancelled_subscriptions,
		SUM(s.subscription_value_euro) AS cancelled_subscription_value,
		COUNT(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS cancelled_subscriptions_failed_delivery,
		SUM(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value_euro END) AS cancelled_subscription_value_failed_delivery,
		COUNT(CASE WHEN c.cancellation_reason_churn = 'customer request' THEN s.subscription_id END) AS cancelled_subscriptions_customer_request,
		SUM(CASE WHEN c.cancellation_reason_churn = 'customer request' THEN s.subscription_value_euro END) AS cancelled_subscription_value_customer_request
	FROM ods_production.subscription s
	LEFT JOIN ods_production.subscription_cancellation_reason c
		ON s.subscription_id=c.subscription_id
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping f 
		ON f.company_type_name = c2.company_type_name
	WHERE s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3
)
,historical AS (
	SELECT 
		date AS reporting_date, 
		CASE 
		 	WHEN s.store_commercial like ('%B2B%') AND f.is_freelancer = 1
 		  		THEN 'B2B-Freelancers'
		 	WHEN  s.store_commercial like ('%B2B%')
		 		THEN 'B2B-Non Freelancers'
		 	WHEN s.store_commercial like ('%Partnerships%') 
		 		THEN 'Partnerships-Total'
		 	WHEN s.store_commercial = 'Grover Germany'
		 		THEN 'Grover-DE'
		 	WHEN s.store_label in ('Grover - UK online','Grover - Netherlands online','Grover - Austria online', 'Grover - Spain online')
		 		THEN 'Grover-EU'
		 	WHEN s.store_label in ('Grover - USA old online')
		 		THEN 'Grover-US old'
		 	WHEN s.store_label in ('Grover - United States online')
		 		THEN 'Grover-US'
		 	ELSE 'Grover-DE'
		 	END AS store_label,
		COALESCE(s.country_name, 'No Info') AS country,
		SUM(CASE WHEN s.status='ACTIVE' THEN (committed_sub_value + COALESCE(s.additional_committed_sub_value,0)) END) AS committed_sub_value,
		SUM(CASE WHEN s.status='ACTIVE' THEN commited_sub_revenue_future END) AS commited_sub_revenue_future
	FROM master.subscription_historical s 
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping f 
		ON f.company_type_name = c2.company_type_name
	WHERE s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3
	order by 1 desc
)
, last_phase_subscriptions AS (
	SELECT 
		DISTINCT subscription_id,
				 fact_day::date AS fact_day,
				 CASE 
		 			WHEN s.store_commercial LIKE ('%B2B%') AND f.is_freelancer = 1
 		  				THEN 'B2B-Freelancers'
		 			WHEN  s.store_commercial LIKE ('%B2B%')
				 		THEN 'B2B-Non Freelancers'
				 	WHEN s.store_commercial LIKE ('%Partnerships%') 
				 		THEN 'Partnerships-Total'
				 	WHEN s.store_commercial = 'Grover Germany'
				 		THEN 'Grover-DE'
				 	WHEN s.store_label IN ('Grover - UK online','Grover - Netherlands online','Grover - Austria online', 'Grover - Spain online')
				 		THEN 'Grover-EU'
				 	WHEN s.store_label IN ('Grover - USA old online')
				 		THEN 'Grover-US old'
				 	WHEN s.store_label IN ('Grover - United States online')
				 		THEN 'Grover-US'
				 	ELSE 'Grover-DE'
		 		END AS store_label,
				country_name AS country,
				subscription_value_eur
	FROM ods_production.subscription_phase_mapping s
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping f 
		ON f.company_type_name = c2.company_type_name
	WHERE latest_phase_idx = 1
		AND s.status = 'ACTIVE'
		AND s.free_year_offer_taken IS TRUE 
)
, last_phase_before_change_of_price AS (
	SELECT 
		s.subscription_id,
		s.subscription_value_eur AS subscription_value_eur_before_free_year_offer
	FROM ods_production.subscription_phase_mapping s
	WHERE s.status = 'ACTIVE'
		AND latest_phase_idx = 2 --last phase before freeyear acceptance
		AND EXISTS (SELECT NULL FROM last_phase_subscriptions lp WHERE lp.subscription_id = s.subscription_id)
	GROUP BY 1,2
)
, subscription_price_changes AS (
	SELECT 
		s.fact_day AS last_change_date,
		s.store_label,
		s.country,
		sum(subscription_value_eur) AS subscription_value_eur_free_offer,
		sum(lp.subscription_value_eur_before_free_year_offer) AS subscription_value_eur_before_free_year_offer,
		sum(subscription_value_eur) - sum(lp.subscription_value_eur_before_free_year_offer) AS decrease_from_free_year_offer
	FROM last_phase_subscriptions s
	LEFT JOIN last_phase_before_change_of_price lp
		ON lp.subscription_id = s.subscription_id
	GROUP BY 1,2,3	
)
, commercial_targets_daily_store_commercial_2022_2023_table AS (
	SELECT  
		to_date AS datum, 
		CASE WHEN store_label = 'NL' THEN 'Netherlands'
			 WHEN store_label = 'US' THEN 'United States'
			 WHEN store_label = 'AT' THEN 'Austria'
			 WHEN store_label = 'ES' THEN 'Spain'
			 WHEN store_label = 'DE' THEN 'Germany'
			 WHEN store_label = 'Total' THEN 'No Info'
			 END AS country,
		CASE WHEN b2b_split = 'Freelancers' THEN 'B2B-Freelancers'
			 WHEN b2b_split = 'Non-Freelancers' THEN 'B2B-Non Freelancers'
			 WHEN channel_type = 'Retail' THEN 'Partnerships-Total'
			 WHEN store_label IN ('AT','ES','NL') THEN 'Grover-EU'
			 ELSE 'Grover-'+ store_label
			 END AS store_country,
		CASE WHEN measures = 'ASV' THEN amount END AS active_sub_value_daily_target ,
		CASE WHEN measures = 'Acquired ASV' THEN amount END AS acquired_subvalue_daily,
		CASE WHEN measures = 'Cancelled ASV' THEN amount END AS cancelled_subvalue_daily
	FROM dm_commercial.r_commercial_daily_targets_since_2022							-- it includes end of 2022 and whole 2023
	WHERE date_trunc('month',datum) >= '2022-06-01'
)
, commercial_targets_daily_store_commercial_2022_2023 AS (
	SELECT  
		datum, 
		country,
		store_country,
		d.month_bom,
		d.month_eom,
		SUM(active_sub_value_daily_target) AS active_sub_value_daily_target ,
		SUM(acquired_subvalue_daily) AS acquired_subvalue_daily,
		SUM(cancelled_subvalue_daily) AS cancelled_subvalue_daily
	FROM commercial_targets_daily_store_commercial_2022_2023_table t
	LEFT JOIN dates d
		ON t.datum = d.fact_date
	GROUP BY 1,2,3,4,5
)
, commercial_targets_daily_store_commercial_2022_1st_part AS (
	SELECT 
		datum,
		CASE WHEN country = 'NL' THEN 'Netherlands'
			 WHEN country = 'US' THEN 'United States'
			 WHEN country = 'AT' THEN 'Austria'
			 WHEN country = 'ES' THEN 'Spain'
			 WHEN country = 'DE' THEN 'Germany'
			 WHEN country = 'Total' THEN 'No Info'
			 END AS country,
		CASE WHEN store = 'B2C' AND country IN ('AT','ES','NL') THEN 'Grover-EU'
			 WHEN store = 'B2C' THEN 'Grover-'+country
			 WHEN store = 'Retail' THEN 'Partnerships-Total'
			 WHEN store = 'B2B-Freelancers' THEN 'B2B-Freelancers'
			 WHEN store = 'B2B-Non-Freelancers' THEN 'B2B-Non Freelancers'
			 END AS store_country,	 
		d.month_bom,
		d.month_eom,
		SUM(asv) as active_sub_value_daily_target,
 		SUM(acquired_asv) as acquired_subvalue_daily,
 		SUM(cancelled_asv) aS cancelled_subvalue_daily
	FROM dw_targets.gs_commercial_targets_2022 t
	LEFT JOIN dates d 
		ON t.datum = d.fact_date
	WHERE date_trunc('month',datum) BETWEEN '2022-01-01' AND '2022-05-31'
	GROUP BY 1,2,3,4,5
)
, commercial_targets_daily_store_commercial AS (
	SELECT * FROM commercial_targets_daily_store_commercial_2022_2023 
	UNION ALL
	SELECT * FROM commercial_targets_daily_store_commercial_2022_1st_part 
)
, fx AS (
	SELECT 
		dateadd('day',1,date_) as fact_month,
		date_,
		currency,
		exchange_rate_eur
	FROM trans_dev.daily_exchange_rate  er
	LEFT JOIN public.dim_dates dd on er.date_ = dd.datum 
	WHERE day_is_last_of_month
)
SELECT DISTINCT 
	COALESCE(l.fact_date,t.datum) AS fact_date,
	COALESCE(l.store_label,t.store_country) AS store_label,
	COALESCE(l.month_bom,t.month_bom) AS month_bom,
	COALESCE(l.month_eom,t.month_eom) AS month_eom,
	COALESCE(l.country,t.country) AS country,
	 --active customers
	COALESCE(active_customers,0) AS active_customers,
	COALESCE(active_default_customers,0) AS active_default_customers,
	COALESCE(active_customers_failed_delivery,0) AS active_customers_failed_delivery,
 	--active subs
 	COALESCE(active_subscriptions,0) AS active_subscriptions,
	COALESCE(active_default_subscriptions,0) AS active_default_subscriptions,
	COALESCE(active_subscriptions_failed_delivery,0) AS active_subscriptions_failed_delivery,
	COALESCE(active_subscription_value,0) AS active_subscription_value,
	COALESCE(actual_subscription_value,0) AS actual_subscription_value,
	COALESCE(active_default_subscription_value,0) AS active_default_subscription_value,
	COALESCE(active_subscription_value_failed_delivery,0) AS active_subscription_value_failed_delivery,
	 --new subs
	COALESCE(acquired_subscription_value,0) AS acquired_subscription_value,
	COALESCE(acquired_committed_sub_value,0) AS acquired_committed_sub_value,
	--
	COALESCE(new_orders,0) AS new_orders,
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
	COALESCE(t.active_sub_value_daily_target,0) AS active_sub_value_daily_target,
	COALESCE((CASE WHEN t.country = 'United States' THEN t.active_sub_value_daily_target/fx.exchange_rate_eur 
				ELSE t.active_sub_value_daily_target END),0) AS active_sub_value_daily_target_in_currency,
	--	t.incrementalsubsvalue_daily,
	COALESCE(t.cancelled_subvalue_daily,0) AS cancelled_subvalue_daily,
	COALESCE(t.acquired_subvalue_daily,0) AS cquired_subvalue_daily,
	d.day_is_first_of_month,
	d.day_is_last_of_month,
	COALESCE(h.committed_sub_value,0) AS committed_sub_value,
	COALESCE(h.commited_sub_revenue_future,0) AS commited_sub_revenue_future,
	COALESCE(p.collected_subscription_revenue,0) AS collected_subscription_revenue,
	COALESCE(pc.subscription_value_eur_free_offer,0) AS subscription_value_eur_free_offer,
	COALESCE(pc.subscription_value_eur_before_free_year_offer,0) AS subscription_value_eur_before_free_year_offer,
	COALESCE(pc.decrease_from_free_year_offer,0) AS decrease_from_free_year_offer
FROM active_subs_final l
LEFT JOIN acquisition a 
	ON l.fact_date::date=a.start_date::date
	AND l.store_label=a.store_label
	AND l.country = a.country
LEFT JOIN churn c 
	ON l.fact_date::date=c.cancellation_date::date 
	AND l.store_label=c.store_label
	AND l.country = c.country  
LEFT JOIN subscription_price_changes pc
	ON l.fact_date::date = pc.last_change_date::date 
	AND l.store_label = pc.store_label
	AND l.country = pc.country  
FULL JOIN commercial_targets_daily_store_commercial t 
	ON t.datum::date=l.fact_date::date 
	AND l.store_label=t.store_country
	AND l.country = t.country
LEFT JOIN fx 
	ON 
		(CASE WHEN DATE_TRUNC('month',t.datum) > DATE_TRUNC('month',current_date) THEN DATE_TRUNC('month',current_date)
				ELSE DATE_TRUNC('month',t.datum) END) = fx.fact_month
		AND t.country = 'United States'
LEFT JOIN public.dim_dates d 
	ON d.datum::date=l.fact_date::date
LEFT JOIN historical h 
	ON h.reporting_date::date=l.fact_date::date
	AND l.store_label=h.store_label
	AND l.country = h.country
LEFT JOIN payments p 
	ON p.paid_date::date=l.fact_Date::date
	AND p.store_label=l.store_label
	AND l.country = p.country
;

GRANT SELECT ON dwh.reporting_churn_store_commercial TO tableau;
