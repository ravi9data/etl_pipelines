DROP TABLE IF EXISTS dwh.reporting_churn_first_allocation_store;
CREATE TABLE dwh.reporting_churn_first_allocation_store AS
WITH dates AS (
	SELECT DISTINCT 
		 datum AS fact_date, 
		 date_trunc('month',datum)::DATE AS month_bom,
		 LEAST(DATE_TRUNC('MONTH',DATEADD('MONTH',1,DATUM))::DATE-1,CURRENT_DATE) AS MONTH_EOM
	FROM public.dim_dates
	WHERE datum <= current_date
	ORDER BY 1 DESC 
)
,active_subs AS (
	SELECT 
		 fact_date,
		 CASE 
		 	WHEN s.store_commercial LIKE ('%B2B%') 
		  		THEN 'B2B-Total'
		 	WHEN csd.first_subscription_acquisition_channel LIKE ('%Partners%') 
		 		THEN 'Partnerships-Total'
		 	WHEN csd.first_subscription_store LIKE 'Grover - Germany%'
		 		THEN 'Grover-DE'
		 	WHEN csd.first_subscription_store IN ('Grover - UK online')
		 		THEN 'Grover-UK'
		  	WHEN csd.first_subscription_store IN ('Grover - Netherlands online')
		  		THEN 'Grover-NL'
		  	WHEN csd.first_subscription_store IN ('Grover - Austria online')
		 		THEN 'Grover-AT'
		  	WHEN csd.first_subscription_store IN ('Grover - Spain online')
		  		THEN 'Grover-ES'
		 	WHEN csd.first_subscription_store IN ('Grover - United States online')
		  		THEN 'Grover-US'
		  	WHEN csd.first_subscription_store IN ('Grover - USA old online')
		  		THEN 'Grover-US old'
		 	ELSE 'Grover-DE'
		 	END AS first_subscription_store,
		 month_bom,
		 month_eom,
		 s.country_name AS country, 
		 COUNT(DISTINCT s.subscription_id) AS active_subscriptions,
		 COUNT(DISTINCT 
		 	CASE 
			  	WHEN s.cancellation_reason_churn <> 'failed delivery' 
			    	 AND last_valid_payment_category LIKE ('%DEFAULT%') 
			    	 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
			    	 AND (default_date+31) < fact_date 
					THEN s.subscription_id 
			  		END) AS active_default_subscriptions,
		 COUNT(DISTINCT s.customer_id) AS active_customers,
		 COUNT(DISTINCT 
		 	CASE 
				WHEN s.cancellation_reason_churn <> 'failed delivery' 
			    	 AND last_valid_payment_category LIKE ('%DEFAULT%') 
			    	 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
			    	 AND (default_date+31) < fact_date 
					THEN s.customer_id 
			  		END) AS active_default_customers,
		 SUM(s.subscription_value_eur) AS active_subscription_value,
		 SUM(s.subscription_value_lc) AS actual_subscription_value,
		 SUM(CASE 
			 	WHEN s.cancellation_reason_churn <> 'failed delivery' 
			    	 AND last_valid_payment_category LIKE ('%DEFAULT%') 
			    	 AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
			    	 AND (default_date+31) < fact_date 
					THEN s.subscription_value_eur 
			  		END) AS active_default_subscription_value,
		 COUNT(DISTINCT CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS active_subscriptions_failed_delivery,
		 COUNT(DISTINCT CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.customer_id END) AS active_customers_failed_delivery,
		 SUM(CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value_eur END) AS active_subscription_value_failed_delivery
	FROM dates d
	LEFT JOIN ods_production.subscription_phase_mapping s
		ON d.fact_date::date >= s.fact_day::date 
		AND d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
	LEFT JOIN ods_production.customer_subscription_details csd 
		ON csd.customer_id = s.customer_id 
	LEFT JOIN ods_production.subscription_cashflow cf 
		ON s.subscription_id=cf.subscription_id
	WHERE csd.first_subscription_store NOT IN ('Grover - USA old online','Grover - UK online')
	GROUP BY 1,2,3,4,5
)
, countries_data AS (
	SELECT DISTINCT
		 fact_date,
		 CASE 
		 	WHEN s.store_commercial LIKE ('%B2B%') 
		  		THEN 'B2B-Total'
		 	WHEN csd.first_subscription_acquisition_channel LIKE ('%Partners%') 
		 		THEN 'Partnerships-Total'
		 	WHEN csd.first_subscription_store LIKE 'Grover - Germany%'
		 		THEN 'Grover-DE'
		 	WHEN csd.first_subscription_store IN ('Grover - UK online')
		 		THEN 'Grover-UK'
		  	WHEN csd.first_subscription_store IN ('Grover - Netherlands online')
		  		THEN 'Grover-NL'
		  	WHEN csd.first_subscription_store IN ('Grover - Austria online')
		 		THEN 'Grover-AT'
		  	WHEN csd.first_subscription_store IN ('Grover - Spain online')
		  		THEN 'Grover-ES'
		 	WHEN csd.first_subscription_store IN ('Grover - United States online')
		  		THEN 'Grover-US'
		  	WHEN csd.first_subscription_store IN ('Grover - USA old online')
		  		THEN 'Grover-US old'
		 	ELSE 'Grover-DE'
		 	END AS first_subscription_store,
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
	LEFT JOIN ods_production.subscription_phase_mapping s
		ON d.fact_date::date >= s.fact_day::date 
		AND d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
	LEFT JOIN ods_production.customer_subscription_details csd 
		ON csd.customer_id = s.customer_id 
	WHERE csd.first_subscription_store NOT IN ('Grover - USA old online','Grover - UK online')
	GROUP BY 1,2,3,4,5
)
, active_subs_final AS (
	SELECT * FROM active_subs
	UNION ALL
	SELECT * FROM countries_data
)
,payments AS (
	SELECT 
		sp.paid_date::date AS paid_date,
		CASE 
			WHEN s.store_commercial LIKE ('%B2B%') 
			  THEN 'B2B-Total'
			WHEN csd.first_subscription_acquisition_channel LIKE ('%Partners%') 
			  THEN 'Partnerships-Total'
			WHEN csd.first_subscription_store LIKE 'Grover - Germany%'
			  THEN 'Grover-DE'
			WHEN csd.first_subscription_store IN ('Grover - UK online')
			  THEN 'Grover-UK'
			WHEN csd.first_subscription_store IN ('Grover - Netherlands online')
			  THEN 'Grover-NL'
			WHEN csd.first_subscription_store IN ('Grover - Austria online')
			  THEN 'Grover-AT'
			WHEN csd.first_subscription_store IN ('Grover - Spain online')
			  THEN 'Grover-ES'
			WHEN csd.first_subscription_store IN ('Grover - United States online')
			  THEN 'Grover-US'
			WHEN csd.first_subscription_store IN ('Grover - USA old online')
			  THEN 'Grover-US old'
			ELSE 'Grover-DE'
			END AS first_subscription_store,
		sp.country_name AS country,
		SUM(sp.amount_paid) AS collected_subscription_revenue
	FROM master.subscription_payment sp 
	LEFT JOIN master.subscription s 
		ON sp.subscription_id=s.subscription_id 
	LEFT JOIN ods_production.customer_subscription_details csd 
		ON csd.customer_id = s.customer_id
	WHERE csd.first_subscription_store NOT IN ('Grover - USA old online','Grover - UK online')
	GROUP BY 1,2,3
)
,acquisition AS (
	SELECT 
		s.start_date::date AS start_date, 
		CASE 
			WHEN s.store_commercial LIKE ('%B2B%') 
				THEN 'B2B-Total'
			WHEN csd.first_subscription_acquisition_channel LIKE ('%Partners%') 
				THEN 'Partnerships-Total'
			WHEN csd.first_subscription_store LIKE 'Grover - Germany%'
				THEN 'Grover-DE'
			WHEN csd.first_subscription_store IN ('Grover - UK online')
				THEN 'Grover-UK'
			WHEN csd.first_subscription_store IN ('Grover - Netherlands online')
				THEN 'Grover-NL'
			WHEN csd.first_subscription_store IN ('Grover - Austria online')
				THEN 'Grover-AT'
			WHEN csd.first_subscription_store IN ('Grover - Spain online')
				THEN 'Grover-ES'
			WHEN csd.first_subscription_store IN ('Grover - United States online')
				THEN 'Grover-US'
			WHEN csd.first_subscription_store IN ('Grover - USA old online')
				THEN 'Grover-US old'
		 	ELSE 'Grover-DE'
			END AS first_subscription_store,
		s.country_name AS country, 
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
	LEFT JOIN ods_production.customer_subscription_details csd 
		ON csd.customer_id = s.customer_id
	WHERE csd.first_subscription_store NOT IN ('Grover - USA old online','Grover - UK online')
	GROUP BY 1,2,3
)
,churn AS (
	select 
		s.cancellation_date::date as cancellation_date,
		CASE 
			WHEN s.store_commercial LIKE ('%B2B%') 
				THEN 'B2B-Total'
			WHEN csd.first_subscription_acquisition_channel LIKE ('%Partners%') 
				THEN 'Partnerships-Total'
			WHEN csd.first_subscription_store LIKE 'Grover - Germany%'
				THEN 'Grover-DE'
			WHEN csd.first_subscription_store IN ('Grover - UK online')
				THEN 'Grover-UK'
			WHEN csd.first_subscription_store IN ('Grover - Netherlands online')
				THEN 'Grover-NL'
			WHEN csd.first_subscription_store IN ('Grover - Austria online')
				THEN 'Grover-AT'
			WHEN csd.first_subscription_store IN ('Grover - Spain online')
				THEN 'Grover-ES'
			WHEN csd.first_subscription_store IN ('Grover - United States online')
				THEN 'Grover-US'
			WHEN csd.first_subscription_store IN ('Grover - USA old online')
				THEN 'Grover-US old'
		 	ELSE 'Grover-DE'
			END AS first_subscription_store,
		s.country_name AS country,
		COUNT(s.subscription_id) AS cancelled_subscriptions,
		SUM(s.subscription_value_euro) AS cancelled_subscription_value,
		COUNT(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS cancelled_subscriptions_failed_delivery,
		SUM(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value_euro END) AS cancelled_subscription_value_failed_delivery,
		COUNT(CASE WHEN c.cancellation_reason_churn = 'customer request' THEN s.subscription_id END) AS cancelled_subscriptions_customer_request,
		SUM(CASE WHEN c.cancellation_reason_churn = 'customer request' THEN s.subscription_value_euro END) AS cancelled_subscription_value_customer_request
	FROM ods_production.subscription s
	LEFT JOIN ods_production.subscription_cancellation_reason c
		ON s.subscription_id=c.subscription_id
	LEFT JOIN ods_production.customer_subscription_details csd 
		ON csd.customer_id = s.customer_id
	WHERE csd.first_subscription_store NOT IN ('Grover - USA old online','Grover - UK online')
	GROUP BY 1,2,3
)
,hisotrical AS (
	SELECT 
		date AS reporting_date, 
		CASE 
			WHEN s.store_commercial LIKE ('%B2B%') 
				THEN 'B2B-Total'
			WHEN csd.first_subscription_acquisition_channel LIKE ('%Partners%') 
				THEN 'Partnerships-Total'
			WHEN csd.first_subscription_store LIKE 'Grover - Germany%'
				THEN 'Grover-DE'
			WHEN csd.first_subscription_store IN ('Grover - UK online')
				THEN 'Grover-UK'
			WHEN csd.first_subscription_store IN ('Grover - Netherlands online')
				THEN 'Grover-NL'
			WHEN csd.first_subscription_store IN ('Grover - Austria online')
				THEN 'Grover-AT'
			WHEN csd.first_subscription_store IN ('Grover - Spain online')
				THEN 'Grover-ES'
			WHEN csd.first_subscription_store IN ('Grover - United States online')
				THEN 'Grover-US'
			WHEN csd.first_subscription_store IN ('Grover - USA old online')
				THEN 'Grover-US old'
		 	ELSE 'Grover-DE'
			END AS first_subscription_store,
		COALESCE(s.country_name, 'No Info') AS country,
		SUM(CASE WHEN status='ACTIVE' THEN (committed_sub_value + COALESCE(s.additional_committed_sub_value,0)) END) AS committed_sub_value,
		SUM(CASE WHEN status='ACTIVE' THEN commited_sub_revenue_future END) AS commited_sub_revenue_future
	FROM master.subscription_historical s 
	LEFT JOIN ods_production.customer_subscription_details csd 
		ON csd.customer_id = s.customer_id
	WHERE csd.first_subscription_store NOT IN ('Grover - USA old online','Grover - UK online')
	GROUP BY 1,2,3
)
, commercial_targets_daily_store_commercial_from_2022_2nd_part_table AS (
	SELECT  
		to_date AS datum, 
		CASE WHEN store_label = 'NL' THEN 'Netherlands'
			 WHEN store_label = 'US' THEN 'United States'
			 WHEN store_label = 'AT' THEN 'Austria'
			 WHEN store_label = 'ES' THEN 'Spain'
			 WHEN store_label = 'DE' THEN 'Germany'
			 WHEN store_label = 'Total' THEN 'No Info'
			 END AS country,
		CASE WHEN b2b_split = 'Freelancers' THEN 'B2B-Total'
			 WHEN b2b_split = 'Non-Freelancers' THEN 'B2B-Total'
			 WHEN channel_type = 'Retail' THEN 'Partnerships-Total'
			 ELSE 'Grover-'+ store_label
			 END AS store_country,
		CASE WHEN measures = 'ASV' THEN amount END AS active_sub_value_daily_target ,
		CASE WHEN measures = 'Acquired ASV' THEN amount END AS acquired_subvalue_daily,
		CASE WHEN measures = 'Cancelled ASV' THEN amount END AS cancelled_subvalue_daily
	FROM dm_commercial.r_commercial_daily_targets_since_2022
	WHERE  date_trunc('month',to_date)>='2022-06-01'
)
, commercial_targets_daily_store_commercial_2022_2nd_part AS (
	SELECT  
		datum, 
		country,
		store_country,
		d.month_bom,
		d.month_eom,
		SUM(active_sub_value_daily_target) AS active_sub_value_daily_target ,
		SUM(acquired_subvalue_daily) AS acquired_subvalue_daily,
		SUM(cancelled_subvalue_daily) AS cancelled_subvalue_daily
	FROM commercial_targets_daily_store_commercial_from_2022_2nd_part_table t
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
		CASE WHEN store = 'B2C' THEN 'Grover-'+country
			 WHEN store = 'Retail' THEN 'Partnerships-Total'
			 WHEN store = 'B2B-Freelancers' THEN 'B2B-Total'
			 WHEN store = 'B2B-Non-Freelancers' THEN 'B2B-Total'
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
	SELECT * FROM commercial_targets_daily_store_commercial_2022_2nd_part 
	UNION ALL
	SELECT * FROM commercial_targets_daily_store_commercial_2022_1st_part 
)
SELECT DISTINCT 
	COALESCE(l.fact_date,t.datum) AS fact_date,
	COALESCE(l.first_subscription_store,t.store_country) AS first_subscription_store,
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
	t.active_sub_value_daily_target,
	t.cancelled_subvalue_daily,
	t.acquired_subvalue_daily,
	d.day_is_first_of_month,
	d.day_is_last_of_month,
	COALESCE(h.committed_sub_value,0) AS committed_sub_value,
	COALESCE(h.commited_sub_revenue_future,0) AS commited_sub_revenue_future,
	COALESCE(p.collected_subscription_revenue,0) AS collected_subscription_revenue
FROM active_subs_final l
LEFT JOIN acquisition a 
	ON l.fact_date::date=a.start_date::date 
	AND l.first_subscription_store=a.first_subscription_store
	AND l.country = a.country
LEFT JOIN churn c 
	ON l.fact_date::date=c.cancellation_date::date 
	AND l.first_subscription_store=c.first_subscription_store
	AND l.country = c.country
FULL JOIN commercial_targets_daily_store_commercial t 
	ON t.datum::date=l.fact_date::date 
	AND l.first_subscription_store=t.store_country
	AND l.country = t.country
LEFT JOIN public.dim_dates d 
	ON d.datum::date=l.fact_date::date
LEFT JOIN hisotrical h 
	ON h.reporting_date::date=l.fact_date::date
	AND l.first_subscription_store=h.first_subscription_store
	AND l.country = h.country
LEFT JOIN payments p 
	ON p.paid_date::date=l.fact_Date::date
	AND p.first_subscription_store=l.first_subscription_store
	AND l.country = p.country
;

GRANT SELECT ON dwh.reporting_churn_first_allocation_store TO tableau;
