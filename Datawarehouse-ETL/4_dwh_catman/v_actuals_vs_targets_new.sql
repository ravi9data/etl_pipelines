-- dm_commercial.v_actuals_vs_targets_new source

CREATE OR REPLACE VIEW dm_commercial.v_actuals_vs_targets_new AS
WITH base AS (
	SELECT 
		d.datum AS reporting_date,
		s2.country_name,
		s2.customer_type,
		s2.customer_id,
		s2.store_short,
		s2.store_name,
		CASE 
           WHEN s2.store_name = 'Grover B2B Germany' THEN 'Grover B2B Germany'
           ELSE 'Other stores'
        END AS is_b2b_germany,
		s2.category_name,
		s2.subcategory_name,
		s2.currency,
		s.subscription_value_lc AS actual_subscription_value,
		CASE
			WHEN s2.country_name = 'United States' AND ah.purchased_date >= '2022-11-01'
			 THEN s.subscription_value_lc 
		END AS subscription_value_US_nov22_cohort,
		CASE 
			WHEN s2.country_name <> 'United States' AND ah.purchased_date >= '2023-05-01'
			 THEN s.subscription_value_lc 
		END AS subscription_value_EU_may23_cohort,
		ROW_NUMBER () OVER (PARTITION BY s.subscription_id, d.datum ORDER BY ah.created_at,ah.updated_at ASC) AS rowno --missing one piece
	FROM public.dim_dates d
	LEFT JOIN ods_production.subscription_phase_mapping s
		ON d.datum::date >= s.fact_day::date 
		 AND d.datum::date <= coalesce(s.end_date::date, d.datum::date+1)
	LEFT JOIN master.asset_historical ah 
		ON COALESCE(ah.subscription_id,ah.active_subscription_id) = s.subscription_id 
		 AND ah.date = d.datum
	LEFT JOIN master.subscription s2	
		ON s2.subscription_id = s.subscription_id 
	WHERE d.day_is_last_of_month = 1
		AND d.datum >= '2023-01-01' AND d.datum <= current_date 
)
, payments_wo_sold_assets AS (
	SELECT 
		dateadd('day',-1,dateadd('month',1,date_trunc('month',paid_date)))::date AS reporting_date, --EoM
		s.country_name,
		s.customer_type,
		s.store_short,
		s.store_name,
		pa.currency,
		CASE 
           WHEN s.store_name = 'Grover B2B Germany' THEN 'Grover B2B Germany'
           ELSE 'Other stores'
        END AS is_b2b_germany,
        s.category_name,
		s.subcategory_name,
		sum(pa.amount_paid) AS amount_paid
	FROM ods_production.payment_all pa
	LEFT JOIN master.subscription s 
		ON s.subscription_id = pa.subscription_id
	WHERE payment_type IN (
							'CHARGE BACK SUBSCRIPTION_PAYMENT', 
						    'REFUND SUBSCRIPTION_PAYMENT', 
						    'SHIPMENT', 
						    'RECURRENT',  
						    'FIRST'
		AND paid_date::date >= '2023-01-01'					    				    
	GROUP BY 1,2,3,4,5,6,7,8,9
)	
SELECT 
	b.reporting_date,
	b.country_name,
	b.store_short,
	b.store_name,
	b.customer_type,
	b.is_b2b_germany,
	b.category_name,
	b.subcategory_name,
	b.currency,
	amount_paid,
	sum(b.actual_subscription_value) AS ASV_eom,
	sum(b.subscription_value_US_nov22_cohort) AS ASV_eom_US_nov22_cohort,
	sum(b.subscription_value_EU_may23_cohort) AS ASV_eom_EU_may23_cohort
FROM base b
LEFT JOIN payments_wo_sold_assets p
	ON p.reporting_date = b.reporting_date 
		AND p.country_name = b.country_name
		AND p.store_short = b.store_short
		AND p.store_name = b.store_name
		AND p.customer_type = b.customer_type
		AND p.is_b2b_germany = b.is_b2b_germany
		AND p.category_name = b.category_name
		AND p.subcategory_name = b.subcategory_name
		AND p.currency = b.currency
WHERE b.rowno = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_commercial.v_actuals_vs_targets_new TO hightouch;



