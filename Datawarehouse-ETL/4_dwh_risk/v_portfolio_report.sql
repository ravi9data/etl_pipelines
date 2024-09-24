CREATE OR REPLACE VIEW dm_risk.v_portfolio_report AS
WITH clean_allocations_table AS  -- Removing duplicates FROM this TABLE. 
(
SELECT 
	asset_id,
	allocation_id,
	subscription_id,
	allocated_at,
	delivered_at,
	failed_delivery_at,
	return_delivery_date,
	return_shipment_at, 
	ROW_NUMBER() OVER (PARTITION BY allocation_id ORDER BY return_shipment_at DESC) AS row_num
FROM ods_operations.allocation_shipment asm 
)
, luxco_reporting AS 
(
SELECT 
	reporting_date,
	asset_id,
	final_price
FROM ods_production.spv_report_master
)
, allocations AS 
(
SELECT DISTINCT
	al.allocation_id , 
	CASE WHEN s.store_label IN 
			('Grover - Austria online',
			'Grover - Germany online',
			'Grover - Netherlands online',
			'Grover - Spain online',
			'Grover - United States online') 
			THEN s.country_name
			ELSE s.store_short  
	END AS store_label,
	s.customer_type,
	s.country_name AS store_country,
	al.delivered_at , 
	al.return_delivery_date,
	al.return_shipment_at, -- this IS courier pick-up date. 
	s.cancellation_date,
	CASE WHEN s.cancellation_reason_new = 'DEBT COLLECTION' THEN 1 ELSE 0 END AS is_dc_cancellation,
	CASE
		WHEN a.currency = 'USD'
			THEN  COALESCE(spv.final_price, spv_2.final_price, spv_3.final_price) * fx.exchange_rate_eur
		ELSE COALESCE(spv.final_price, spv_2.final_price, spv_3.final_price)
	END AS value_at_subs_start_eur
FROM clean_allocations_table al
LEFT JOIN master.asset a 
	ON a.asset_id = al.asset_id
INNER JOIN master.subscription s
	ON s.subscription_id = al.subscription_id
	AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
LEFT JOIN luxco_reporting spv
	ON al.asset_id = spv.asset_id
	AND spv.reporting_date::DATE = CASE WHEN al.allocated_at < '2018-06-30' THEN '2018-06-30' ELSE al.allocated_at END :: DATE
	AND a.currency IN ('EUR', 'USD')
LEFT JOIN luxco_reporting spv_2 -- BACKUP 1, take END OF MONTH.
	ON al.asset_id = spv_2.asset_id
	AND spv_2.reporting_date::DATE = LAST_DAY(al.allocated_at)::DATE
	AND a.currency IN ('EUR', 'USD')
LEFT JOIN luxco_reporting spv_3 -- BACKUP 2, take END OF NEXT MONTH.
	ON al.asset_id = spv_3.asset_id
	AND spv_3.reporting_date::DATE = LAST_DAY(DATE_ADD('month', 1, al.allocated_at))::DATE
	AND a.currency IN ('EUR', 'USD')
LEFT JOIN trans_dev.daily_exchange_rate fx
	ON fx.date_::DATE = al.allocated_at::DATE
WHERE
	al.delivered_at IS NOT NULL
		AND al.failed_delivery_at IS NULL 
			AND al.row_num = 1 -- cleaned allocation data
)
, outstanding_assets AS 
(
SELECT
	dd.datum AS fact_month,
	al.store_label,
	al.store_country,
	al.customer_type,
	al.allocation_id,
	al.is_dc_cancellation,
	CASE
		WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
    		THEN DATEADD('day', 14, sp.due_date)
		ELSE sp.due_date
	END AS effective_due_date,
	al.value_at_subs_start_eur AS outstanding_asset_value_eur, -- Keeping NULLS intentionally FOR later CHECK.
	COALESCE(DATEDIFF('day', effective_due_date, dd.datum),0)::INT AS dpd, 
	ROW_NUMBER() OVER (PARTITION BY dd.datum, al.allocation_id ORDER BY effective_due_date) row_num-- earliest unpaid payment
FROM public.dim_dates dd
LEFT JOIN allocations al
	ON dd.datum BETWEEN al.delivered_at::DATE  -- Asset Must be delivered
		-- Asset must be outstanding or in Active state. 
		AND COALESCE(DATEADD('day', -1, LEAST(al.return_delivery_date, al.cancellation_date, al.return_shipment_at)), 
				DATEADD('day', 1, dd.datum))::DATE
LEFT JOIN master.subscription_payment sp 
	ON sp.allocation_id = al.allocation_id
	AND 
		(CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
    		THEN DATEADD('day', 14, sp.due_date) -- correcting FOR REAL b2b pbi due date
		ELSE sp.due_date END) < dd.datum
	AND (sp.paid_date IS NULL OR sp.paid_date > dd.datum)
	AND sp.status != 'PLANNED'
WHERE day_is_last_of_month = 1
	-- We have the last complete month (e.g. August 2023) and back until that months 1 year prior (e.g. August 2022)
	AND dd.datum BETWEEN DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('year', -3, CURRENT_DATE))) AND CURRENT_DATE
)
, outstanding_assets_grouped AS 
(
SELECT
	fact_month,
	store_label,
	store_country,
	customer_type,
	dpd,
	COUNT(DISTINCT allocation_id) AS num_outstanding_assets,
	COUNT(DISTINCT CASE WHEN outstanding_asset_value_eur IS NOT NULL THEN allocation_id END) AS num_outstanding_assets_w_value, -- NULL checks.
	COALESCE(SUM(outstanding_asset_value_eur),0) AS total_outstanding_asset_value,
	NULL AS num_active_subscriptions,
	NULL AS num_dc_cancellations
FROM
	outstanding_assets
WHERE 
	row_num = 1 -- This IS TO link 1:1 allocation TO earliest payment AFTER SP JOIN.
GROUP BY 1,2,3,4,5
)
, active_subscriptions_monthly AS -- Subscription LEVEL. 
(
SELECT
	dd.datum AS fact_month,
	CASE WHEN s.store_label IN 
			('Grover - Austria online',
			'Grover - Germany online',
			'Grover - Netherlands online',
			'Grover - Spain online',
			'Grover - United States online') 
			THEN s.country_name
			ELSE s.store_short  
	END AS store_label,
	s.customer_type,
	s.country_name AS store_country,  
	COUNT(DISTINCT s.subscription_id) AS num_active_subscriptions 
FROM public.dim_dates dd
LEFT JOIN master.subscription s 
	ON s.start_date::DATE <= dd.datum -- Subscription started BY EOM.
	AND COALESCE(s.cancellation_date::DATE, DATE_ADD('day', 1, dd.datum)) >= DATE_TRUNC('month', dd.datum) 
	AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
WHERE dd.day_is_last_of_month = 1
	AND dd.datum BETWEEN DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('year', -3, CURRENT_DATE))) AND CURRENT_DATE
GROUP BY 1,2,3,4
)
, dc_cancellations_monthly AS
(
SELECT
	dd.datum AS fact_month,
	CASE WHEN s.store_label IN 
			('Grover - Austria online',
			'Grover - Germany online',
			'Grover - Netherlands online',
			'Grover - Spain online',
			'Grover - United States online') 
			THEN s.country_name
			ELSE s.store_short  
	END AS store_label,
	s.customer_type,
	s.country_name AS store_country,
	COUNT(DISTINCT s.subscription_id) AS num_dc_cancellations
FROM public.dim_dates dd
LEFT JOIN master.subscription s 
	ON s.cancellation_date::DATE BETWEEN DATE_TRUNC('month', dd.datum) AND dd.datum
	AND s.cancellation_reason_new = 'DEBT COLLECTION'
	AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
WHERE dd.day_is_last_of_month = 1
	AND dd.datum BETWEEN DATE_ADD('day', -1, DATE_TRUNC('month', DATE_ADD('year', -3, CURRENT_DATE))) AND CURRENT_DATE
GROUP BY 1,2,3,4
)
, dc_metrics_combined AS 
(
SELECT 
	COALESCE(a.fact_month, dc.fact_month) AS fact_month,
	COALESCE(a.store_label, dc.store_label) AS store_label,
	COALESCE(a.store_country, dc.store_country) AS store_country,
	COALESCE(a.customer_type, dc.customer_type) AS customer_type,
	NULL AS dpd,
	NULL AS num_outstanding_assets,
	NULL AS num_outstanding_assets_w_value, 
	NULL AS total_outstanding_asset_value,
	COALESCE(a.num_active_subscriptions,0) AS num_active_subscriptions,
	COALESCE(dc.num_dc_cancellations,0) AS num_dc_cancellations
FROM active_subscriptions_monthly a
FULL OUTER JOIN dc_cancellations_monthly dc
	ON dc.fact_month = a.fact_month
	AND dc.store_label = a.store_label
	AND dc.customer_type = a.customer_type
	AND dc.store_country = a.store_country
)
SELECT 
	fact_month,
	store_label,
	store_country,
	customer_type,
	dpd,
	num_outstanding_assets,
	num_outstanding_assets_w_value,
	total_outstanding_asset_value,
	num_active_subscriptions,
	num_dc_cancellations
FROM outstanding_assets_grouped
UNION
SELECT 
	fact_month,
	store_label,
	store_country,
	customer_type,
	dpd,
	num_outstanding_assets,
	num_outstanding_assets_w_value, 
	total_outstanding_asset_value,
	num_active_subscriptions,
	num_dc_cancellations
FROM dc_metrics_combined
WITH NO SCHEMA BINDING ;

GRANT SELECT ON dm_risk.v_portfolio_report TO risk_users_redash, tableau ;
