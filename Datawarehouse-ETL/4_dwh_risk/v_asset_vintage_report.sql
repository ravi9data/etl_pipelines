
CREATE OR REPLACE VIEW dm_risk.v_asset_vintage_report AS 
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
	s.rental_period,
	s.country_name AS store_country,
	s.category_name,
	s.subcategory_name,
	al.delivered_at::DATE AS delivery_date, 
	al.return_delivery_date,
	al.return_shipment_at, -- this IS courier pick-up date. 
	s.cancellation_date,
	CASE WHEN s.cancellation_reason_new = 'DEBT COLLECTION' THEN s.cancellation_date END AS dc_cancellation_date,
	CASE
		WHEN a.currency = 'USD'
			THEN  COALESCE(spv.final_price, spv_2.final_price, spv_3.final_price) * fx.exchange_rate_eur
		ELSE COALESCE(spv.final_price, spv_2.final_price, spv_3.final_price)
	END AS value_at_subs_start_eur,
	ROW_NUMBER() OVER (PARTITION BY al.allocation_id) AS row_num -- delete duplicates indiscriminately... until better option arises 
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
, month_range AS 
(
SELECT 
	ordinal AS numbers
FROM public.numbers
WHERE ordinal < 240 -- 240 months = 20 years. TO reduce later JOIN SIZE. 
)
, month_on_book_dates AS 
(
SELECT 
	DATE_ADD('month', mr.numbers, al.delivery_date)::DATE AS fact_month,
	DATE_DIFF('month', al.delivery_date, fact_month) AS months_on_book,
	al.allocation_id,
	al.delivery_date,
	al.category_name,
	al.subcategory_name,
	al.store_label,
	al.customer_type,
	al.rental_period,
	al.store_country,
	al.return_delivery_date,
	al.cancellation_date,
	al.dc_cancellation_date,
	al.return_shipment_at,
	al.value_at_subs_start_eur AS outstanding_asset_value_eur  -- Intentionally no coalesce here, keep NULLS FOR downstream checks.
FROM allocations al 
CROSS JOIN month_range mr 
WHERE row_num = 1
	AND fact_month < CURRENT_DATE 
		AND delivery_date::DATE >= DATE_ADD('month', -37, DATE_TRUNC('month', CURRENT_DATE))
)
, asset_vintage_dpds AS 
(
SELECT
	dd.fact_month,
	dd.months_on_book,
	dd.delivery_date,
	dd.category_name,
	dd.subcategory_name,
	dd.store_label,
	dd.store_country,
	dd.customer_type,
	dd.rental_period,
	dd.allocation_id,
	dd.dc_cancellation_date,
	dd.return_delivery_date,
	CASE
		WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
    		THEN DATEADD('day', 14, sp.due_date)
		ELSE sp.due_date
	END AS effective_due_date,
	dd.outstanding_asset_value_eur, -- Intentionally no coalesce here, keep NULLS FOR downstream checks.
	CASE WHEN fact_month >= dd.dc_cancellation_date::DATE THEN 1 ELSE 0 END AS is_dc_cancelled,
	CASE WHEN fact_month >= dd.return_shipment_at::DATE THEN 1 ELSE 0 END AS is_returned_at_intermediary,
	CASE WHEN fact_month >= dd.return_delivery_date::DATE THEN 1 ELSE 0 END AS is_received_at_warehouse,
	COALESCE(DATEDIFF('day', effective_due_date, dd.fact_month),0) AS dpd, -- We will EXCLUDE certain DPDs AS Delinquent based ON Asset Status IN Report. 
	ROW_NUMBER() OVER (PARTITION BY dd.allocation_id, dd.months_on_book ORDER BY effective_due_date) row_num -- Earliest unpaid payment
FROM month_on_book_dates dd
LEFT JOIN master.subscription_payment sp 
	ON sp.allocation_id = dd.allocation_id
	AND 
		(CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
    		THEN DATEADD('day', 14, sp.due_date) -- correcting FOR REAL b2b pbi due date
		ELSE sp.due_date END) < LEAST(dd.fact_month, dd.dc_cancellation_date, dd.return_delivery_date) -- Only if earlier than fact date OR cancellation date if it exists. 
	AND (sp.paid_date IS NULL OR sp.paid_date > dd.fact_month)
	AND sp.status != 'PLANNED'
)
, asset_vintage_grouped AS 
(
SELECT
	LAST_DAY(fact_month) AS fact_month, -- Take EOM date
	LAST_DAY(delivery_date) AS delivery_month,    -- Delivery Cohort
	months_on_book,
	store_label,
	store_country,
	customer_type,
	rental_period,
	category_name,
	subcategory_name,
	is_dc_cancelled,
	is_returned_at_intermediary,
	is_received_at_warehouse,
	dpd,
	COUNT(DISTINCT allocation_id) AS number_allocated_assets, 
	COALESCE(SUM(outstanding_asset_value_eur),0) AS total_asset_value_delivered,
	NULL AS num_active_subscriptions,
	NULL AS num_dc_cancellations
FROM
	asset_vintage_dpds
WHERE 
	row_num = 1 -- This IS TO link 1:1 allocation TO earliest payment AFTER SP JOIN.
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
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
	s.rental_period,
	s.country_name AS store_country,  
	COUNT(DISTINCT s.subscription_id) AS num_active_subscriptions 
FROM public.dim_dates dd
LEFT JOIN master.subscription s 
	ON s.start_date::DATE <= dd.datum -- Subscription started BY EOM.
	AND COALESCE(s.cancellation_date::DATE, DATE_ADD('day', 1, dd.datum)) >= DATE_TRUNC('month', dd.datum) 
	AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
WHERE dd.day_is_last_of_month = 1
	AND dd.datum BETWEEN DATE_ADD('month', -37, DATE_TRUNC('month', CURRENT_DATE)) AND CURRENT_DATE
GROUP BY 1,2,3,4,5
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
	s.rental_period,
	s.country_name AS store_country,
	COUNT(DISTINCT s.subscription_id) AS num_dc_cancellations
FROM public.dim_dates dd
LEFT JOIN master.subscription s 
	ON s.cancellation_date::DATE BETWEEN DATE_TRUNC('month', dd.datum) AND dd.datum
	AND s.cancellation_reason_new = 'DEBT COLLECTION'
	AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
WHERE dd.day_is_last_of_month = 1
	AND dd.datum BETWEEN DATE_ADD('month', -37, DATE_TRUNC('month', CURRENT_DATE)) AND CURRENT_DATE
GROUP BY 1,2,3,4,5
)
, dc_metrics_combined AS 
(
SELECT 
	COALESCE(a.fact_month, dc.fact_month) AS fact_month,
	NULL AS months_on_book,
	NULL AS delivery_month,
	COALESCE(a.store_label, dc.store_label) AS store_label,
	COALESCE(a.store_country, dc.store_country) AS store_country,
	COALESCE(a.customer_type, dc.customer_type) AS customer_type,
	COALESCE(a.rental_period, dc.rental_period) AS rental_period,
	NULL AS category_name,
	NULL AS subcategory_name,
	NULL AS is_dc_cancelled,
	NULL AS is_returned_at_intermediary,
	NULL AS is_received_at_warehouse,
	NULL AS dpd,
	NULL AS number_allocated_assets,
	NULL AS total_asset_value_delivered,
	COALESCE(a.num_active_subscriptions,0) AS num_active_subscriptions,
	COALESCE(dc.num_dc_cancellations,0) AS num_dc_cancellations
FROM active_subscriptions_monthly a
FULL OUTER JOIN dc_cancellations_monthly dc
	ON dc.fact_month = a.fact_month
	AND dc.store_label = a.store_label
	AND dc.customer_type = a.customer_type
	AND dc.store_country = a.store_country
	AND dc.rental_period = a.rental_period
	)
SELECT 
	fact_month,
	months_on_book,
	delivery_month,
	store_label,
	store_country,
	customer_type,
	rental_period,
	category_name,
	subcategory_name,
	is_dc_cancelled,
	is_returned_at_intermediary,
	is_received_at_warehouse,
	dpd,
	number_allocated_assets,
	total_asset_value_delivered,
	num_active_subscriptions,
	num_dc_cancellations
FROM asset_vintage_grouped
UNION
SELECT 
	fact_month,
	months_on_book,
	delivery_month,
	store_label,
	store_country,
	customer_type,
	rental_period,
	category_name,
	subcategory_name,
	is_dc_cancelled,
	is_returned_at_intermediary,
	is_received_at_warehouse,
	dpd,
	number_allocated_assets,
	total_asset_value_delivered,
	num_active_subscriptions,
	num_dc_cancellations
FROM dc_metrics_combined
WITH NO SCHEMA BINDING ;

GRANT ALL ON dm_risk.v_asset_vintage_report TO tableau ; 
