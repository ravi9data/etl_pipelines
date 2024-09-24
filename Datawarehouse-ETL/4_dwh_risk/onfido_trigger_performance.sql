-- Onfido Analysis, June 2022
-- This is a base layer upon which Views are built. 
DROP TABLE IF EXISTS dm_risk.onfido_trigger_performance;
DROP TABLE IF EXISTS control_group_final; 

CREATE TEMP TABLE control_group_final AS
WITH onfido_start_date AS    -- 181430 rows/orders 168k, custoemrs,
(
	SELECT MIN(submitted_date::date)
	FROM master."ORDER" o
	LEFT JOIN ods_production.order_scoring os USING(order_id)
	WHERE os.onfido_trigger IS NOT NULL
)
, exclude_from_control_group AS   -- i.e. customers who have been asked onfido AT LEAST once.
(
SELECT user_id AS customer_id,
	COUNT(onfido_trigger) AS request_count
FROM ods_production.order_scoring
GROUP BY 1
HAVING request_count > 0
)
, control_group_raw AS
(
	SELECT
		o.customer_id,
		o.order_id,
		o.submitted_date AS submitted_timestamp_onfido,
		'Control Group' AS onfido_trigger,
		o.store_country,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY submitted_date) AS row_num -- FIRST ORDER. Seems TO be the correct METHOD.
	FROM master."order" o
	LEFT JOIN ods_production.order_scoring os USING(order_id)
	WHERE o.submitted_date IS NOT NULL -- Ensure same conditions AS TRIGGER GROUP.
		AND NOT EXISTS (SELECT NULL FROM exclude_from_control_group ce WHERE ce.customer_id = o.customer_id) -- Remove onfido-triggered customers
		AND os.onfido_trigger IS NULL -- Likely can DELETE this line.
		AND o.store_country IN ('Germany', 'Austria', 'Netherlands', 'Spain')		
)
, control_group_firstorder AS
(
	SELECT *
	FROM control_group_raw
	WHERE row_num = 1
)
SELECT *
FROM control_group_firstorder
WHERE submitted_timestamp_onfido >= (SELECT * FROM onfido_start_date)  -- Ensure we ARE operating WITHIN the same timeframe.
ORDER BY RAND(5)   -- Want a random sample FROM this non-onfido population. Have SET a random seed OF 5 TO GET a STABLE population.
LIMIT 10000; -- The 10K  corresponds TO apprx 7% OF the base data count (129k) in June 22, a size in the range of other trigger sizes. 


CREATE TABLE dm_risk.onfido_trigger_performance AS
WITH base_data_onfido AS    -- 181430 rows/orders 168k, custoemrs,
(
	SELECT
		o.customer_id,
		o.order_id,
		o.submitted_date AS submitted_timestamp_onfido,
		os.onfido_trigger,
		o.store_country,
		-- os.verification_state,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY submitted_date) AS row_num   -- Taking 1st onfido verification.
	FROM master."order" o
	LEFT JOIN ods_production.order_scoring os USING(order_id)
	WHERE os.onfido_trigger IS NOT NULL 
		AND o.submitted_date IS NOT NULL -- Ensure same conditions AS CONTROL GROUP. 
		AND os.verification_state IN ('verified')
)
, base_data AS 
(
	(SELECT * FROM base_data_onfido WHERE row_num = 1)
	UNION ALL
	(SELECT * FROM control_group_final) -- ROW num IS already implemented above.
)
, customer_labels_raw AS -- organised ON customer id LEVEL. This could potentially introduce duplicates at some point so we will clean up.
(
	SELECT 
		customer_id, 
		label,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS row_num
	FROM stg_order_approval.customer_labels2
)
, customer_labels AS
(
	SELECT 
		customer_id, 
		label
	FROM customer_labels_raw
	WHERE row_num = 1
)
, order_payment_performance AS 
(
	SELECT sp.customer_id,
		sp.order_id,
	  	max(sp.dpd) as max_dpd_order,
	  	COUNT(CASE WHEN sp.status = 'PAID' THEN sp.status END) AS payments_paid,
	  	COUNT(*) AS payments_total,
	  	payments_total - payments_paid  AS payments_due,
	  	COALESCE(sum(sp.amount_paid),0) as total_amount_paid,
	  	COALESCE(sum(sp.amount_due),0) as total_amount_due
	FROM master.subscription_payment sp
	WHERE EXISTS (SELECT NULL FROM base_data bd WHERE sp.customer_id = bd.customer_id) -- SELECT ONLY FOR onfido + control group customers. 
		AND due_date <= DATE_ADD('month', -1, CURRENT_DATE) -- EXCLUDE CURRENT MONTH (may not yet be resolved)
		AND sp.status != 'PLANNED'
		AND sp.payment_type != 'FIRST'
		AND asset_was_delivered = 1 -- FOR "normal" cases WHERE NO problems occured during frist MONTH. 
		AND amount_due > 0 -- ignoring the effects OF discounts/vouchers. prevents divide BY zero error later AND focuses ON monetary payments.
	GROUP BY 1,2
)
, order_payment_performance_enriched AS
(
	SELECT
		pp.*,
		o.submitted_date AS submitted_timestamp,
		bd.submitted_timestamp_onfido,
		CASE
			WHEN submitted_timestamp >= submitted_timestamp_onfido 
				THEN 1
			ELSE 0
		END AS is_order_post_onfido_verification_inclusive, -- includes 1st/onfido order
		CASE
			WHEN submitted_timestamp > submitted_timestamp_onfido 
				THEN 1
			ELSE 0
		END AS is_order_post_onfido_verification_exclusive -- excludes 1st/onfido order
	FROM order_payment_performance pp
	LEFT JOIN master."order" o        -- This JOIN performed fine.
		ON pp.order_id = o.order_id
	LEFT JOIN base_data bd 
		ON pp.customer_id  = bd.customer_id 
)
, customer_payment_performance_all AS
(
	SELECT 
		customer_id,
		COUNT(DISTINCT order_id) AS orders_after_verification_all,
		max(max_dpd_order) as customer_max_dpd_all,
	  	SUM(payments_paid) AS customer_payments_paid_all,
	  	SUM(payments_total) AS customer_payments_total_all
	  	-- SUM(payments_due) AS customer_payments_due,
	  	-- COALESCE(sum(total_amount_paid),0) as customer_total_amount_paid,
	  	-- COALESCE(sum(total_amount_due),0) as customer_total_amount_due
	FROM order_payment_performance_enriched
	WHERE is_order_post_onfido_verification_inclusive = 1
	GROUP BY customer_id
)
, customer_payment_performance_postonfido AS
(
	SELECT 
		customer_id,
		max(max_dpd_order) as customer_max_dpd_postonfido,
	  	SUM(payments_paid) AS customer_payments_paid_postonfido,
	  	SUM(payments_total) AS customer_payments_total_postonfido
	  	-- SUM(payments_due) AS customer_payments_due,
	  	-- COALESCE(sum(total_amount_paid),0) as customer_total_amount_paid,
	  	-- COALESCE(sum(total_amount_due),0) as customer_total_amount_due
	FROM order_payment_performance_enriched
	WHERE is_order_post_onfido_verification_exclusive = 1
	GROUP BY customer_id
)
, orderapprovals_postverification AS
(
	SELECT
		bd.customer_id,
		COUNT(DISTINCT CASE WHEN o.approved_date IS NOT NULL THEN o.order_id END) AS postverification_approvedorders
	FROM base_data bd
	LEFT JOIN master."order" o
		ON bd.customer_id = o.customer_id
		AND o.submitted_date >= bd.submitted_timestamp_onfido 
	GROUP BY 1
)
, data_final AS
(
	SELECT
		bd.customer_id,
		bd.order_id,
		bd.submitted_timestamp_onfido::date AS submitted_date,
		bd.onfido_trigger,
		bd.store_country,
		l.label,
		oa.postverification_approvedorders,		
		-- Onfido order metrics (order related to 1st onfido verification)
		opp.max_dpd_order AS order_max_dpd,
		opp.payments_paid AS order_payments_count_paid,
		opp.payments_total AS order_payments_count_total,
		-- Customer performance metrics (TOTAL - Onfido + subsequent orders)
		cppt.customer_max_dpd_all as customer_max_dpd_all,
		cppt.customer_payments_paid_all  AS customer_payments_count_paid_all,
		cppt.customer_payments_total_all AS customer_payments_count_total_all,
		-- Customer performance metrics (Post-Onfido order only)
		cpp.customer_max_dpd_postonfido as customer_max_dpd_postonfido,
		cpp.customer_payments_paid_postonfido  AS customer_payments_count_paid_postonfido,
		cpp.customer_payments_total_postonfido AS customer_payments_count_total_postonfido,
		-- Data Enriching	
		(DATE_TRUNC('week', c.customer_acquisition_cohort::date))::date AS customer_cohort_week_
	FROM base_data bd
	LEFT JOIN customer_labels l ON bd.customer_id = l.customer_id
	LEFT JOIN order_payment_performance opp ON bd.order_id = opp.order_id  -- could ALSO JOIN ON customerid AS well 
	LEFT JOIN customer_payment_performance_all cppt ON bd.customer_id = cppt.customer_id
	LEFT JOIN customer_payment_performance_postonfido cpp ON bd.customer_id = cpp.customer_id
	LEFT JOIN master.customer c ON bd.customer_id = c.customer_id 
	LEFT JOIN orderapprovals_postverification oa ON bd.customer_id = oa.customer_id 
)
SELECT *
FROM data_final;

DROP TABLE IF EXISTS control_group_final; 