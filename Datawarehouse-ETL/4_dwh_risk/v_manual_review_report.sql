CREATE OR REPLACE VIEW dm_risk.v_manual_review_report AS 
WITH manual_review_raw AS  -- Clean results field.
(
SELECT
 	customer_id,
 	order_id,
	CASE
		WHEN mr.RESULT IN ('APPROVED', 'Approved') THEN 'APPROVED'
		WHEN mr.RESULT IN ('DECLINED', 'ID_VERIFICATION') THEN mr.RESULT
	END AS mr_result,
	result_reason,
	result_comment,
	employee_id,
	created_at::timestamp as decision_date,
	updated_at::timestamp,
	consumed_at::timestamp
FROM stg_curated.risk_internal_eu_manual_review_result_v1 mr
WHERE mr_result IS NOT NULL
),
results_grouping AS 
(
	SELECT 
		customer_id,
		order_id,
		count(DISTINCT mr_result) AS num_of_unique_results
	FROM manual_review_raw
	GROUP BY 1,2
)
, manual_review_eu_clean_up AS ( -- Take FIRST approve OR decline, AND WHERE neither exist take the ID verification IF EXISTS.
     SELECT 
        employee_id,
        customer_id,
        order_id,
        mr_result,
        result_reason,
        result_comment,
        decision_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id, order_id ORDER BY decision_date, updated_at, consumed_at) as rowno
    FROM manual_review_raw
    WHERE mr_result IN ('APPROVED', 'DECLINED')
    UNION 
    SELECT 
        mr.employee_id,
        mr.customer_id,
        mr.order_id,
        mr.mr_result,
        mr.result_reason,
        mr.result_comment,
        mr.decision_date,
        ROW_NUMBER() OVER (PARTITION BY mr.customer_id, mr.order_id ORDER BY mr.decision_date, mr.updated_at, mr.consumed_at) as rowno
    FROM manual_review_raw mr
    INNER JOIN results_grouping mrr
      ON mrr.customer_id = mr.customer_id
     AND mrr.order_id = mr.order_id
     AND mrr.num_of_unique_results = 1
    WHERE mr.mr_result NOT IN ('APPROVED', 'DECLINED') 
)
, manual_review_us_clean_up AS
(
    SELECT
        created_by AS employee_id,
        customer_id,
        order_id,
        CASE WHEN status ILIKE '%clin%' THEN 'DECLINED' WHEN status ILIKE '%prov%' THEN 'APPROVED' END as mr_result, -- Cleaning required here
        reason AS result_reason,
        comments as result_comment,
        created_at::timestamp AS decision_date,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at, updated_at, consumed_at) AS rowno
	FROM stg_curated.risk_internal_us_risk_manual_review_order_v1
	WHERE mr_result IN ('APPROVED', 'DECLINED') 
)
, mr_final AS 
(
    SELECT
    	customer_id,
        order_id,
        employee_id,
        decision_date,
        mr_result,
        result_reason,
        result_comment
    FROM manual_review_eu_clean_up eu
    WHERE eu.rowno = 1
    UNION
    SELECT
        customer_id,
        order_id,
        employee_id,
        decision_date,
        mr_result,
        result_reason,
        result_comment
    FROM manual_review_us_clean_up us
    WHERE us.rowno = 1
        AND NOT EXISTS (SELECT NULL FROM manual_review_eu_clean_up eu WHERE eu.order_id = us.order_id)
)
, payment_information AS 
(
SELECT
	sp.order_id, 
	-----------------------------
	-- Overall Payment Metrics --
	-----------------------------
	---------10 DPD
	COUNT(DISTINCT CASE WHEN DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_10dpd,
	SUM(CASE WHEN DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_10dpd,
	SUM(CASE WHEN DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 10, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_10dpd,
	---------30 DPD
	COUNT(DISTINCT CASE WHEN DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_30dpd,
	SUM(CASE WHEN DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_30dpd,
	SUM(CASE WHEN DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 30, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_30dpd,	
	-------------------------
	-- 2nd Payment Metrics --
	-------------------------
	---------10 DPD
	COUNT(DISTINCT CASE WHEN payment_number = 2
		AND DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_2ndpay_10dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_2ndpay_10dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 10, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_2ndpay_10dpd,
	---------30 DPD
	COUNT(DISTINCT CASE WHEN payment_number = 2
		AND DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_2ndpay_30dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_2ndpay_30dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 30, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_2ndpay_30dpd
FROM master.subscription_payment sp
WHERE sp.currency IN ('EUR', 'USD')
	AND sp.status NOT IN ('PLANNED', 'HELD', 'REFUND')
	AND payment_number::INT > 1 -- Recurrent payments ONLY.
	AND sp.order_id IS NOT NULL -- One NULL FOUND.
	AND due_date::DATE < CURRENT_DATE -- SOME Future paid and unpaid payments (mostly unpaid) not classified as "planned". 
GROUP BY 1
)
SELECT 
    o.customer_id,
    o.order_id,
	o.submitted_date::DATE,
    mr.decision_date::DATE,	
	mr.employee_id,
    mr.mr_result,
    mr.result_reason,
    mr.result_comment,
    o.store_country,
    o.store_label,
    o.new_recurring,
    c.customer_type,
    o.new_recurring_risk,
    oj.order_journey_mapping_risk,
    CASE WHEN o.paid_date IS NOT NULL THEN 1 ELSE 0 END AS is_paid_order,
    CASE WHEN mr.mr_result = 'APPROVED' THEN 1 ELSE 0 END as is_approved_order,
    CASE WHEN mr.mr_result = 'DECLINED' THEN 1 ELSE 0 END as is_declined_order,
    CASE WHEN mr.mr_result = 'ID_VERIFICATION' THEN 1 ELSE 0 END as is_id_verification,
    CASE WHEN mr.order_id IS NOT NULL THEN 1 ELSE 0 END as overall_mr_orders,
    -- Recurrent Payments
    pm.no_dues_10dpd,
    pm.amount_due_10dpd,
    pm.amount_paid_10dpd, 
    pm.no_dues_30dpd,
    pm.amount_due_30dpd,
    pm.amount_paid_30dpd, 
    -- 2nd Payment Only
    pm.no_dues_2ndpay_10dpd,
    pm.amount_due_2ndpay_10dpd,
    pm.amount_paid_2ndpay_10dpd, 
    pm.no_dues_2ndpay_30dpd,
    pm.amount_due_2ndpay_30dpd,
    pm.amount_paid_2ndpay_30dpd   
FROM master.order o
LEFT JOIN mr_final mr
	ON mr.order_id = o.order_id
LEFT JOIN master.customer c 
	ON c.customer_id = o.customer_id
LEFT JOIN payment_information pm
	ON pm.order_id = o.order_id
LEFT JOIN ods_production.order_journey oj 
	ON oj.order_id = o.order_id
WHERE o.submitted_date::date > DATEADD('year', -1, CURRENT_DATE)
WITH NO SCHEMA BINDING;

