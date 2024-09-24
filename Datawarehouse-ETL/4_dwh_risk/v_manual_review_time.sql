CREATE OR REPLACE VIEW dm_risk.v_manual_review_time AS
WITH 
onfido AS
(
SELECT DISTINCT os.order_id
	FROM ods_production.order_scoring os
	LEFT JOIN master."order" o 
		ON o.order_id = os.order_id
	WHERE store_country <> 'United States'
	AND onfido_trigger IS NOT NULL
	UNION 
	SELECT DISTINCT order_id 
	FROM 
	stg_curated.risk_internal_us_risk_id_verification_request_v1
	WHERE verification_state IS NOT NULL
)
SELECT 
	o.order_id,
	o.customer_id,
	o.is_user_logged_in,
	o.store_id,
	o.created_date,
	o.submitted_date,
	o.manual_review_ends_at,
	o.paid_date,
	o.updated_date,
	o.approved_date,
	o.first_charge_date,
	o.canceled_date,
	o.billingcountry,
	o.shippingcountry,
	o.status,
	o.cancellation_reason,
	o.declined_reason,
	o.order_value,
	o.order_item_count,
	o.avg_plan_duration,
	o.ordered_product_category,
	o.ordered_sku,
	o.store_type,
	o.store_commercial,
	o.store_country,
	o.initial_scoring_decision,
	o.acquisition_date,
	CASE WHEN oc.order_id_onfido IS NULL THEN 'Not Onfido process'
		WHEN oc.order_id_onfido IS NOT NULL THEN 'Onfido process'
		END AS onfido_process_stg_table,
	CASE WHEN oa.order_id_outstanding_amount IS NULL THEN 'Outstanding amount <> 0'
		WHEN oa.order_id_outstanding_amount IS NOT NULL THEN 'Outstanding amount = 0'
		END AS outstanding_amount_payment_order_pii,
	CASE WHEN pv.order_id_pending_value IS NULL THEN 'Pending Value <> 0'
		WHEN pv.order_id_pending_value IS NOT NULL THEN 'Pending Value = 0'
		END AS pending_value,
	CASE WHEN mrh.order_id_outstanding_amount_review_historical IS NULL THEN 'Outstanding amount <> 0'
		WHEN mrh.order_id_outstanding_amount_review_historical IS NOT NULL THEN 'Outstanding amount = 0'
		END AS outstanding_amount_review_historical,
	CASE WHEN ofd.order_id IS NULL THEN 'Not Onfido process'
		WHEN ofd.order_id IS NOT NULL THEN 'Onfido process'
		END AS onfido_process_risk_report
FROM ods_production.ORDER o
LEFT JOIN
	(
	SELECT DISTINCT 
		order_id AS order_id_onfido
	FROM s3_spectrum_rds_dwh_order_approval.id_verification_order
	) oc
	ON oc.order_id_onfido = o.order_id
LEFT JOIN 
	(
	SELECT DISTINCT
		order_id AS order_id_outstanding_amount
	FROM ods_data_sensitive.payment_order_pii
	WHERE outstanding_amount = 0
	) oa
	ON oa.order_id_outstanding_amount = o.order_id
LEFT JOIN 
	(
	SELECT DISTINCT 
		order_id AS order_id_pending_value
	FROM ods_production.order_scoring
	WHERE pending_value = 0
	) pv
	ON pv.order_id_pending_value = o.order_id
LEFT JOIN
	(
	SELECT DISTINCT 
		order_id AS order_id_outstanding_amount_review_historical
	FROM dwh.manual_review_historical
	WHERE outstanding_amount = 0
	) mrh
	ON mrh.order_id_outstanding_amount_review_historical = o.order_id
LEFT JOIN onfido ofd 
	ON ofd.order_id = o.order_id
WHERE o.store_country = 'United States'
	OR o.manual_review_ends_at IS NOT NULL
WITH NO SCHEMA BINDING;

