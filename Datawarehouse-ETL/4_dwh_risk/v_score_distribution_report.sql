CREATE OR REPLACE VIEW dm_risk.v_score_distribution_report AS 
WITH customer_labels AS
(
SELECT customer_id,
	label AS customer_label_risk,
	ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, updated_at DESC) AS row_num
FROM s3_spectrum_rds_dwh_order_approval.customer_labels3
)
SELECT
	DATE_TRUNC('week', o.created_date)::date as year_week,
	DATE_TRUNC('week', o.submitted_date)::date as year_week_sub,
    o.created_date::date created_date,
    o.submitted_date::date submitted_date,
    o.store_country,
	o.new_recurring,
	o.new_recurring_risk,
	o.customer_type,
	o.customer_id,
	o.order_id,
	o.total_orders,
	o.order_rank,
	o.completed_orders, 
    oj.order_journey_mapping_risk,
    cl.customer_label_risk,
	CASE WHEN o.approved_date IS NOT NULL THEN 1 ELSE 0 END is_approved ,
	CASE WHEN o.canceled_date IS NOT NULL THEN 1 ELSE 0 END is_cancelled ,
	CASE WHEN o.paid_date IS NOT NULL THEN 1 ELSE 0 END is_paid ,
	o.scoring_decision ,
	o.declined_reason ,
	o.status,
	o.voucher_type ,
	o.marketing_channel ,
	o.marketing_campaign,
	COALESCE(oc.marketing_source, 'n/a') AS marketing_source,
	o.order_value ,
	-- Credit and Fraud Scores
	sd.es_experian_score_rating,
	sd.es_experian_score_value,
	sd.es_experian_avg_pd,
	sd.es_equifax_score_rating,
	sd.es_equifax_score_value,
	sd.es_equifax_avg_pd,
	sd.de_schufa_score_rating,
	sd.de_schufa_score_value,
	sd.de_schufa_avg_pd, 
	sd.at_crif_score_rating,
	sd.at_crif_score_value,
	sd.at_crif_avg_pd, 
	sd.nl_focum_score_rating,
	sd.nl_focum_score_value,
	sd.nl_focum_avg_pd, 
	sd.nl_experian_score_rating,
	sd.nl_experian_score_value,
	sd.nl_experian_avg_pd,
	sd.us_precise_score_rating,
	sd.us_precise_score_value,
	sd.us_precise_avg_pd,
	sd.us_fico_score_rating,
	sd.us_fico_score_value,
	sd.us_clarity_score_value,
	sd.us_clarity_score_rating,
	sd.us_vantage_score_value,
	sd.us_vantage_score_rating,
	-- Model Scores
	os.score AS model_score,
	os.model_name,
	os.approve_cutoff,
	os.decline_cutoff
FROM master."order" o 
LEFT JOIN ods_data_sensitive.external_provider_order_score sd
    ON o.customer_id = sd.customer_id
   AND o.order_id = sd.order_id
LEFT JOIN ods_production.order_scoring os 
	ON os.order_id = o.order_id
LEFT JOIN ods_production.order_journey oj
	ON o.order_id = oj.order_id 
LEFT JOIN customer_labels cl
	ON o.customer_id = cl.customer_id
	AND cl.row_num = 1
LEFT JOIN ods_production.order_marketing_channel oc 
	ON o.order_id = oc.order_id
WHERE o.store_country IN ('Spain', 'Germany', 'Austria', 'Netherlands', 'United States')
  AND o.submitted_date >= DATEADD('month', -12, current_date)
WITH NO SCHEMA BINDING

;