-- This View is very similar to the Score Distribution Report (order level) but enhanced with subscription level data. 

CREATE OR REPLACE VIEW dm_risk.v_score_distribution_report_subscriptions AS 
WITH mr_data_eu AS
(
	SELECT DISTINCT order_id
	FROM stg_curated.risk_internal_eu_manual_review_result_v1 mr
	WHERE "result" IN ('APPROVED', 'DECLINED')
),
mr_data_us AS
(
	SELECT DISTINCT order_id
	FROM stg_curated.risk_internal_us_manual_review_result_v1 mr
	WHERE "result" IN ('APPROVED', 'DECLINED')
),
orders_without_subscriptions AS
(
	SELECT DISTINCT 
		oi.order_id,
		oi.order_id || '-' || ROW_NUMBER() OVER (PARTITION BY oi.order_id) AS feux_subscription_id,
		oi.brand,
		oi.category_name,
		oi.subcategory_name
	FROM ods_production.order_item oi
	INNER JOIN master."order" o
		ON o.order_id = oi.order_id
		AND o.submitted_date >= DATEADD('month', -13, current_date)
	WHERE NOT EXISTS ( SELECT NULL FROM master.subscription s WHERE s.order_id = oi.order_id)
)
SELECT
	-- Order level information
	o.order_id,
	o.created_date::date order_created_date,
    o.submitted_date::date order_submitted_date,
    o.status AS order_status,
    o.completed_orders, -- Must inspect logic. Replicating Approval Funnel logic.
    oj.order_journey_mapping_risk,
    CASE 
	    WHEN o.approved_date IS NOT NULL THEN 1 
	    ELSE 0
	 END order_is_approved ,
	CASE
		WHEN o.canceled_date IS NOT NULL THEN 1 
		ELSE 0 
	END order_is_cancelled ,
	CASE 
		WHEN o.paid_date IS NOT NULL THEN 1 
		ELSE 0 
	END order_is_paid ,
	CASE
     	WHEN mreu.order_id IS NOT NULL 
     		OR mrus.order_id IS NOT NULL 
     			THEN 1
     	ELSE 0
     END AS order_went_to_mr,
	o.store_country,
	o.new_recurring,
	o.new_recurring_risk,
	o.marketing_channel,
	o.customer_type,
	-- Subscription level information
	COALESCE(s.subscription_id, ows.feux_subscription_id) AS subscription_id_enhanced,
	COALESCE(s.brand, ows.brand) AS product_brand, -- IF order declined, we know the Brand/Cat./Subcat. OF the potential subscription.
	COALESCE(s.category_name, ows.category_name) AS product_category,
	COALESCE(s.subcategory_name, ows.subcategory_name) AS product_subcategory, 
	COALESCE(s.status, 'NEVER ACTIVE') AS subscription_status,
	-- Order Score Values
	sd.es_experian_score_rating,
	sd.es_experian_score_value,
	sd.es_equifax_score_rating,
	sd.es_equifax_score_value,
	sd.de_schufa_score_rating,
	sd.de_schufa_score_value,
	sd.at_crif_score_rating,
	sd.at_crif_score_value,
	sd.nl_focum_score_rating,
	sd.nl_focum_score_value,
	sd.nl_experian_score_rating,
	sd.nl_experian_score_value,
	sd.us_precise_score_rating,
	sd.us_precise_score_value,
	sd.us_fico_score_rating,
	sd.us_fico_score_value,
	sd.global_seon_fraud_score,
	sd.eu_ekata_identity_risk_score,
	sd.eu_ekata_identity_network_score,
	sd.us_clarity_score_value,
	sd.us_clarity_score_rating,
	sd.us_vantage_score_value,
	sd.us_vantage_score_rating,
	os.score AS model_score,
	os.model_name 
FROM master."order" o
LEFT JOIN mr_data_eu mreu
	ON o.order_id = mreu.order_id
LEFT JOIN mr_data_us mrus
	ON o.order_id = mrus.order_id
LEFT JOIN ods_data_sensitive.external_provider_order_score sd
    ON o.customer_id = sd.customer_id
   AND o.order_id = sd.order_id
LEFT JOIN ods_production.order_journey oj
	ON o.order_id = oj.order_id 
LEFT JOIN master.subscription s
	ON s.order_id = o.order_id
LEFT JOIN orders_without_subscriptions ows
	ON ows.order_id = o.order_id
LEFT JOIN ods_production.order_scoring os 
	ON os.order_id = o.order_id
WHERE o.store_country IN ('Spain', 'Germany', 'Austria', 'Netherlands', 'United States')
	AND o.completed_orders  = 1 
  AND o.submitted_date >= DATEADD('month', -13, current_date)
WITH NO SCHEMA BINDING;