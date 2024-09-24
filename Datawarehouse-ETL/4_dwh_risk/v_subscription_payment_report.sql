CREATE OR REPLACE VIEW dm_risk.v_subscription_payments_report AS 
WITH order_o AS 
(
SELECT
		o.order_id,
		CASE
			WHEN shippingcountry IN ('Germany', 'Deutschland') 
				THEN 'DE'
			WHEN shippingcountry IN ('The Netherlands', 'Netherlands', 'Netherlands.') 
				THEN 'NL'
			WHEN shippingcountry IN ('Austria', 'Österreich')  
				THEN 'AT'
			WHEN shippingcountry IN ('Spain') 
				THEN 'ES'
			WHEN shippingcountry IN ('United States') 
				THEN 'US'
			WHEN shippingcountry IN ('United Kingdom','UK') 
				THEN 'UK'
			WHEN shippingcountry IN ('France') 
				THEN 'FR'
			WHEN shippingcountry IN ('Belgium') 
				THEN 'BE'
			ELSE shippingcountry
		END AS shipping_country_code
	FROM ods_production.order o
),
order_shipping_info AS
(
	SELECT
		s.subscription_id ,
		COALESCE( a.asset_risk_fraud_rate,'NA') AS asset_risk_fraud_rate,
		o.shipping_country_code
	FROM master.subscription s 
	LEFT JOIN order_o o
		ON s.order_id = o.order_id
	LEFT JOIN stg_fraud_and_credit_risk.asset_risk_categories a
		ON a.sku_variant = s.variant_sku 
		AND a.country_code = o.shipping_country_code
		AND a.newest_risk_category IS TRUE
),
subscription_information AS
(
	SELECT 
		osi.asset_risk_fraud_rate,
		s.first_asset_delivery_date,
		o.initial_scoring_decision,
		o.is_pay_by_invoice,
		o.is_special_voucher,
		o.is_voucher_recurring,
		o.manual_review_ends_at,
		s.new_recurring,
		oo.new_recurring_risk,
		o.order_mode,
		s.payment_count,
		s.payment_method,
		s.product_name,
		s.product_sku,
		osi.shipping_country_code,
		s.store_type,
		s.subscription_id,
		s.trial_days,
		s.trial_variant,
		o.voucher_code,
		oo.device,
		s.customer_id,
		o.order_id,
		s.order_created_date,
		o.paid_date as order_paid_date,
		o.submitted_date AS order_submitted_date,
		s.rental_period,
		s.status,
		CASE WHEN so."number" IS NOT NULL THEN 1 ELSE 0 END AS is_old_infra_order
	FROM master.subscription s 
	LEFT JOIN ods_production."order" o
		ON o.order_id = s.order_id
	LEFT JOIN master."order" oo
		ON oo.order_id = s.order_id	
	LEFT JOIN order_shipping_info osi 
		ON osi.subscription_id = s.subscription_id
	LEFT JOIN stg_api_production.spree_orders so 
		ON so."number" = s.order_id		
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
,former_non_paid_payments AS (
	SELECT 
	p1.payment_id,
	SUM(CASE
		WHEN p2.payment_id IS NOT NULL 
		AND p2.paid_date IS NULL
		THEN 1
		ELSE 0
	END) AS nr_of_non_paid_former_payment
	FROM master.subscription_payment p1
	LEFT JOIN master.subscription_payment p2
		ON p1.subscription_id = p2.subscription_id 
		AND p1.due_date > p2.due_date
	WHERE p1.subscription_payment_category <> 'NYD'
	GROUP BY 1
)
, migrated_data as
(
	SELECT 
		sp.subscription_id,
		sp.payment_id,
		sp.customer_id,
		sp.amount_discount,
		sp.amount_due,
		sp.amount_overdue_fee,
		sp.amount_paid,
		sp.amount_shipment,
		sp.amount_subscription,
		sp.amount_voucher,
		sp.asset_was_returned,
		sp.brand,
		sp.burgel_risk_category,
		sp.customer_type,
		sp.dpd,
		CASE 
			WHEN sp.store_label = 'Grover - United States online' 
				THEN convert_timezone('UTC', 'EST', sp.due_date)
			ELSE sp.due_date 
		END AS due_date,
		CASE
			WHEN sp.store_label = 'Grover - United States online' 
				THEN convert_timezone('UTC', 'EST', sp.paid_date) 
			ELSE sp.paid_date
		END as paid_date,
		sp.country_name,
		sp.payment_method_detailed,
		sp.payment_number,
		coalesce(sp.payment_processor_message,usf.failedreason) as payment_processor_message,
		sp.store_label,
		sp.store_short,
		sp.subcategory_name,
		sp.subscription_payment_category,
		sp.subscription_start_date,
		sp.allocation_id,
		si.asset_risk_fraud_rate,
		si.first_asset_delivery_date,
		si.initial_scoring_decision,
		si.is_pay_by_invoice,
		si.is_special_voucher,
		si.is_voucher_recurring,
		si.manual_review_ends_at,
		si.new_recurring,
		si.new_recurring_risk,
		si.order_mode,
		si.payment_count,
		si.payment_method,
		si.product_name,
		si.product_sku,
		si.shipping_country_code,
		si.is_old_infra_order,
		si.store_type,
		si.device,
		--si.subscription_id, --check if Tableau is using it or not cause already exists one in sp.table
		si.trial_days,
		si.trial_variant,
		si.voucher_code,
		si.order_id,
		si.order_created_date,
		si.order_paid_date,
		si.order_submitted_date,
		si.rental_period,
		si.status AS subscription_status,
		a.initial_price AS asset_purchase_price,
		a.currency AS asset_purchase_price_currency,
		CASE WHEN a.currency = 'USD' 
				THEN (a.initial_price*exchange_rate_eur)::decimal(10,2)
			 ELSE a.initial_price 
		END AS asset_purchase_price_euro,
		CASE
	     	WHEN sp.customer_type = 'normal_customer' THEN 'B2C customer'
	     	WHEN sp.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
	     	WHEN sp.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
		WHEN sp.customer_type = 'business_customer' THEN 'B2B nonfreelancer'
	     	ELSE 'Null'
     	END AS customer_type_freelancer_split,
     	sp.status as subscription_payment_status,
		fnpp.nr_of_non_paid_former_payment
	FROM master.subscription_payment sp
	LEFT JOIN subscription_information si 
		ON sp.subscription_id = si.subscription_id
	LEFT JOIN master.asset a 
		ON sp.asset_id = a.asset_id
	LEFT JOIN master.customer cc 
		ON cc.customer_id = sp.customer_id 
	LEFT JOIN dm_risk.b2b_freelancer_mapping fre
		ON cc.company_type_name = fre.company_type_name 
	left join trans_dev.failed_payment_reasons_us usf
	on usf.payment_id = sp.payment_id 
	LEFT JOIN fx
		ON (CASE WHEN DATE_TRUNC('month',a.purchased_date) <= '2021-04-01' THEN '2021-04-01'
				ELSE DATE_TRUNC('month',a.purchased_date) END) = fx.fact_month
	LEFT JOIN former_non_paid_payments fnpp
		ON fnpp.payment_id = sp.payment_id
	WHERE sp.subscription_start_date > DATE_ADD('month', -24, CURRENT_DATE)
), 
asset_value AS 
(
	SELECT 
		md.order_id,
		SUM(COALESCE(ah.last_market_valuation,0)) AS asset_market_price
	FROM migrated_data md
	LEFT JOIN master.asset_historical ah
		ON md.subscription_id = ah.subscription_id 
		AND ah.date = md.first_asset_delivery_date::date
	GROUP BY 1
),
verification_data AS
(
	SELECT DISTINCT customer_id -- Note that this includes ALL customers who underwent verification (passed OR failed)
	FROM s3_spectrum_rds_dwh_order_approval.decision_tree_algorithm_results
	WHERE additional_reason = 'BAS service'
),
shipcloud AS 
(
	SELECT
		customer_id, 
		shipcloud_profile, 
		ident_check, 
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, updated_at DESC, id DESC) AS rowno
	FROM stg_order_approval.dhl_ident_check_test
),
us_risk_tag_raw AS (
	SELECT
		customer_id,
		created_at::date AS tag_applied_date,
		tag_name,
		row_number() over (partition by customer_id, tag_id order BY created_at desc) as row_n
	FROM s3_spectrum_kafka_topics_raw.risk_customer_tags_apply_v1
	WHERE action_type = 'apply'
	  AND tag_name IN ('us_ml_test_treatment', 'us_ml_test_control', 'precise_id_test_treatment', 'precise_id_test_control')
),
us_risk_tag AS (
	SELECT 
		customer_id,
		CASE 
			WHEN LISTAGG(tag_name::VARCHAR, ', ') IN ('precise_id_test_treatment, us_ml_test_control', 'us_ml_test_control, precise_id_test_treatment')
				THEN 'ml_control_&_precise_treatment'
			WHEN LISTAGG(tag_name::VARCHAR, ', ') IN ('us_ml_test_treatment, precise_id_test_treatment', 'precise_id_test_treatment, us_ml_test_treatment')
				THEN 'ml_treatment_&_precise_treatment'		
			WHEN LISTAGG(tag_name::VARCHAR, ', ') IN ('precise_id_test_control, us_ml_test_treatment', 'us_ml_test_treatment, precise_id_test_control')
				THEN 'ml_treatment_&_precise_control'
			WHEN LISTAGG(tag_name::VARCHAR, ', ') IN ('us_ml_test_control, precise_id_test_control', 'precise_id_test_control, us_ml_test_control')
				THEN 'ml_control_&_precise_control'
			WHEN LISTAGG(tag_name::VARCHAR, ', ') = 'us_ml_test_control'
				THEN 'ml_control'
			WHEN LISTAGG(tag_name::VARCHAR, ', ') = 'us_ml_test_treatment'
				THEN 'ml_treatment'
		END AS us_risk_tag,
		MIN(tag_applied_date) AS tag_applied_date
	FROM us_risk_tag_raw 
	WHERE row_n = 1
	GROUP BY 1
),
recurring_customer_flow AS (
	SELECT 
		customer_id, 
		new_recurring_flow AS is_new_recurring_flow,
		updated_at AS recurring_flow_date,
		LAG(updated_at) OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS next_flow_date
 	FROM stg_order_approval.recurring_customer_new_flow 
),
us_model_decline_tags AS
(
	SELECT 
		customer_id, 
		tag_name, 
		created_at::timestamp, 
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM s3_spectrum_kafka_topics_raw.risk_customer_tags_apply_v1
	WHERE tag_name IN ( 'us_ml_decline_treatment', 'us_ml_decline_control')
),
es_onfido_test_tags AS
(
	SELECT 
		customer_id, 
		tag_name, 
		created_at::timestamp, 
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM s3_spectrum_kafka_topics_raw.risk_customer_tags_apply_v1
	WHERE tag_name IN ( 'onfido_es_control', 'onfido_es_treatment')
),
es_seon_idv_test_tags AS
(
	SELECT 
		customer_id, 
		tag_name, 
		created_at::timestamp, 
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM s3_spectrum_kafka_topics_raw.risk_customer_tags_apply_v1
	WHERE tag_name IN ( 'seon_idv_es_treatment', 'seon_idv_es_control')
)
, risk_tags_technical_raw AS -- TO be applied ON subscriptions started AFTER application OF customer tags.
(
	SELECT
		customer_id, 
		tag_name, 
		comments,
		created_at::timestamp, 
		ROW_NUMBER() OVER (PARTITION BY customer_id, tag_name ORDER BY created_at DESC, consumed_at DESC) AS row_num
	FROM s3_spectrum_kafka_topics_raw.risk_customer_tags_apply_v1
	WHERE tag_type = 'technical'
		AND action_type = 'apply'
)
, all_risk_tags_technical AS
(
	SELECT
		s.customer_id,
		s.subscription_id,
		LISTAGG(tag_name, ', ') AS risk_tags_technical
	FROM master.subscription s 
	LEFT JOIN risk_tags_technical_raw tg 
			ON tg.customer_id = s.customer_id 
			AND tg.created_at < s.start_date
			AND tg.row_num = 1
	GROUP BY 1,2
)
SELECT DISTINCT
	md.*,
	a.shipment_provider,
	cs.accounts_cookie,
	cs.agg_unpaid_products,
	cs.burgel_address_details,
	cs.burgel_person_known,
	--cs.burgel_risk_category_(customer_scoring), --check if Tableau is using, cause already exist in .md table
	cs.burgel_score,
	cs.burgel_score_details,
	cs.burgelcrif_person_known,
	cs.burgelcrif_score_decision,
	cs.burgelcrif_score_value,
	cs.country_grade,
	cs.current_subscription_limit,
	cs.customer_created_at,
	-- cs.customer_id_(customer_scoring), --check if Tableau is using, cause already exist in .md table
	cs.customer_label, 
	cs.customer_label_new, 
	cs.customer_scoring_result, 
	cs.delphi_score, 
	cs.distinct_customers_with_ip, 
	cs.document_number, 
	cs.documents_match, 
	cs.equifax_rating, 
	cs.equifax_score, 
	cs.experian_address_known, 
	cs.experian_person_known, 
	cs.experian_person_known_at_address, 
	cs.experian_score, 
	cs.first_failed_payments, -- _(customer_scoring), --check the correct name at the table
	cs.first_paid_payments,
	cs.focum_rating, 
	cs.focum_score, 
	cs.fraud_type, 
	cs.id_check_result, 
	cs.id_check_state, 
	cs.id_check_url, 
	cs.id_check_type, 
	cs.initial_subscription_limit, 
	cs.is_blacklisted, 
	cs.is_negative_remarks, 
	cs.is_whitelisted, 
	cs.max_fraud_detected, 
	cs.min_fraud_detected, 
	cs.nethone_risk_category, 
	cs.orders_count, 
	cs.paid_amount, 
	cs.payment_errors, 
	cs.paypal_address_confirmed, 
	cs.paypal_verified, 
	cs.phone_carrier, 
	cs.previous_manual_reviews, 
	cs.recurring_failed_payments, 
	cs.recurring_paid_months, 
	cs.recurring_paid_payments, 
	cs.schufa_class, -- _(customer_scoring), -- A) check, if Tableau is using, cause there is the same field on os. table
	cs.schufa_identity_confirmed, 
	cs.schufa_paid_debt, 
	cs.schufa_score, -- _(customer_scoring), --check the correct name at the table
	cs.schufa_total_debt, 
	cs.seon_link, 
	cs.subscription_limit_change_date, 
	cs.subscription_limit_defined_date, 
	cs.subscriptions_count, 
	cs.tag_name, 
	cs.trust_type, 
	cs.updated_at, -- _(customer_scoring), --check the correct name at the table
	cs.verita_person_known_at_address, 
	cs.verita_score, 
	cs.web_users, 
	os.decision_code_label, 
	os.model_name, 
	os.order_scoring_comments, 
	os.schufa_class AS schufa_class_two, -- A) check, if Tableau is using, cause there is the same field on cs. table
	os.scoring_model_id, 
	os.verification_state, 
	p.risk_label,
	CASE 
		WHEN vd.customer_id IS NOT NULL 
			THEN 'BAS Verified' -- Includes all those who underwent BAS verification (passed or failed).
		ELSE 'Not BAS Verified' 
	END AS customer_bas_verified, 
	CASE 
		WHEN LEFT(shipment_provider,3) <> LEFT(shipcloud_profile,3)
		 AND LOWER(shipment_provider) NOT ILIKE '%hermes%'
			THEN 'wrong shipcloud'
		WHEN sc.customer_id IS NOT NULL 
		 AND av.asset_market_price = 0
			THEN 'no market_price - ' + shipcloud_profile 
		WHEN sc.shipcloud_profile = 'DHL Ident Check' 
		 AND av.asset_market_price < 2500
		 AND md.order_created_date >= '2022-02-15 17:30:00'
			THEN '< 2500 - DHL Named person only' --specific request: BI-4803
		WHEN sc.customer_id IS NOT NULL 
		 AND av.asset_market_price >= 2500 
			THEN '>= 2500 - ' + sc.shipcloud_profile
		WHEN sc.customer_id IS NOT NULL
			THEN '< 2500 - ' + sc.shipcloud_profile 
		ELSE 'no shipcloud'
	END AS customer_shipcloud,
	usrt.us_risk_tag,
	rcf.is_new_recurring_flow,
   	tg.tag_name AS us_model_decline_group,
 	tg2.tag_name AS es_onfido_test_group,
 	tg3.tag_name AS es_seon_idv_test_group,    	
 	rtg.risk_tags_technical,
	COALESCE(es_experian.es_experian_score_rating, 'N/A') AS es_experian_score_rating,
	COALESCE(es_equifax.es_equifax_score_rating, 'N/A') AS es_equifax_score_rating,
	COALESCE(nl_experian.nl_experian_score_rating, 'N/A') AS nl_experian_score_rating,
	COALESCE(nl_focum.nl_focum_score_rating, 'N/A') AS nl_focum_score_rating,
	COALESCE(at_crif.at_crif_score_rating, 'N/A') AS at_crif_score_rating,
	COALESCE(us_precise.us_precise_score_rating, 'N/A') AS us_precise_score_rating,
	COALESCE(de_schufa.de_schufa_score_rating, 'N/A') AS de_schufa_score_rating
FROM migrated_data md
LEFT JOIN master.allocation a 
	ON a.allocation_id = md.allocation_id
LEFT JOIN ods_production.customer_scoring cs 
	ON md.customer_id = cs.customer_id
LEFT JOIN ods_production.order_scoring os 
	ON md.order_id = os.order_id
LEFT JOIN ods_production.product p 
	ON md.product_sku = p.product_sku 
LEFT JOIN verification_data vd
	ON md.customer_id = vd.customer_id
LEFT JOIN shipcloud sc
	ON md.customer_id = sc.customer_id 
   AND sc.rowno = 1	
LEFT JOIN recurring_customer_flow rcf
	ON md.customer_id = rcf.customer_id 
   AND md.order_created_date BETWEEN rcf.recurring_flow_date AND COALESCE(rcf.next_flow_date, '9999-12-31') 
LEFT JOIN asset_value av
	ON md.order_id = av.order_id
LEFT JOIN us_risk_tag usrt 
	ON md.customer_id = usrt.customer_id 
   AND md.order_paid_date >= usrt.tag_applied_date 
LEFT JOIN us_model_decline_tags tg 
	ON tg.customer_id = md.customer_id 
	AND tg.created_at < md.subscription_start_date
	AND tg.row_num = 1
LEFT JOIN es_onfido_test_tags tg2 
	ON tg2.customer_id = md.customer_id 
	AND tg2.created_at < md.subscription_start_date
	AND tg2.row_num = 1	
LEFT JOIN es_seon_idv_test_tags tg3
	ON tg3.customer_id = md.customer_id 
	AND tg3.created_at < md.subscription_start_date
	AND tg3.row_num = 1		
LEFT JOIN all_risk_tags_technical rtg
	ON rtg.customer_id = md.customer_id 
	AND rtg.subscription_id = md.subscription_id
LEFT JOIN ods_data_sensitive.external_provider_order_score es_experian
	ON md.order_id = es_experian.order_id
LEFT JOIN ods_data_sensitive.external_provider_order_score es_equifax
	ON md.order_id = es_equifax.order_id
LEFT JOIN ods_data_sensitive.external_provider_order_score nl_experian
	ON md.order_id = nl_experian.order_id
LEFT JOIN ods_data_sensitive.external_provider_order_score nl_focum
	ON md.order_id = nl_focum.order_id
LEFT JOIN ods_data_sensitive.external_provider_order_score at_crif
	ON md.order_id = at_crif.order_id
LEFT JOIN ods_data_sensitive.external_provider_order_score us_precise
	ON md.order_id = us_precise.order_id
LEFT JOIN ods_data_sensitive.external_provider_order_score de_schufa
	ON md.order_id = de_schufa.order_id
WITH NO SCHEMA BINDING;
