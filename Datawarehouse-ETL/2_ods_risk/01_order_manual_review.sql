DROP TABLE IF EXISTS ods_data_sensitive.order_manual_review_tmp;

CREATE TABLE ods_data_sensitive.order_manual_review_tmp
SORTKEY (
	customer_id,
	order_id,
	created_date
)
DISTSTYLE ALL
AS
SELECT
	TRUE AS dag_enriched,
	o.order_id,
	o.customer_id,
	cp.customer_type,
	ip_address_order,
	omr.last_month_submitted_orders,
	o.total_orders,
	o.order_rank,
	omr.market_price,
	o.order_item_count,
	o.ordered_products AS product_names,
	sd.active_subscription_product_names AS active_subscriptions,
	sd.active_subscription_category AS active_subscriptions_categories,
	CASE
		WHEN sd.active_subscription_category LIKE '%' || SPLIT_PART(o.ordered_product_category, ',', 1)|| '%' THEN 1
		ELSE 0
	END AS held_category,
	o.ordered_quantities AS order_item_quantity,
	o.ordered_plan_durations AS order_item_plan_duration,
	o.ordered_risk_labels,
	cs.nethone_risk_category,
	o.basket_size AS order_item_prices,
	o.status,
	o.voucher_code,
	o.created_date,
	o.submitted_date,
	manual_review_ends_at,
	o.approved_date,
	o.store_type,
	o.store_country,
	o.declined_reason,
	o.initial_scoring_decision,
	odr.decline_reason_new,
	cu.created_at AS customer_created_at,
	sd.start_date_of_first_subscription AS first_subscription_date,
	org.new_recurring,
	org.retention_group,
	o.billingcity AS billing_city,
	o.billingpostalcode AS billing_postal_code,
	o.billingcountry AS billing_country,
	o.shippingstreet AS shipping_street,
	o.shippingadditionalinfo AS shipping_additional_info,
	o.shippingcity AS shipping_city,
	o.shippingpostalcode AS shipping_postal_code,
	cp.first_name,
	cp.last_name,
	cp.phone_number,
	cs.phone_carrier,
	cp.birthdate,
	cp.age,
	cp.email AS customer_email,
	cp.paypal_email AS customer_paypal_email,
	cs.paypal_verified,
	cs.paypal_address_confirmed,
	cs.customer_scoring_result,
	cs.customer_label_new AS customer_label,
	CASE
		WHEN omr.is_fraud_type = 1 THEN cs.fraud_type
		ELSE NULL
	END AS fraud_type,
	cs.burgel_person_known,
	cs.burgel_score,
	cs.verita_person_known_at_address,
	round(cs.verita_score,0)::int as verita_score,
	cs.schufa_class,
	cs.schufa_identity_confirmed,
	round(cs.schufa_paid_debt,0)::int as schufa_paid_debt ,
	round(cs.schufa_total_debt,0)::int as schufa_total_debt,
	cs.burgelcrif_score_value,
	cs.burgelcrif_score_decision,
	cs.burgelcrif_person_known,
	cs.experian_score,
	cs.experian_person_known,
	cs.delphi_score,
	cs.equifax_score,
	cs.equifax_rating,
	cs.agg_unpaid_products,
	cs.focum_score,
	cs.focum_rating,
	sc.score,
	sc.order_scoring_comments,
	cp.subscription_limit,
	cs.initial_subscription_limit,
	sd.active_subscription_value,
	sd.subscriptions,
	sd.failed_subscriptions AS failed_recurring_payments,
	sd.subscription_revenue_paid,
	sd.subscription_revenue_due - sd.subscription_revenue_paid AS outstanding_amount,
	omr.outstanding_asset_value,
	sd.subscription_revenue_chargeback,
	sd.subscription_revenue_refunded,
	sc.payment_category_fraud_rate,
	sc.payment_category_info,
	sc.cart_risk_fraud_rate,
	sc.cart_risk_info,
	op.payment_method,
	op.payment_method_type,
	op.sepa_mandate_id,
	op.sepa_mandate_date,
	op.paypal_email,
	op.card_type,
	op.card_issuing_country,
	op.card_issuing_bank,
	op.funding_source,
	op.cardholder_name,
	op.shopper_email,
	op.gateway_type,
	omr.is_duplicate_ip,
	omr.is_gmx_email,
	omr.mismatching_name,
	omr.mismatching_city,
	omr.is_foreign_card,
	omr.is_prepaid_card,
	omr.is_fraud_type,
	omr.has_failed_first_payments,
	omr.rules_score,
	omr.levenshtein_distance AS levenshtein_distance_namecheck_emailcheck,
	pod.related_order_ids,
	omr.is_negative_remarks,
	omr.manual_review_orders,
	sc.anomaly_score,
	sc.approve_cutoff,
	sc.decline_cutoff,
	sc.creation_timestamp,
	sc.file_path,
	sc.model_name,
	omc.geo_cities AS ip_location,
	upi.related_customer_info,
	cs.accounts_cookie,
	cs.web_users,
	sc.similar_customers_count,
	sc.top_similar_scores,
	cs.previous_manual_reviews,
	cp.schufa_address_string,
	omr.verified_zipcode,
	cs.id_check_state,
	cs.id_check_type,
	cs.id_check_url,
	cs.id_check_result,
	cs.documents_match,
	cs.document_number,
	sc.first_month_free_voucher,
	cp.paypal_name,
	sc.onfido_trigger,
	c.company_name,
	c.company_type_name,
	CAST(c.company_type_id AS VARCHAR(5)) AS company_type_id,
	c.lead_type,
	omr.is_in_dca,
	cs.seon_link,
	omr.product_details,
	omr.subs_details,
	omr.previous_orders_details,
	cs.tag_name,
	cs.payment_errors,
	CAST(NULL AS INTEGER) AS fico,
	CAST(NULL AS INTEGER) AS precise_id_score,
	CAST(NULL AS INTEGER) AS fpdscore,
	CAST(NULL AS NUMERIC(18, 8)) AS identity_network_score,
	CAST(NULL AS INTEGER) AS identity_risk_score,
	CAST('' AS VARCHAR(10)) AS address_first_seen_days,
	CAST('' AS VARCHAR(20)) AS address_to_name,
	CAST('' AS VARCHAR(50)) AS address_validity_level,
	CAST('' AS VARCHAR(10)) AS email_first_seen_days,
	CAST('' AS VARCHAR(50)) AS email_to_name,
	CAST('' AS VARCHAR(50)) AS phone_line_type,
	CAST('' AS VARCHAR(50)) AS phone_to_name,
	CAST(NULL AS INTEGER) AS nb_addr_matching_customer_id,
	CAST('' AS VARCHAR(65535)) AS users_matching_addrr,
	CAST('' AS VARCHAR(300)) AS signals,
	CAST('' AS VARCHAR(65535)) AS credit_card_match,
	CAST(NULL AS INTEGER) AS num_diff_cc,
	CAST(NULL AS INTEGER) AS num_diff_pp,
	CAST(NULL AS INTEGER) AS declined_orders,
	CAST(NULL AS INTEGER) AS approved_orders,
	CAST(NULL AS INTEGER) AS cancelled_orders,
	CAST(NULL AS NUMERIC(10, 2)) AS ffp
FROM
	ods_production."order" AS o
LEFT JOIN ods_production.order_retention_group AS org ON
	o.order_id = org.order_id
LEFT JOIN ods_data_sensitive.customer_pii AS cp ON
	o.customer_id = cp.customer_id
LEFT JOIN ods_production.customer_scoring AS cs ON
	o.customer_id = cs.customer_id
LEFT JOIN ods_production.order_scoring AS sc ON
	o.order_id = sc.order_id
LEFT JOIN ods_production.customer_subscription_details AS sd ON
	o.customer_id = sd.customer_id
LEFT JOIN ods_data_sensitive.order_payment_method AS op ON
	o.order_id = op.order_id
LEFT JOIN ods_production.order_manual_review_rules AS omr ON
	o.order_id = omr.order_id
LEFT JOIN ods_production.order_decline_reason AS odr ON
	o.order_id = odr.order_id
LEFT JOIN ods_production.order_manual_review_previous_order_history AS pod ON
	o.order_id = pod.main_order_id
LEFT JOIN ods_production.customer AS cu ON
	o.customer_id = cu.customer_id
LEFT JOIN ods_production.order_marketing_channel AS omc ON
	o.order_id = omc.order_id
LEFT JOIN ods_production.customer_unique_phone_info AS upi ON
	o.customer_id = upi.main_customer_id
LEFT JOIN ods_production.companies AS c ON
	o.customer_id = c.customer_id
WHERE
		o.status = 'MANUAL REVIEW'
	OR (
				LEFT(o.order_id, 1) IN (
					'F', 'M', 'S'
		)
			AND o.status = 'PENDING APPROVAL'
			AND sc.order_scoring_comments IS NOT NULL
	)
	AND o.created_date >= CURRENT_DATE - INTERVAL '30 day'
;

/*Creating a staging enriched table*/
DROP TABLE IF EXISTS enriched_order_manual_review_us;

CREATE TEMP TABLE enriched_order_manual_review_us
AS
SELECT
	CASE
		WHEN omru.order_id IS NULL THEN FALSE
		ELSE TRUE
	END AS dag_enriched,
	omr.order_id,
	omr.customer_id,
	omr.customer_type,
	COALESCE(LEFT(omru.ip_address, 40), omr.ip_address_order) AS ip_address_order,
	omr.last_month_submitted_orders,
	omr.total_orders,
	omr.order_rank,
	omr.market_price,
	COALESCE(omru.num_products, omr.order_item_count) AS order_item_count,
	omr.product_names,
	omr.active_subscriptions,
	omr.active_subscriptions_categories,
	omr.held_category,
	omr.order_item_quantity,
	omr.order_item_plan_duration,
	omr.ordered_risk_labels,
	omr.nethone_risk_category,
	omr.order_item_prices,
	omr.status,
	COALESCE(omru.discount_code, omr.voucher_code) AS voucher_code,
	omr.created_date,
	omr.submitted_date,
	omr.manual_review_ends_at,
	omr.approved_date,
	omr.store_type,
	omr.store_country,
	omr.declined_reason,
	omr.initial_scoring_decision,
	omr.decline_reason_new,
	omr.customer_created_at,
	COALESCE(omru.start_date_of_first_subscription, omr.first_subscription_date) AS first_subscription_date,
	omr.new_recurring,
	omr.retention_group,
	omr.billing_city,
	omr.billing_postal_code,
	omr.billing_country,
	omr.shipping_street,
	omr.shipping_additional_info,
	omr.shipping_city,
	omr.shipping_postal_code,
	omr.first_name,
	omr.last_name,
	omr.phone_number,
	omr.phone_carrier,
	omr.birthdate,
	--omru.birth_date,
	omr.age,
	omr.customer_email,
	omr.customer_paypal_email,
	--omr.paypal_verified AS paypal_verified,
	(
		CASE
			WHEN omr.paypal_verified THEN TRUE
			WHEN omr.paypal_verified IS NULL THEN NULL
			ELSE FALSE
		END
	)::BOOLEAN AS paypal_verified,
	--CAST(omr.paypal_verified AS BOOL) AS paypal_verified,
	omr.paypal_address_confirmed,
	omr.customer_scoring_result,
	omr.customer_label,
	omr.fraud_type,
	omr.burgel_person_known,
	omr.burgel_score,
	omr.verita_person_known_at_address,
	omr.verita_score,
	omr.schufa_class,
	omr.schufa_identity_confirmed,
	omr.schufa_paid_debt,
	omr.schufa_total_debt,
	omr.burgelcrif_score_value,
	omr.burgelcrif_score_decision,
	omr.burgelcrif_person_known,
	omr.experian_score,
	omr.experian_person_known,
	omr.delphi_score,
	omr.equifax_score,
	omr.equifax_rating,
	omr.agg_unpaid_products,
	omr.focum_score,
	omr.focum_rating,
	omr.score,
	COALESCE(LEFT(omru.reason, 16383), omr.order_scoring_comments) AS order_scoring_comments,
	COALESCE(CAST(nullif(omru."limit",'') AS INTEGER), omr.subscription_limit) AS subscription_limit,
	omr.initial_subscription_limit,
	omr.active_subscription_value,
	omr.subscriptions,
	omr.failed_recurring_payments,
	omr.subscription_revenue_paid,
	COALESCE(omru.outstanding_payment, omr.outstanding_amount) AS outstanding_amount,
	omr.outstanding_asset_value,
	omr.subscription_revenue_chargeback,
	omr.subscription_revenue_refunded,
	omr.payment_category_fraud_rate,
	omr.payment_category_info,
	omr.cart_risk_fraud_rate,
	omr.cart_risk_info,
	omr.payment_method,
	omr.payment_method_type,
	omr.sepa_mandate_id,
	omr.sepa_mandate_date,
	omr.psp_reference,
	omr.paypal_email,
	omr.card_type,
	omr.card_issuing_country,
	COALESCE(omru.credit_card_bank_name, omr.card_issuing_bank) AS card_issuing_bank,
	omr.funding_source,
	omr.cardholder_name,
	omr.shopper_email,
	omr.gateway_type,
	omr.is_duplicate_ip,
	omr.is_gmx_email,
	omr.mismatching_name,
	omr.mismatching_city,
	omr.is_foreign_card,
	omr.is_prepaid_card,
	omr.is_fraud_type,
	omr.has_failed_first_payments,
	omr.rules_score,
	omr.levenshtein_distance_namecheck_emailcheck,
	omr.related_order_ids,
	omr.is_negative_remarks,
	omr.manual_review_orders,
	COALESCE(omru.anomaly_score, omr.anomaly_score) AS anomaly_score,
	omr.approve_cutoff,
	omr.decline_cutoff,
	omr.creation_timestamp,
	omr.file_path,
	omr.model_name,
	omr.link_customer,
	omr.ip_location,
	omr.related_customer_info,
	omr.accounts_cookie,
	omr.web_users,
	omr.similar_customers_count,
	omr.top_similar_scores,
	omr.snf_link,
	omr.previous_manual_reviews,
	omr.schufa_address_string,
	omr.verified_zipcode,
	omr.id_check_state,
	omr.id_check_type,
	omr.id_check_url,
	omr.id_check_result,
	omr.documents_match,
	omr.document_number,
	omr.first_month_free_voucher,
	omr.paypal_name,
	omr.onfido_trigger,
	omr.company_name,
	omr.company_type_name,
	omr.company_type_id,
	omr.lead_type,
	omr.is_in_dca,
	omr.seon_link,
	omr.product_details,
	omr.subs_details,
	omr.previous_orders_details,
	omr.tag_name,
	omr.payment_errors,
	omru.fico,
	omru.precise_id_score,
	omru.fpdscore,
	omru.identity_network_score,
	omru.identity_risk_score,
	omru.address_first_seen_days,
	omru.address_to_name,
	omru.address_validity_level,
	omru.email_first_seen_days,
	omru.email_to_name,
	omru.phone_line_type,
	omru.phone_to_name,
	omru.nb_addr_matching_customer_id,
	omru.users_matching_addrr,
	omru.signals,
	omru.credit_card_match,
	omru.num_diff_cc,
	omru.num_diff_pp,
	omru.declined_orders,
	omru.approved_orders,
	omru.cancelled_orders,
	omru.ffp
FROM
	ods_data_sensitive.order_manual_review_tmp AS omr
LEFT JOIN ods_data_sensitive.order_manual_review_us AS omru ON
	omr.order_id = omru.order_id
WHERE
	omr.store_country = 'United States'
ORDER BY
	omr.created_date DESC
;

/*Deleting from prod the orders in the staging table*/
DELETE
FROM
	ods_data_sensitive.order_manual_review_tmp
WHERE
	store_country = 'United States'
	AND order_id IN (
		SELECT
			DISTINCT order_id
		FROM
			enriched_order_manual_review_us
	)
;

/*Inserting the staging table orders in prod*/
INSERT
	INTO
		ods_data_sensitive.order_manual_review_tmp
    (
			dag_enriched,
			order_id,
			customer_id,
			customer_type,
			ip_address_order,
			last_month_submitted_orders,
			total_orders,
			order_rank,
			market_price,
			order_item_count,
			product_names,
			active_subscriptions,
			active_subscriptions_categories,
			held_category,
			order_item_quantity,
			order_item_plan_duration,
			ordered_risk_labels,
			nethone_risk_category,
			order_item_prices,
			status,
			voucher_code,
			created_date,
			submitted_date,
			manual_review_ends_at,
			approved_date,
			store_type,
			store_country,
			declined_reason,
			initial_scoring_decision,
			decline_reason_new,
			customer_created_at,
			first_subscription_date,
			new_recurring,
			retention_group,
			billing_city,
			billing_postal_code,
			billing_country,
			shipping_street,
			shipping_additional_info,
			shipping_city,
			shipping_postal_code,
			first_name,
			last_name,
			phone_number,
			phone_carrier,
			birthdate,
			age,
			customer_email,
			customer_paypal_email,
			paypal_verified,
			paypal_address_confirmed,
			customer_scoring_result,
			customer_label,
			fraud_type,
			burgel_person_known,
			burgel_score,
			verita_person_known_at_address,
			verita_score,
			schufa_class,
			schufa_identity_confirmed,
			schufa_paid_debt,
			schufa_total_debt,
			burgelcrif_score_value,
			burgelcrif_score_decision,
			burgelcrif_person_known,
			experian_score,
			experian_person_known,
			delphi_score,
			equifax_score,
			equifax_rating,
			agg_unpaid_products,
			focum_score,
			focum_rating,
			score,
			order_scoring_comments,
			subscription_limit,
			initial_subscription_limit,
			active_subscription_value,
			subscriptions,
			failed_recurring_payments,
			subscription_revenue_paid,
			outstanding_amount,
			outstanding_asset_value,
			subscription_revenue_chargeback,
			subscription_revenue_refunded,
			payment_category_fraud_rate,
			payment_category_info,
			cart_risk_fraud_rate,
			cart_risk_info,
			payment_method,
			payment_method_type,
			sepa_mandate_id,
			sepa_mandate_date,
			psp_reference,
			paypal_email,
			card_type,
			card_issuing_country,
			card_issuing_bank,
			funding_source,
			cardholder_name,
			shopper_email,
			gateway_type,
			is_duplicate_ip,
			is_gmx_email,
			mismatching_name,
			mismatching_city,
			is_foreign_card,
			is_prepaid_card,
			is_fraud_type,
			has_failed_first_payments,
			rules_score,
			levenshtein_distance_namecheck_emailcheck,
			related_order_ids,
			is_negative_remarks,
			manual_review_orders,
			anomaly_score,
			approve_cutoff,
			decline_cutoff,
			creation_timestamp,
			file_path,
			model_name,
			link_customer,
			ip_location,
			related_customer_info,
			accounts_cookie,
			web_users,
			similar_customers_count,
			top_similar_scores,
			snf_link,
			previous_manual_reviews,
			schufa_address_string,
			verified_zipcode,
			id_check_state,
			id_check_type,
			id_check_url,
			id_check_result,
			documents_match,
			document_number,
			first_month_free_voucher,
			paypal_name,
			onfido_trigger,
			company_name,
			company_type_name,
			company_type_id,
			lead_type,
			is_in_dca,
			seon_link,
			product_details,
			subs_details,
			previous_orders_details,
			tag_name,
			payment_errors,
			fico,
			precise_id_score,
			fpdscore,
			identity_network_score,
			identity_risk_score,
			address_first_seen_days,
			address_to_name,
			address_validity_level,
			email_first_seen_days,
			email_to_name,
			phone_line_type,
			phone_to_name,
			nb_addr_matching_customer_id,
			users_matching_addrr,
			signals,
			credit_card_match,
			num_diff_cc,
			num_diff_pp,
			declined_orders,
			approved_orders,
			cancelled_orders,
			ffp
	)
		SELECT
			dag_enriched,
			order_id,
			customer_id,
			customer_type,
			ip_address_order,
			last_month_submitted_orders,
			total_orders,
			order_rank,
			market_price,
			order_item_count,
			product_names,
			active_subscriptions,
			active_subscriptions_categories,
			held_category,
			order_item_quantity,
			order_item_plan_duration,
			ordered_risk_labels,
			nethone_risk_category,
			order_item_prices,
			status,
			voucher_code,
			created_date,
			submitted_date,
			manual_review_ends_at,
			approved_date,
			store_type,
			store_country,
			declined_reason,
			initial_scoring_decision,
			decline_reason_new,
			customer_created_at,
			first_subscription_date,
			new_recurring,
			retention_group,
			billing_city,
			billing_postal_code,
			billing_country,
			shipping_street,
			shipping_additional_info,
			shipping_city,
			shipping_postal_code,
			first_name,
			last_name,
			phone_number,
			phone_carrier,
			birthdate,
			age,
			customer_email,
			customer_paypal_email,
			paypal_verified,
			paypal_address_confirmed,
			customer_scoring_result,
			customer_label,
			fraud_type,
			burgel_person_known,
			burgel_score,
			verita_person_known_at_address,
			verita_score,
			schufa_class,
			schufa_identity_confirmed,
			schufa_paid_debt,
			schufa_total_debt,
			burgelcrif_score_value,
			burgelcrif_score_decision,
			burgelcrif_person_known,
			experian_score,
			experian_person_known,
			delphi_score,
			equifax_score,
			equifax_rating,
			agg_unpaid_products,
			focum_score,
			focum_rating,
			score,
			order_scoring_comments,
			subscription_limit,
			initial_subscription_limit,
			active_subscription_value,
			subscriptions,
			failed_recurring_payments,
			subscription_revenue_paid,
			outstanding_amount,
			outstanding_asset_value,
			subscription_revenue_chargeback,
			subscription_revenue_refunded,
			payment_category_fraud_rate,
			payment_category_info,
			cart_risk_fraud_rate,
			cart_risk_info,
			payment_method,
			payment_method_type,
			sepa_mandate_id,
			sepa_mandate_date,
			psp_reference,
			paypal_email,
			card_type,
			card_issuing_country,
			card_issuing_bank,
			funding_source,
			cardholder_name,
			shopper_email,
			gateway_type,
			is_duplicate_ip,
			is_gmx_email,
			mismatching_name,
			mismatching_city,
			is_foreign_card,
			is_prepaid_card,
			is_fraud_type,
			has_failed_first_payments,
			rules_score,
			levenshtein_distance_namecheck_emailcheck,
			related_order_ids,
			is_negative_remarks,
			manual_review_orders,
			anomaly_score,
			approve_cutoff,
			decline_cutoff,
			creation_timestamp,
			file_path,
			model_name,
			link_customer,
			ip_location,
			related_customer_info,
			accounts_cookie,
			web_users,
			similar_customers_count,
			top_similar_scores,
			snf_link,
			previous_manual_reviews,
			schufa_address_string,
			verified_zipcode,
			id_check_state,
			id_check_type,
			id_check_url,
			id_check_result,
			documents_match,
			document_number,
			first_month_free_voucher,
			paypal_name,
			onfido_trigger,
			company_name,
			company_type_name,
			company_type_id,
			lead_type,
			is_in_dca,
			seon_link,
			product_details,
			subs_details,
			previous_orders_details,
			tag_name,
			payment_errors,
			fico,
			precise_id_score,
			fpdscore,
			identity_network_score,
			identity_risk_score,
			address_first_seen_days,
			address_to_name,
			address_validity_level,
			email_first_seen_days,
			email_to_name,
			phone_line_type,
			phone_to_name,
			nb_addr_matching_customer_id,
			users_matching_addrr,
			signals,
			credit_card_match,
			num_diff_cc,
			num_diff_pp,
			declined_orders,
			approved_orders,
			cancelled_orders,
			ffp
FROM
			enriched_order_manual_review_us
;

DROP TABLE IF EXISTS tmp_auto_approve_flag;

CREATE TEMP TABLE tmp_auto_approve_flag AS
WITH customers AS (
SELECT DISTINCT o.customer_id
FROM ods_production."order" AS o
WHERE o.submitted_date >= CURRENT_DATE - INTERVAL '31 day'
)
,cust_payment AS (
SELECT
	sp.customer_id AS customer_id,
	sum(CASE WHEN status = 'PLANNED' THEN 0 ELSE amount_due END) AS total_amt_due,
	sum(amount_paid) AS total_amt_paid,
	sum(CASE WHEN status = 'PLANNED' THEN 0 ELSE amount_due END) -sum(amount_paid) AS outstanding_amount,
	max(CASE WHEN status IN ('FAILED', 'FAILED FULLY') THEN dpd ELSE 0 END) AS max_dpd_failed,
	max(CASE WHEN status NOT IN ('PLANNED') THEN dpd ELSE 0 END) AS max_dpd_ever
FROM
	master.subscription_payment sp
	inner JOIN customers c2
		ON sp.customer_id =c2.customer_id
WHERE
	status NOT IN ('PLANNED', 'HELD', 'PENDING', 'NEW')
GROUP BY
	1 )
SELECT
	order_id,
	   CASE
		WHEN dangerous_outstanding_flag = 0
		AND total_amt_paid >= 250
		AND max_dpd_failed = 0
		AND max_dpd_ever <= 30
		AND outstanding_amount <= 0
		AND label IN ('good', 'recovered good')
		AND ratio <= 1 THEN 'auto-approve'
		ELSE 'other'
	END AS auto_approve_flag
FROM
	(
	SELECT
		o.order_id ,
		o.created_date AS order_created_at,
		o.created_date ::date created_day,
		o.status AS order_status ,
		os.order_scoring_comments ,
		CASE
			WHEN lower(os.order_scoring_comments) LIKE '%matches with outstanding_amount%'
				OR lower(os.order_scoring_comments) LIKE '%outstanding amount%' THEN 1
				ELSE 0
			END AS dangerous_outstanding_flag,
			cp.total_amt_paid,
			cp.total_amt_due,
			cp.outstanding_amount,
			cp.max_dpd_failed,
			cp.max_dpd_ever,
			c.subscription_limit ,
			c.active_subscription_value ,
			os.pending_value ,
			---this is order_value + asv
	   CASE
				WHEN c.subscription_limit = 0 THEN 9999
				ELSE os.pending_value::float / c.subscription_limit::float
			END AS ratio,
			cl3.label
		FROM ods_production."order" o
		LEFT JOIN ods_production.order_scoring os
			ON o.order_id = os.order_id
		LEFT JOIN master.customer c ON
			c.customer_id = os.user_id
		LEFT JOIN cust_payment cp ON
			cp.customer_id = os.user_id
		LEFT JOIN s3_spectrum_rds_dwh_order_approval.customer_labels3 cl3 ON
			cl3.customer_id = os.user_id
		WHERE o.submitted_date >= CURRENT_DATE - INTERVAL '31 day') AS t1;

-- code to add the attributes from risk_eu_order_decision table

drop table if exists tmp_risk_eu_order_decision;
create temp table tmp_risk_eu_order_decision as
with dedup as (
select row_number()over(partition by order_id order by updated_at desc)rr,
deyde_identity_degree_of_resemblance as degree_of_resemblance,
deyde_identity_registered as registered,
order_id,
case when deyde_identity_degree_of_resemblance <= '6' or deyde_identity_degree_of_resemblance <= '6' then '1- High Similarity'
	 when deyde_identity_degree_of_resemblance <='16' or deyde_identity_degree_of_resemblance <= '16' then '2- Low Similarity'
	 when deyde_identity_degree_of_resemblance = '17' then '3- Unidentified'
	 when deyde_identity_degree_of_resemblance = '18' then '4- Identified-Revoked'
	 when deyde_identity_degree_of_resemblance = '19' then '5- Identified-Low'
	 when deyde_identity_degree_of_resemblance = '99' then '6 - Unregistered, no reliability value'
else '99- No Info' end as similarity_category
from stg_curated.risk_eu_order_decision_final_v1
)
select order_id, degree_of_resemblance, registered,similarity_category from dedup where rr=1;



BEGIN;
/*Deleting all records in production table*/
DELETE FROM ods_data_sensitive.order_manual_review;

/*Inserting the staging table in prod table*/
INSERT
	INTO
		ods_data_sensitive.order_manual_review
    (
			dag_enriched,
			order_id,
			customer_id,
			customer_type,
			ip_address_order,
			last_month_submitted_orders,
			total_orders,
			order_rank,
			market_price,
			order_item_count,
			product_names,
			active_subscriptions,
			active_subscriptions_categories,
			held_category,
			order_item_quantity,
			order_item_plan_duration,
			ordered_risk_labels,
			nethone_risk_category,
			order_item_prices,
			status,
			voucher_code,
			created_date,
			submitted_date,
			manual_review_ends_at,
			approved_date,
			store_type,
			store_country,
			declined_reason,
			initial_scoring_decision,
			decline_reason_new,
			customer_created_at,
			first_subscription_date,
			new_recurring,
			retention_group,
			billing_city,
			billing_postal_code,
			billing_country,
			shipping_street,
			shipping_additional_info,
			shipping_city,
			shipping_postal_code,
			first_name,
			last_name,
			phone_number,
			phone_carrier,
			birthdate,
			age,
			customer_email,
			customer_paypal_email,
			paypal_verified,
			paypal_address_confirmed,
			customer_scoring_result,
			customer_label,
			fraud_type,
			burgel_person_known,
			burgel_score,
			verita_person_known_at_address,
			verita_score,
			schufa_class,
			schufa_identity_confirmed,
			schufa_paid_debt,
			schufa_total_debt,
			burgelcrif_score_value,
			burgelcrif_score_decision,
			burgelcrif_person_known,
			experian_score,
			experian_person_known,
			delphi_score,
			equifax_score,
			equifax_rating,
			agg_unpaid_products,
			focum_score,
			focum_rating,
			score,
			order_scoring_comments,
			subscription_limit,
			initial_subscription_limit,
			active_subscription_value,
			subscriptions,
			failed_recurring_payments,
			subscription_revenue_paid,
			outstanding_amount,
			outstanding_asset_value,
			subscription_revenue_chargeback,
			subscription_revenue_refunded,
			payment_category_fraud_rate,
			payment_category_info,
			cart_risk_fraud_rate,
			cart_risk_info,
			payment_method,
			payment_method_type,
			sepa_mandate_id,
			sepa_mandate_date,
			psp_reference,
			paypal_email,
			card_type,
			card_issuing_country,
			card_issuing_bank,
			funding_source,
			cardholder_name,
			shopper_email,
			gateway_type,
			is_duplicate_ip,
			is_gmx_email,
			mismatching_name,
			mismatching_city,
			is_foreign_card,
			is_prepaid_card,
			is_fraud_type,
			has_failed_first_payments,
			rules_score,
			levenshtein_distance_namecheck_emailcheck,
			related_order_ids,
			is_negative_remarks,
			manual_review_orders,
			anomaly_score,
			approve_cutoff,
			decline_cutoff,
			creation_timestamp,
			file_path,
			model_name,
			link_customer,
			ip_location,
			related_customer_info,
			accounts_cookie,
			web_users,
			similar_customers_count,
			top_similar_scores,
			snf_link,
			previous_manual_reviews,
			schufa_address_string,
			verified_zipcode,
			id_check_state,
			id_check_type,
			id_check_url,
			id_check_result,
			documents_match,
			document_number,
			first_month_free_voucher,
			paypal_name,
			onfido_trigger,
			company_name,
			company_type_name,
			company_type_id,
			lead_type,
			is_in_dca,
			seon_link,
			product_details,
			subs_details,
			previous_orders_details,
			tag_name,
			payment_errors,
			fico,
			precise_id_score,
			fpdscore,
			identity_network_score,
			identity_risk_score,
			address_first_seen_days,
			address_to_name,
			address_validity_level,
			email_first_seen_days,
			email_to_name,
			phone_line_type,
			phone_to_name,
			nb_addr_matching_customer_id,
			users_matching_addrr,
			signals,
			credit_card_match,
			num_diff_cc,
			num_diff_pp,
			declined_orders,
			approved_orders,
			cancelled_orders,
			ffp,
			auto_approve_flag,
			es_deyde_score,
			registered,
			es_deyde_result,
			is_active_card,
			is_submitted_after_card
	)
		select distinct
			dag_enriched,
			omr.order_id,
			omr.customer_id,
			customer_type,
			ip_address_order,
			last_month_submitted_orders,
			total_orders,
			order_rank,
			market_price,
			order_item_count,
			product_names,
			active_subscriptions,
			active_subscriptions_categories,
			held_category,
			order_item_quantity,
			order_item_plan_duration,
			ordered_risk_labels,
			nethone_risk_category,
			order_item_prices,
			status,
			voucher_code,
			created_date,
			submitted_date,
			manual_review_ends_at,
			approved_date,
			store_type,
			store_country,
			declined_reason,
			initial_scoring_decision,
			decline_reason_new,
			customer_created_at,
			first_subscription_date,
			new_recurring,
			retention_group,
			billing_city,
			billing_postal_code,
			billing_country,
			shipping_street,
			shipping_additional_info,
			shipping_city,
			shipping_postal_code,
			first_name,
			last_name,
			phone_number,
			phone_carrier,
			birthdate,
			age,
			customer_email,
			customer_paypal_email,
			paypal_verified,
			paypal_address_confirmed,
			customer_scoring_result,
			customer_label,
			fraud_type,
			burgel_person_known,
			burgel_score,
			verita_person_known_at_address,
			verita_score,
			schufa_class,
			schufa_identity_confirmed,
			schufa_paid_debt,
			schufa_total_debt,
			burgelcrif_score_value,
			burgelcrif_score_decision,
			burgelcrif_person_known,
			experian_score,
			experian_person_known,
			delphi_score,
			equifax_score,
			equifax_rating,
			agg_unpaid_products,
			focum_score,
			focum_rating,
			score,
			order_scoring_comments,
			subscription_limit,
			initial_subscription_limit,
			active_subscription_value,
			subscriptions,
			failed_recurring_payments,
			subscription_revenue_paid,
			outstanding_amount,
			outstanding_asset_value,
			subscription_revenue_chargeback,
			subscription_revenue_refunded,
			payment_category_fraud_rate,
			payment_category_info,
			cart_risk_fraud_rate,
			cart_risk_info,
			payment_method,
			payment_method_type,
			sepa_mandate_id,
			sepa_mandate_date,
			psp_reference,
			paypal_email,
			card_type,
			card_issuing_country,
			card_issuing_bank,
			funding_source,
			cardholder_name,
			shopper_email,
			gateway_type,
			is_duplicate_ip,
			is_gmx_email,
			mismatching_name,
			mismatching_city,
			is_foreign_card,
			is_prepaid_card,
			is_fraud_type,
			has_failed_first_payments,
			rules_score,
			levenshtein_distance_namecheck_emailcheck,
			related_order_ids,
			is_negative_remarks,
			manual_review_orders,
			anomaly_score,
			approve_cutoff,
			decline_cutoff,
			creation_timestamp,
			file_path,
			model_name,
			link_customer,
			ip_location,
			related_customer_info,
			accounts_cookie,
			web_users,
			similar_customers_count,
			top_similar_scores,
			snf_link,
			previous_manual_reviews,
			schufa_address_string,
			verified_zipcode,
			id_check_state,
			id_check_type,
			id_check_url,
			id_check_result,
			documents_match,
			document_number,
			first_month_free_voucher,
			paypal_name,
			onfido_trigger,
			company_name,
			company_type_name,
			company_type_id,
			lead_type,
			is_in_dca,
			seon_link,
			product_details,
			subs_details,
			previous_orders_details,
			tag_name,
			payment_errors,
			fico,
			precise_id_score,
			fpdscore,
			identity_network_score,
			identity_risk_score,
			address_first_seen_days,
			address_to_name,
			address_validity_level,
			email_first_seen_days,
			email_to_name,
			phone_line_type,
			phone_to_name,
			nb_addr_matching_customer_id,
			users_matching_addrr,
			signals,
			credit_card_match,
			num_diff_cc,
			num_diff_pp,
			declined_orders,
			approved_orders,
			cancelled_orders,
			ffp,
			af.auto_approve_flag,
			reu.degree_of_resemblance as es_deyde_score,
			reu.registered,
			reu.similarity_category as es_deyde_result,
			COALESCE(cc.is_active_card,False) as is_active_card,
			CASE WHEN co.is_submitted_after_card=1 THEN True ELSE False END as is_submitted_after_card
FROM
		ods_data_sensitive.order_manual_review_tmp omr
		LEFT JOIN tmp_auto_approve_flag af
			ON omr.order_id = af.order_id
		LEFT join tmp_risk_eu_order_decision reu
			on omr.order_id = reu.order_id
		LEFT JOIN (
		SELECT
			customer_id,
			first_card_created_date,
			is_active_card
		FROM
				) cc ON omr.customer_id = cc.customer_id
		LEFT JOIN (
		SELECT
			order_id,
			is_submitted_after_card
		FROM
			) co ON omr.order_id = co.order_id
;

COMMIT;

truncate table skyvia.critical_orders;
insert into skyvia.critical_orders
 with a as (
 select o.status,i.* from ods_production.inventory_reservation i
 left join ods_production.order o on o.order_id = i.order_number
 where o.status in ('MANUAL REVIEW')
 order by quantity desc
)
,a_n as (
select sku_variant_code,sum(quantity) as orders
from a
where store_id not in ('621','629')
group by 1)
, stock as (
select sku_variant_code,max(in_stock_count) as in_stock_count,
min(available_count) as available_count
from ods_production.inventory_store_variant_availability isva
left join ods_production.store s on isva.store_id = s.id
and store_id  not in ('621', '629')
group by 1
)
,check_ as (
select a.*,s.in_stock_count,
available_count,
orders - in_Stock_count as diff,
v.variant_name
from a_n a
left join stock s on a.sku_variant_code = s.sku_variant_code
left join ods_production.variant v  on v.variant_sku = s.sku_variant_code
where available_count < 0
)
select a.order_number as order_id,
store_id,
c.sku_variant_code as variant_sku,
variant_name,
sum(quantity) as item_quantity
from check_ c
left join a on c.sku_variant_code =  a.sku_variant_code
group by 1,2,3,4 ;

-- Grant access to dremio user group
GRANT SELECT on ods_data_sensitive.order_manual_review TO GROUP data_lake_engine;

DROP TABLE IF EXISTS skyvia.order_manual_review;

CREATE TABLE skyvia.order_manual_review AS
SELECT * FROM ods_data_sensitive.order_manual_review;

GRANT SELECT ON skyvia.order_manual_review TO risk_users_redash;

-- Store execution ts
DROP TABLE IF EXISTS ods_data_sensitive.order_manual_review_last_update;
CREATE TABLE ods_data_sensitive.order_manual_review_last_update AS
SELECT GETDATE() AS last_update;

GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO skyvia;
