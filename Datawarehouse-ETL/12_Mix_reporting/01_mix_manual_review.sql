DROP TABLE IF EXISTS stg_last_decision_result;
CREATE TEMP TABLE stg_last_decision_result
SORTKEY(order_id)
DISTKEY(order_id)
AS
SELECT
    order_id,
    amount,
    code,
    message,
    ROW_NUMBER () OVER (PARTITION BY order_id ORDER BY updated_at::timestamp DESC) AS row_num
FROM
    s3_spectrum_rds_dwh_order_approval.decision_result
WHERE
    LEFT(order_id, 1) = 'M';

--getting only most up to date row from Risk decision_result table
DROP TABLE IF EXISTS decision_result;
CREATE TEMP TABLE decision_result
SORTKEY(order_id)
DISTKEY(order_id)
AS
SELECT
    order_id,
    amount,
    code,
    message
FROM
    stg_last_decision_result
WHERE
    row_num=1;



DROP TABLE IF EXISTS ods_data_sensitive.mix_manual_review;
CREATE TABLE ods_data_sensitive.mix_manual_review AS
SELECT
    o.submitted_date,
    o.order_id,
    o.customer_id,
    cp.customer_type,
    sd.active_subscription_product_names AS active_subscriptions,
    sd.active_subscription_category AS active_subscriptions_categories,
    dr.amount,
    dr.code,
    dr.message,
    sd.start_date_of_first_subscription AS first_subscription_date,
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
    cs.burgel_person_known,
    cs.burgel_score,
    cs.verita_person_known_at_address,
    cs.verita_score,
    cs.schufa_class,
    cs.schufa_identity_confirmed,
    cs.schufa_paid_debt,
    cs.schufa_total_debt,
    cs.burgelcrif_score_value,
    cs.burgelcrif_person_known,
    cs.experian_score,
    cs.experian_person_known,
    cs.delphi_score,
    cp.subscription_limit,
    cs.initial_subscription_limit,
    sd.active_subscription_value,
    sd.subscriptions,
    sd.failed_subscriptions AS failed_recurring_payments,
    sd.subscription_revenue_paid,
    sd.subscription_revenue_due - sd.subscription_revenue_paid AS outstanding_amount,
    sd.subscription_revenue_chargeback,
    sd.subscription_revenue_refunded,
    op.payment_method,
    op.payment_method_type,
    op.sepa_mandate_id,
    op.sepa_mandate_date,
    op.psp_reference,
    op.paypal_email,
    op.card_type,
    op.card_issuing_country,
    op.card_issuing_bank,
    op.funding_source,
    op.cardholder_name,
    op.shopper_email,
    op.gateway_type,
    pod.related_order_ids,
    omc.geo_cities AS ip_location,
    upi.related_customer_info,
    cs.accounts_cookie,
    cs.web_users,
    cs.previous_manual_reviews,
    cp.schufa_address_string,
    cs.id_check_state,
    cs.id_check_url,
    cs.id_check_result,
    cs.documents_match,
    cp.paypal_name,
    cs.seon_link
FROM ods_production.mix_order_submitted AS o
LEFT JOIN decision_result AS dr ON o.order_id = dr.order_id
LEFT JOIN ods_production.order_retention_group AS org ON o.order_id = org.order_id
LEFT JOIN ods_data_sensitive.customer_pii AS cp ON o.customer_id = cp.customer_id
LEFT JOIN ods_production.customer_scoring AS cs ON o.customer_id = cs.customer_id
LEFT JOIN ods_production.customer_subscription_details AS sd ON o.customer_id = sd.customer_id
LEFT JOIN ods_data_sensitive.order_payment_method AS op ON o.order_id = op.order_id
LEFT JOIN ods_production.order_decline_reason AS odr ON o.order_id = odr.order_id
LEFT JOIN ods_production.order_manual_review_previous_order_history AS pod ON o.order_id = pod.main_order_id
LEFT JOIN ods_production.customer AS cu ON o.customer_id = cu.customer_id
LEFT JOIN ods_production.order_marketing_channel AS omc ON o.order_id = omc.order_id
LEFT JOIN ods_production.customer_unique_phone_info AS upi ON o.customer_id = upi.main_customer_id
WHERE o.order_status = 'SUBMITTED';


DROP TABLE IF EXISTS skyvia.mix_manual_review;

CREATE TABLE skyvia.mix_manual_review AS
SELECT * FROM ods_data_sensitive.mix_manual_review;


GRANT SELECT ON ALL TABLES IN SCHEMA skyvia TO skyvia;
