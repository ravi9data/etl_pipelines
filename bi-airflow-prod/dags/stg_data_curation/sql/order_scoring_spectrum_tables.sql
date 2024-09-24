------------------------ONE-----------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.order_approval_results
WHERE extracted_at >= CURRENT_DATE -2;

INSERT INTO stg_curated.order_approval_results
SELECT DISTINCT
creation_timestamp,
updated_at,
"_airbyte_emitted_at" as extracted_at,
order_id,
credit_limit,
decision_action,
decision_code,
comments
FROM stg_curated_dl.order_approval_results s
WHERE  "_airbyte_emitted_at" >= CURRENT_DATE -2;

------------------------TWO------------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.risk_internal_us_risk_decision_result_v1
WHERE consumed_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.risk_internal_us_risk_decision_result_v1
SELECT DISTINCT
created_at,
updated_at,
consumed_at,
order_id,
amount,
code,
message
FROM s3_spectrum_kafka_topics_raw.risk_internal_us_risk_decision_result_v1 s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND consumed_at >= CURRENT_DATE -2;

------------------------THREE----------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.decision_tree_algorithm_results
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.decision_tree_algorithm_results
SELECT DISTINCT
creation_timestamp,
extracted_at,
id,
customer_id,
main_reason
FROM s3_spectrum_rds_dwh_order_approval.decision_tree_algorithm_results s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;

-------------------------FOUR----------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.risk_eu_order_decision_final_v1_from_spectrum
WHERE consumed_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.risk_eu_order_decision_final_v1_from_spectrum
SELECT DISTINCT
order_id,
collected_data,
created_at,
consumed_at,
updated_at,
outcome_is_final
FROM s3_spectrum_kafka_topics_raw.risk_eu_order_decision_final_v1 s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND consumed_at >= CURRENT_DATE -2;

-------------------------FIVE-----------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.risk_us_order_decision_final_v1
WHERE consumed_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.risk_us_order_decision_final_v1
SELECT DISTINCT
order_id,
collected_data,
created_at,
consumed_at,
updated_at,
outcome_is_final
FROM s3_spectrum_kafka_topics_raw_sensitive.risk_us_order_decision_final_v1 s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND consumed_at >= CURRENT_DATE -2;

------------------------SIX----------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.prediction_models
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.prediction_models
SELECT DISTINCT
id,
file_path,
extracted_at
FROM s3_spectrum_rds_dwh_fraud_credit_and_risk.prediction_models s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;

--------------------------SEVEN-----------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.spree_users_subscription_limit_log
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.spree_users_subscription_limit_log
SELECT DISTINCT
created_at,
user_id,
old_subscription_limit,
new_subscription_limit,
extracted_at
FROM s3_spectrum_rds_dwh_order_approval.spree_users_subscription_limit_log s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;

------------------------EIGHT---------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.anomaly_score_order
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.anomaly_score_order
SELECT DISTINCT
order_id,
anomaly_score,
extracted_at
FROM s3_spectrum_rds_dwh_fraud_credit_and_risk.anomaly_score_order s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;

------------------------NINE---------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.payment_risk_categories
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.payment_risk_categories
SELECT DISTINCT
order_id,
payment_category_fraud_rate,
payment_category_info,
extracted_at
FROM s3_spectrum_rds_dwh_fraud_credit_and_risk.payment_risk_categories s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;

------------------------TEN---------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.customer_data
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.customer_data
SELECT DISTINCT
subscription_limit,
order_id,
extracted_at,
creation_timestamp,
payments,
recurrent_failed_payments,
first_failed_payments,
pending_value
FROM s3_spectrum_rds_dwh_fraud_credit_and_risk.customer_data s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;

------------------------ELEVEN---------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.asset_risk_categories_order
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.asset_risk_categories_order
SELECT DISTINCT
order_id,
asset_risk_info,
numeric_fraud_rate,
extracted_at
FROM s3_spectrum_rds_dwh_fraud_credit_and_risk.asset_risk_categories_order s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;

------------------------TWELVE---------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.order_similarity_result
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.order_similarity_result
SELECT DISTINCT
order_id,
customers_count,
top_scores,
extracted_at
FROM s3_spectrum_rds_dwh_order_approval.order_similarity_result s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;

------------------------THIRTEEN---------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.id_verification_order
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.id_verification_order
SELECT DISTINCT
order_id,
customer_id,
verification_state,
is_processed,
"trigger",
"data",
extracted_at,
updated_at
FROM s3_spectrum_rds_dwh_order_approval.id_verification_order s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;


------------------------FOURTEEN----------------------------------
------------------------------------------------------------------

DELETE FROM stg_curated.order_predictions
WHERE extracted_at >= CURRENT_DATE-2;

INSERT INTO stg_curated.order_predictions
SELECT DISTINCT
order_id,
approve_cutoff,
decline_cutoff,
creation_timestamp,
extracted_at,
model_id,
score
FROM s3_spectrum_rds_dwh_fraud_credit_and_risk.order_predictions s
WHERE CAST((s.YEAR||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND extracted_at >= CURRENT_DATE -2;
