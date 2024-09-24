INSERT INTO stg_curated.risk_eu_order_decision_intermediate_v1
(SELECT
updated_at:: TIMESTAMP AS updated_at,
outcome_namespace,
outcome_message,
published_at:: TIMESTAMP AS published_at,
created_at:: TIMESTAMP created_at,
store_country_iso,
order_id,
customer_id:: INTEGER AS customer_id,
consumed_at:: TIMESTAMP AS consumed_at,
outcome_timeout_days,
rules_applied,
outcome_is_final,
collected_data,
decision_reason
FROM s3_spectrum_kafka_topics_raw.risk_eu_order_decision_intermediate_v1
WHERE CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND consumed_at > (SELECT MAX(consumed_at) FROM stg_curated.risk_eu_order_decision_intermediate_v1));
