INSERT INTO stg_curated.risk_internal_eu_manual_review_result_v1
(SELECT
result_reason,
created_at::TIMESTAMP AS created_at,
store_country_iso,
customer_id,
published_at::TIMESTAMP AS published_at,
result_comment,
"result",
order_id,
employee_id,
consumed_at::TIMESTAMP AS created_date,
updated_at::TIMESTAMP AS updated_at
FROM s3_spectrum_kafka_topics_raw.risk_internal_eu_manual_review_result_v1 t1
WHERE  CAST(("year" || '-' || "month" || '-' || "day") AS date) >= current_date::date-2
AND
created_date > (SELECT MAX(created_date) FROM stg_curated.risk_internal_eu_manual_review_result_v1));
