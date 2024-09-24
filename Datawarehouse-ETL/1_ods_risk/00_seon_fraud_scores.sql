DROP TABLE IF EXISTS ods_production.seon_fraud_scores_curated ;

CREATE TABLE ods_production.seon_fraud_scores_curated AS
WITH seon_email_cleaning AS 
(
SELECT se.*
FROM s3_spectrum_rds_dwh_order_approval.seon_email_data se
INNER JOIN ods_data_sensitive.customer_pii cp  -- Ensure we ARE ONLY taking the correct customer emails. Customers changing email address might cause issues. 
	ON cp.customer_id::VARCHAR(20) = se.customer_id::VARCHAR(20)
	AND cp.email = se.email_address
)
SELECT
	COALESCE(se.seon_id, sp.seon_id) AS seon_id,
	COALESCE(se.customer_id, sp.customer_id) AS customer_id,
	COALESCE(se.order_id, sp.order_id) AS order_id,
	COALESCE(se.fraud_score, sp.fraud_score) AS seon_fraud_score,
	GREATEST(se.created_at, sp.created_at)::timestamp AS score_created_at,
	GREATEST(se.updated_at, sp.updated_at)::timestamp AS updated_at,
	se.email_score,
	sp.phone_score
FROM seon_email_cleaning se
FULL OUTER JOIN s3_spectrum_rds_dwh_order_approval.seon_phone_data sp
	ON COALESCE(se.order_id, 'A') = COALESCE(sp.order_id, 'A')
	AND se.customer_id = sp.customer_id 
	AND se.seon_id = sp.seon_id -- Important TO link via SEON ID so that Fraud Scores MATCH.
; 