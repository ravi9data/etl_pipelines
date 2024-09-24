DROP TABLE IF EXISTS dm_risk.bank_acccount_snapshot_results; 

CREATE TABLE dm_risk.bank_acccount_snapshot_results AS
WITH bas_kafka_data AS 
(
	SELECT DISTINCT
		event_name, 
		kafka_received_at::timestamp event_timestamp,
		json_extract_path_text(payload, 'user_id') AS customer_id,
		json_extract_path_text(payload, 'order_number') AS order_id,
		json_extract_path_text(payload, 'order_approved') AS order_approved,
		json_extract_path_text(payload, 'tracing_id') AS tracing_id,
		json_extract_path_text(payload, 'provider') AS provider,
		json_extract_path_text(payload, 'store_code') AS store_code,
		json_extract_path_text(payload, 'country_id') AS country_id,
		json_extract_path_text(payload, 'id_check_result') AS id_check_result,
		json_extract_path_text(payload, 'id_check_result_reason') AS id_check_result_reason,
		ROW_NUMBER() OVER (PARTITION BY order_id, tracing_id, event_name ORDER BY event_timestamp DESC) AS row_num1,	-- GET latest req/comp event per tracing id + remove duplicates
		ROW_NUMBER() OVER (PARTITION BY order_id, event_name ORDER BY event_timestamp DESC) AS row_num2 -- -- GET latest tracing id per ORDER id (only applying to Requests)
		from stg_curated.risk_users_queue
	WHERE event_name IN ('bank_account_snapshot_request', 'bank_account_snapshot_completed')
)
, completions_remapped AS 
( 
SELECT
	customer_id,
	order_id,
	tracing_id, 
	event_name,
	event_timestamp,
	store_code,
	country_id, 
	CASE -- Note that the data provider changed the format of the result output on 9 May 2022 @ 12pm. This mapping IS TO ensure consistancy OF OUTPUT OVER this boundary.
		WHEN id_check_result_reason = 'Customer not verified: Names mismatch' 
			THEN  'FAILURE'
		WHEN id_check_result_reason = 'rules failed' 
			THEN  'FAILURE'
		WHEN id_check_result_reason = 'Customer verified: Names match' 
			THEN  'SUCCESS'
		WHEN id_check_result_reason = 'rules approved' 
			THEN  'SUCCESS'
		ELSE id_check_result
	END AS id_check_result,
	id_check_result_reason, -- These categories changed ALSO but have LEFT TO PRESERVE detail.
	row_num1,
	row_num2
FROM bas_kafka_data
WHERE event_name = 'bank_account_snapshot_completed'
	AND row_num1 = 1
) 
SELECT 
	r.customer_id,
	r.order_id,
	r.tracing_id,
	r.event_timestamp AS request_timestamp,
	c.event_timestamp AS completion_timestamp,
	c.id_check_result, 
	c.id_check_result_reason,
	r.store_code,
	r.country_id
FROM bas_kafka_data r
LEFT JOIN completions_remapped c
	ON r.customer_id = c.customer_id
	AND r.order_id = c.order_id
	AND r.tracing_id = c.tracing_id
WHERE r.event_name = 'bank_account_snapshot_request'
	AND r.row_num1 = 1 
	AND r.row_num2 = 1