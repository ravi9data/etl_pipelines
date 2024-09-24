CREATE OR REPLACE VIEW dm_risk.v_bankacc_snapshot_report AS
-- IMPORTANT NOTE: The data provider changed the description framework of results on 9 May 2022 @ 12pm (perf. decision boundary). This is fixed/re-mapped within the script. TB.
WITH requests_and_completions AS 
(
	SELECT DISTINCT
		event_name, 
		kafka_received_at::timestamp event_timestamp,
		json_extract_path_text(payload, 'user_id') AS customer_id,
		json_extract_path_text(payload, 'order_number') AS order_id,
		json_extract_path_text(payload, 'tracing_id') AS tracing_id,
		json_extract_path_text(payload, 'id_check_result') AS id_check_result,
		json_extract_path_text(payload, 'id_check_result_reason') AS id_check_result_reason
		-- json_extract_path_text(payload, 'order_approved') AS order_approved
	from stg_curated.risk_users_queue
	WHERE event_name LIKE 'bank_account%' 
		AND kafka_received_at::date >= '2022-02-18'  -- We don't have ANY information BEFORE this date due TO missing import.
),
starts_and_submissions AS 
(
	SELECT DISTINCT
		user_id,
		tracing_id,
		se_action,
		collector_tstamp,
		ROW_NUMBER() OVER (PARTITION BY user_id, tracing_id, se_action ORDER BY collector_tstamp DESC) AS row_num
	from scratch.se_events_flat 
	WHERE se_category='bankAccountSnapshot' AND se_action IN ('started', 'submitted')
),
subscription_limits AS 
(
	SELECT
		user_id, 
		new_subscription_limit,
		ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS row_num
	FROM stg_realtimebrandenburg.spree_users_subscription_limit_log
	WHERE update_source = 'BAS service'
),
completions AS
(
SELECT 
	event_timestamp, 
	tracing_id,
	CASE 
		WHEN id_check_result_reason = 'Customer not verified: Names mismatch' THEN  'FAILURE'
		WHEN id_check_result_reason = 'rules failed' THEN  'FAILURE'
		WHEN id_check_result_reason = 'Customer verified: Names match' THEN  'SUCCESS'
		WHEN id_check_result_reason = 'rules approved' THEN  'SUCCESS'
	END AS id_check_result, 
	CASE 
		WHEN id_check_result_reason = 'Customer not verified: Names mismatch' THEN  'rules failed'
		WHEN id_check_result_reason = 'rules failed' THEN  'rules failed'
		WHEN id_check_result_reason = 'Customer verified: Names match' THEN  'rules approved'
		WHEN id_check_result_reason = 'rules approved' THEN  'rules approved'
	END AS id_check_result_reason
FROM requests_and_completions 
WHERE event_name = 'bank_account_snapshot_completed'
	AND id_check_result != 'NOT_COMPLETE' 
),
starts AS
(
SELECT * 
FROM starts_and_submissions 
WHERE se_action = 'started' 
	AND row_num = 1
),
submissions AS
(
SELECT * 
FROM starts_and_submissions 
WHERE se_action = 'submitted' 
	AND row_num = 1
)
SELECT
	j.customer_id,
	j.order_id,
	o.submitted_date,
	c.id_check_result_reason ,
	j.event_timestamp AS request_timestamp,
	c.event_timestamp AS completed_timestamp,
	js.collector_tstamp AS started_timestamp,
	jss.collector_tstamp AS submitted_timestamp,
	CASE WHEN completed_timestamp IS NOT NULL THEN 1 ELSE 0 END AS is_completed,
	CASE WHEN completed_timestamp IS NOT NULL THEN 1
		WHEN submitted_timestamp IS NOT NULL THEN 1
		WHEN started_timestamp IS NOT NULL THEN 1
		ELSE 0 END AS is_started,
	CASE WHEN completed_timestamp IS NOT NULL THEN 1
		WHEN submitted_timestamp IS NOT NULL THEN 1 ELSE 0 END AS is_submitted,
	o.failed_first_payment_orders,
	o.paid_orders,
	o.new_recurring,
	o.customer_type,
	o.declined_reason,
	o.store_country,
	o.current_subscription_limit,
	sl.new_subscription_limit,
	case 
		when o.status in ('CANCELLED','FAILED FIRST PAYMENT','PAID','PENDING APPROVAL','PENDING PAYMENT') then 'APPROVED'
		else o.status 
	end as order_status_final
FROM requests_and_completions j
LEFT JOIN completions c USING(tracing_id)
LEFT JOIN starts js ON j.tracing_id = js.tracing_id AND j.customer_id = js.user_id
LEFT JOIN submissions jss ON j.tracing_id = jss.tracing_id AND j.customer_id = jss.user_id
LEFT JOIN master."order" o USING(order_id)
LEFT JOIN subscription_limits sl ON j.customer_id = sl.user_id::VARCHAR(25) AND sl.row_num = 1 
WHERE event_name = 'bank_account_snapshot_request'
WITH NO SCHEMA BINDING;