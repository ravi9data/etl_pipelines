CREATE OR REPLACE VIEW dm_marketing.v_us_orders_reporting AS
WITH billing_payments_grouping AS (
	SELECT DISTINCT kafka_received_at event_timestamp,
		event_name,
		payload
	FROM stg_curated.stg_internal_billing_payments
	WHERE is_valid_json(payload)
		and payload like '%USD%'
)
, order_payments AS (
	SELECT
		event_timestamp ,
		event_name,
		JSON_EXTRACT_PATH_text(payload,'contract_type') as rental_mode,
		JSON_EXTRACT_PATH_text(payload,'line_items') as line_item_payload,
		JSON_ARRAY_LENGTH(json_extract_path_text(payload,'line_items'),true) as total_order_items,
		json_extract_path_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text(payload,'line_items'),0),'order_number') as order_number,
		json_extract_path_text(json_extract_path_text(payload,'payment_method'),'type') as payment_method_type,
		count(*) over (partition by order_number) as total_events,
		rank() over (partition by ordeR_number order by event_timestamp asc) as rank_event,
		case when total_events = rank_event then true else false end as is_last_event,
		first_value(event_timestamp) over (partition by order_number order by event_timestamp asc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_charge_date
	FROM billing_payments_grouping
	WHERE payload ilike '%"contract_type": "FLEX"%'	
)
, dates AS (
	SELECT
		order_number,
		max(case when event_name = 'paid' then event_timestamp end ) as paid_date,
		max(case when event_name = 'failed' then event_timestamp end ) as failed_date,
		max(case when event_name = 'refund' then event_timestamp end ) as refund_date
	FROM order_payments
	GROUP BY 1
)
, billing_payments_final AS (
	SELECT
		op.order_number,
		op.event_name,
		op.payment_method_type,
		op.total_order_items,
		op.rental_mode,
		op.line_item_payload,
		da.paid_date  as paid_date,
		da.failed_date as failed_date
	FROM order_payments op
	LEFT JOIN dates da
	  ON op.order_number = da.order_number
	WHERE op.is_last_event
)
, order_placed_tmp AS (
	SELECT
		q_.event_timestamp ,
		q_.order_number,
		q_.order_mode,
		q_.user_id,
		q_.store_id,
		row_number() over (partition by order_number order by event_timestamp asc) as idx
	FROM (
		SELECT
		event_timestamp ,
		JSON_EXTRACT_PATH_text(payload,'order_number') AS order_number,
		JSON_EXTRACT_PATH_text(payload,'order_mode') AS order_mode,
		JSON_EXTRACT_PATH_text(payload,'user', 'user_id') AS user_id,
		JSON_EXTRACT_PATH_text(payload,'store', 'store_id') AS store_id
		FROM stg_kafka_events_full.stream_internal_order_placed_v2
	) q_
	WHERE q_.store_id = 621
)
, order_decision AS (
	SELECT
		q_.event_timestamp ,
		q_.order_number,
		q_.decision,
		q_.decision_message ,
	    row_number() over (partition by order_number order by event_timestamp desc)  as idx
	FROM stg_kafka_events_full.stream_internal_risk_order_decisions_v3 q_
	WHERE q_.store_id = 621
)
, fraud_check_output AS (
	SELECT DISTINCT
		event_timestamp,
		json_extract_path_text(payload, 'user_id') as user_id,
		json_extract_path_text(payload, 'order_number') as order_number,
		json_extract_path_text(payload, 'decision') as fraud_check_decision,
		(timestamptz 'epoch' + (JSON_EXTRACT_PATH_text(payload, 'completed_at'))::int * interval '1 second') as completed_at,
	    ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp desc) AS idx
	FROM stg_kafka_events_full.stream_scoring_customer_fraud_check_completed
)
, order_placed AS (
	SELECT
		opt.order_number,
		opt.order_mode,
		opt.user_id,
		opt.store_id,
		to_timestamp (opt.event_timestamp, 'yyyy-mm-dd HH24:MI:SS') as event_time
	FROM order_placed_tmp opt
	WHERE opt.order_mode ='FLEX'
	  AND opt.idx = 1
 )
, approved AS (
	SELECT
		order_number as order_id,
		decision,
		decision_message,
		to_timestamp (event_timestamp, 'yyyy-mm-dd HH24:MI:SS') as decision_date,
		case when decision = 'decline' then decision_message else null end as declined_reason,
    	case when decision = 'decline' then decision_date end as declined_date,
		case when decision = 'approve' then decision_date end as approved_date
	FROM order_decision
	WHERE idx = 1
)
, cancelled_ AS (
	 SELECT DISTINCT
	 	event_timestamp as canceled_date,
		event_name,
		JSON_EXTRACT_PATH_text(payload,'order_number') as order_number,
		JSON_EXTRACT_PATH_text(payload,'user_id') as user_id,
		INITCAP(REPLACE((JSON_EXTRACT_PATH_text(payload,'reason')),'_',' ')) as reason,
		ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp desc) AS rank_cancelled_orders
	FROM stg_kafka_events_full.stream_internal_order_cancelled
)
SELECT DISTINCT
	op.order_number as order_id,
	op.event_time::timestamp as submitted_date,
	CASE WHEN cn.event_name ='cancelled'
			THEN 'CANCELLED'
		WHEN paid_date is not NULL
			THEN 'PAID'
 		WHEN bp.event_name = 'failed'
 			THEN 'FAILED FIRST PAYMENT'
		WHEN af.decision = 'approve'
			THEN 'APPROVED'
	    WHEN af.decision = 'decline'
	    	THEN 'DECLINED'
		WHEN fd.fraud_check_decision = 'manual_review'
			THEN 'MANUAL REVIEW'
	ELSE 'PENDING APPROVAL'
	END AS status,
    op.store_id as store_id,
    'United States' as store_commercial
FROM order_placed op
LEFT JOIN billing_payments_final bp
  	ON bp.order_number = op.order_number
LEFT JOIN fraud_check_output fd
  	ON fd.order_number = op.order_number
   AND fd.idx = 1
LEFT JOIN approved af
	ON af.order_id = op.order_number
LEFT JOIN cancelled_ cn
	ON cn.order_number = op.order_number
   AND cn.rank_cancelled_orders = 1
LEFT JOIN ods_production.store st
	ON st.id = op.store_id
LEFT JOIN stg_api_production.spree_users u
	ON u.id=op.user_id
WHERE (Date_trunc('day',submitted_date::timestamp) >= current_date-8
   OR (Date_trunc('day',submitted_date::timestamp)) = '2020-11-27'
   OR (Date_trunc('day',submitted_date::timestamp)) = '2020-11-26'
   OR ((date_part('week',submitted_date::timestamp) = date_part('week',current_date)-4))
   OR ((date_part('week',submitted_date::timestamp) = date_part('week',current_date))
        AND (date_part('year',submitted_date::timestamp) = date_part('year',current_date)-1))
   OR ((date_part('week',submitted_date::timestamp) = date_part('week',current_date)+1)
        AND (date_part('year',submitted_date::timestamp) = date_part('year',current_date)-1)))
WITH NO SCHEMA BINDING;
