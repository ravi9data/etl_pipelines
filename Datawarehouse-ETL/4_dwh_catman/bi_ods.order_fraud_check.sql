         
DROP TABLE IF EXISTS tmp_bi_ods_order_fraud_check;
CREATE TEMP TABLE tmp_bi_ods_order_fraud_check AS 

with fraud_check_output as
	(
   select
		distinct
        event_timestamp,
        json_extract_path_text(payload, 'user_id') as user_id,
        json_extract_path_text(payload, 'order_number') as order_number,
		json_extract_path_text(payload, 'decision') as fraud_check_decision,
        (timestamptz 'epoch' + (JSON_EXTRACT_PATH_text(payload, 'completed_at'))::int * interval '1 second') as completed_at,
        ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp desc) AS idx
    from stg_kafka_events_full.stream_scoring_customer_fraud_check_completed
)
	select *
	from fraud_check_output
	where idx = 1
 ;

BEGIN TRANSACTION;

DELETE FROM bi_ods.order_fraud_check WHERE 1=1;

INSERT INTO bi_ods.order_fraud_check
SELECT * FROM tmp_bi_ods_order_fraud_check;

END TRANSACTION;