drop table if exists dm_commercial.order_fraud_check;
create table dm_commercial.order_fraud_check as
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

	GRANT SELECT ON dm_commercial.order_fraud_check TO tableau;
	GRANT SELECT ON dm_commercial.order_fraud_check TO GROUP BI;
