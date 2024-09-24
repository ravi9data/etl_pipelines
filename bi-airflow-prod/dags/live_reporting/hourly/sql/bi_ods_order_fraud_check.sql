DROP TABLE IF EXISTS tmp_bi_ods_order_fraud_check;
CREATE TEMP TABLE tmp_bi_ods_order_fraud_check AS 

WITH fraud_check_output AS
	(
   SELECT
		DISTINCT
        event_timestamp,
        JSON_EXTRACT_PATH_TEXT(payload, 'user_id') AS user_id,
        JSON_EXTRACT_PATH_TEXT(payload, 'order_number') AS order_number,
		JSON_EXTRACT_PATH_TEXT(payload, 'decision') AS fraud_check_decision,
        (TIMESTAMPTZ 'epoch' + (JSON_EXTRACT_PATH_TEXT(payload, 'completed_at'))::INT * interval '1 second') AS completed_at,
        ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp DESC) AS idx
    FROM stg_kafka_events_full.stream_scoring_customer_fraud_check_completed
)
	SELECT *
	FROM fraud_check_output
	WHERE idx = 1
 ;

BEGIN TRANSACTION;

DELETE FROM bi_ods.order_fraud_check WHERE 1=1;

INSERT INTO bi_ods.order_fraud_check
SELECT * FROM tmp_bi_ods_order_fraud_check;

END TRANSACTION;