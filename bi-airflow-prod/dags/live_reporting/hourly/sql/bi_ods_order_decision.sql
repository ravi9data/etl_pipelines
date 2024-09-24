DROP TABLE IF EXISTS tmp_bi_ods_order_decision;
CREATE TEMP TABLE tmp_bi_ods_order_decision AS 

WITH order_decision AS
	(
	SELECT
		 q_.*,
	    ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp DESC) AS idx
	FROM  stg_kafka_events_full.stream_internal_risk_order_decisions_v3 q_
	),
	grouping_risk AS
	(
	SELECT DISTINCT *
	FROM order_decision
	WHERE idx = 1
	)
	,approved AS
	(
	SELECT DISTINCT
		order_number AS order_id,
		decision,
		decision_message,
		to_timestamp (event_timestamp, 'yyyy-mm-dd HH24:MI:SS') AS decision_date,
		CASE WHEN decision = 'decline' THEN decision_message ELSE NULL END AS declined_reason,
		CASE WHEN decision = 'decline' THEN decision_date END AS declined_date,
		CASE WHEN decision = 'approve' THEN decision_date END AS approved_date
	FROM grouping_risk
	)
 SELECT * FROM approved
 ;

BEGIN TRANSACTION;

DELETE FROM bi_ods.order_decision WHERE 1=1;

INSERT INTO bi_ods.order_decision
SELECT * FROM tmp_bi_ods_order_decision;

END TRANSACTION;