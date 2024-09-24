CREATE OR REPLACE VIEW dm_risk.v_orders_in_manual_review_report AS
WITH dates AS (
	SELECT 
		d.datum + dt.time AS fact_date,
		lag(fact_date) OVER ( ORDER BY fact_date) AS previous_fact_date
	FROM public.dim_dates d
	CROSS JOIN public.dim_times dt
	WHERE d.datum >= DATEADD('week', -6, current_date)
		AND fact_date <= convert_timezone('Europe/Berlin',current_timestamp::timestamp) 
		AND dt.is_on_hour 
		AND dt."hour" IN (8, 12, 18) -- the hours that we will CHECK the reports
		--for hourly report, comment the row above
)
, union_tables AS (
--intermediate tables
	SELECT 
		a.order_id,
		convert_timezone('Europe/Berlin',a.published_at::timestamp) AS published_at,
		a.outcome_namespace,
		a.store_country_iso AS store_country,
		NULLIF(lower(a.decision_reason),'null') AS decision_reason,
		a.outcome_message AS outcome_message,
		'False'::varchar AS outcome_is_final,
		COALESCE(c.customer_type, 'n/a') AS customer_type,
		'intermediate'::varchar AS table_source
	FROM stg_curated.risk_eu_order_decision_intermediate_v1 a
	LEFT JOIN master.customer c 
		ON a.customer_id = c.customer_id
	WHERE published_at::date >= DATEADD('week', -12, current_date) --putting one week MORE than the cut TO take the orders that ARE still IN backlog
		UNION 
	SELECT 
		us.order_id,
		convert_timezone('Europe/Berlin', us.published_at::timestamp) AS published_at,
		us.outcome_namespace,
		us.store_country_iso AS store_country,
		NULLIF(lower(us.decision_reason),'null') AS decision_reason,
		us.outcome_message,
		'False'::varchar AS outcome_is_final,
		COALESCE(c.customer_type, 'n/a') AS customer_type,
		'intermediate'::varchar AS table_source
	FROM stg_curated.risk_us_order_decision_intermediate_v1 us
	LEFT JOIN master.customer c 
		ON us.customer_id = c.customer_id
	WHERE us.published_at::date >= DATEADD('week', -12, current_date)
		UNION 
-- final decision tables
	SELECT 
		a.order_id,
		convert_timezone('Europe/Berlin',a.published_at::timestamp) AS published_at,
		a.outcome_namespace,
		a.store_country_iso AS store_country,
		a.decision_reason,
		a.outcome_message,
		a.outcome_is_final,
		COALESCE(c.customer_type, 'n/a') AS customer_type,
		'final decision'::varchar AS table_source
	FROM stg_curated.risk_us_order_decision_final_v1  a
	LEFT JOIN master.customer c 
		ON a.customer_id = c.customer_id
	WHERE published_at::date >= DATEADD('week', -12, current_date)
		UNION 
	SELECT 
		a.order_id,
		convert_timezone('Europe/Berlin',a.published_at::timestamp) AS published_at,
		a.outcome_namespace,
		a.store_country_iso AS store_country,
		a.decision_reason,
		a.outcome_message,
		a.outcome_is_final,
		COALESCE(c.customer_type, 'n/a') AS customer_type ,
		'final decision'::varchar AS table_source
	FROM s3_spectrum_kafka_topics_raw.risk_eu_order_decision_final_v1  a
	LEFT JOIN master.customer c 
		ON a.customer_id = c.customer_id
	WHERE published_at::date >= DATEADD('week', -12, current_date)
-- MR decision
		UNION
	SELECT 
		a.order_id,
		convert_timezone('Europe/Berlin',a.published_at) AS published_at,
		a.RESULT AS outcome_namespace,
		a.store_country_iso AS store_country,
		a.result_reason AS decision_reason,	
		a.result_comment AS outcome_message,
		'False'::varchar AS outcome_is_final,
		COALESCE(c.customer_type, 'n/a') AS customer_type,
		'mr decision'::varchar AS table_source
	FROM stg_curated.risk_internal_eu_manual_review_result_v1 a
	LEFT JOIN master.customer c 
		ON a.customer_id = c.customer_id
	WHERE published_at::date >= DATEADD('week', -12, current_date)
		UNION 
	SELECT 
		us.order_id,
		convert_timezone('Europe/Berlin',us.consumed_at::timestamp) AS published_at,
		us.status AS outcome_namespace,
		'US'::varchar AS store_country,
		us.reason AS decision_reason,	
		us.comments AS outcome_message,
		'False'::varchar AS outcome_is_final,
		COALESCE(c.customer_type, 'n/a') AS customer_type,
		'mr decision'::varchar AS table_source
	FROM stg_curated.risk_internal_us_risk_manual_review_order_v1 us
	LEFT JOIN master.customer c 
		ON us.customer_id = c.customer_id
	WHERE us.consumed_at::date >= DATEADD('week', -12, current_date)	
)
, status_with_fact_date AS (
	SELECT DISTINCT 
		d.fact_date,
		d.previous_fact_date,
		a.order_id,
		a.published_at,
		a.outcome_namespace,
		a.store_country,
		a.decision_reason,	
		a.outcome_message,
		a.outcome_is_final,
		a.customer_type,
		a.table_source,
		row_number() over (partition by a.order_id, d.fact_date order by a.published_at desc) rn1, -- TO SELECT the LAST record FOR EACH ORDER
		row_number() over (partition by a.order_id, d.fact_date, a.table_source order by a.published_at desc) rn3
	FROM dates d
	LEFT JOIN union_tables a
		ON d.fact_date::timestamp >= a.published_at::timestamp	
)
,  processed_orders AS (
	SELECT 
		r.fact_date,
		r.order_id,
		r.published_at,
		r.outcome_namespace,
		r.decision_reason,	
		r.outcome_message
	FROM status_with_fact_date r 
	WHERE r.table_source = 'mr decision'
		AND rn3 = 1 
		AND r.published_at BETWEEN r.previous_fact_date AND r.fact_date
)
, prep AS (
	SELECT DISTINCT 
		r.fact_date,
		r.order_id,
		r.published_at,
		r.outcome_namespace,
		r.store_country,
		r.decision_reason,	
		r.outcome_message,
		r.outcome_is_final,
		r.customer_type,
		r.table_source,
		CASE WHEN r.outcome_namespace IN ('APPROVED', 'DECLINED') THEN
			row_number() over (partition by r.order_id, r.published_at order by r.fact_date ASC) 
			ELSE 1 END AS rn2, -- IF an ORDER has a FINAL outcome, we will keep just IN the date OF the outcome AND THEN, DELETE this ORDER FROM it
		po.published_at AS published_at_processed_orders,
		po.outcome_namespace AS outcome_namespace_processed_orders,
		po.decision_reason AS decision_reason_processed_orders,	
		po.outcome_message AS outcome_message_processed_orders
	FROM status_with_fact_date r
	LEFT JOIN master.ORDER o
		ON r.order_id = o.order_id
		AND COALESCE(o.canceled_date, '2999-01-01 00:00:00') >= r.fact_date
	LEFT JOIN processed_orders po 
		ON po.order_id = r.order_id
		AND po.fact_date = r.fact_date
	WHERE r.rn1 = 1 
		AND o.order_id IS NOT NULL -- TO DELETE the cancelled orders	
)
SELECT DISTINCT 
	r.fact_date,
	r.order_id,
	r.published_at,
	r.outcome_namespace,
	r.store_country,
	r.decision_reason,	
	r.outcome_message,
	r.outcome_is_final,
	r.customer_type,
	r.table_source,
	r.published_at_processed_orders,
	r.outcome_namespace_processed_orders,
	r.decision_reason_processed_orders,	
	r.outcome_message_processed_orders
FROM prep r
WHERE 	
	CASE WHEN outcome_namespace IN ('APPROVED', 'DECLINED') AND rn2 = 1 THEN 1
--		 WHEN outcome_namespace IN ('APPROVED', 'DECLINED') AND fact_date::date = published_at::date THEN 1 -- for hourly report leave this row and comment the next 2 rows
		 WHEN outcome_namespace IN ('APPROVED', 'DECLINED') AND date_part('hour',fact_date) = 12 AND rn2 = 2 THEN 1 -- so the orders that had a FINAL DECISION AT 8am, ALSO SHOW AT 12pm
		 WHEN outcome_namespace IN ('APPROVED', 'DECLINED') AND date_part('hour',fact_date) = 18 AND rn2 IN (2,3) THEN 1 -- so the orders that had a FINAL DECISION AT 8am or 12p8, ALSO SHOW AT 12pm
		 WHEN r.outcome_namespace IN ('ID_VERIFICATION', 'ERROR', 'BAS_VERIFICATION', 'EXPIRED') AND DATEADD('day',7,r.published_at) < r.fact_date THEN 2 
		 		--DELETE the orders WITH these status, IF they are more than 7 days, otherwise the table will only increase
		 ELSE rn2
		END = 1
WITH NO SCHEMA BINDING;
