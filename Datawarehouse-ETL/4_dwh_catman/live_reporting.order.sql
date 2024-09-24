DROP TABLE IF EXISTS tmp_live_reporting_order;
CREATE TEMP TABLE tmp_live_reporting_order AS 

WITH cte_order_placed_v2 AS (--using the new order placed table in case they decommissioned order placed v1 TABLE
	SELECT
	    to_timestamp (event_timestamp, 'yyyy-mm-dd HH24:MI:SS') AS event_time
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'store'), 'store_id') AS store_id
		,JSON_EXTRACT_PATH_text(payload,'order_number') AS order_number
		,JSON_EXTRACT_PATH_text(payload,'order_mode') AS order_mode
		,JSON_EXTRACT_PATH_text(JSON_EXTRACT_PATH_text(payload,'user'), 'user_id') AS user_id
		,row_number() OVER (PARTITION BY order_number ORDER BY event_timestamp ASC) AS idx
	FROM stg_kafka_events_full.stream_internal_order_placed_v2 q_
	WHERE coalesce(user_id::int,0) NOT IN (1550695, 1192749)		
)
, order_placed AS (
	SELECT 
		op.event_time,
		convert_timezone('Europe/Berlin',op.event_time::timestamp) AS submitted_date_berlin_time,
		convert_timezone('US/Eastern',op.event_time::timestamp) AS submitted_date_us_time, --here does NOT have timezone AND they ARE IN uk timezone
		op.order_number,
		op.user_id,
		st.country_name,
		st.store_short,
		op.store_id
	FROM cte_order_placed_v2 op
	LEFT JOIN bi_ods.store st
	    ON st.id = op.store_id
	WHERE op.idx = 1	
		AND op.order_mode != 'SWAP'
		AND COALESCE(st.country_name, 'Andorra') NOT IN ('Andorra', 'United Kingdom')
		AND CASE -- filtering just SPECIFIC days
				WHEN st.country_name = 'United States' AND (
						Date_trunc('day',submitted_date_us_time) >= current_date-30
					OR (Date_trunc('day',submitted_date_us_time)) = '2021-11-26'
					OR (Date_trunc('day',submitted_date_us_time)) = '2022-11-25'
					OR (Date_trunc('day',submitted_date_us_time)) = '2023-11-24'
					OR ((date_part('week',submitted_date_us_time ) = date_part('week',current_date)-4))
					OR ((date_part('week',submitted_date_us_time ) = date_part('week',current_date)) 
						AND (date_part('year',submitted_date_us_time ) = date_part('year',current_date)-1))
					OR ((date_part('week',submitted_date_us_time ) = date_part('week',current_date)+1) 
						AND (date_part('year',submitted_date_us_time ) = date_part('year',current_date)-1))
						) THEN 1 
				WHEN st.country_name <> 'United States' AND (
						Date_trunc('day',submitted_date_berlin_time) >= current_date-30
					OR (Date_trunc('day',submitted_date_berlin_time)) = '2021-11-26'
					OR (Date_trunc('day',submitted_date_berlin_time)) = '2022-11-25'
					OR (Date_trunc('day',submitted_date_berlin_time)) = '2023-11-24'
					OR ((date_part('week',submitted_date_berlin_time ) = date_part('week',current_date)-4))
					OR ((date_part('week',submitted_date_berlin_time ) = date_part('week',current_date)) 
						AND (date_part('year',submitted_date_berlin_time ) = date_part('year',current_date)-1))
					OR ((date_part('week',submitted_date_berlin_time ) = date_part('week',current_date)+1) 
						AND (date_part('year',submitted_date_berlin_time ) = date_part('year',current_date)-1))
						) THEN 1 
					END = 1						
)
, cancelled_ AS (
	SELECT DISTINCT
		event_name,
		JSON_EXTRACT_PATH_text(payload,'order_number') AS order_number,
		ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY event_timestamp DESC) AS rank_cancelled_orders
	FROM stg_kafka_events_full.stream_internal_order_cancelled	
)
--
-- from here is to replace the table live_reporting.billing_payments_final, so we do not need this table to run hourly
--
--
, billing_payments_final_prep AS (
	SELECT
		kafka_received_at as event_timestamp
		,json_extract_path_text(json_extract_array_element_text(JSON_EXTRACT_PATH_text(payload,'line_items'),0),'order_number') as order_number
		,row_number() over (partition by order_number order by kafka_received_at desc) as last_event
		,event_name
	from stg_curated.stg_internal_billing_payments
	where  is_valid_json(payload)
)
, dates_billing_payments_final_prep AS (
	select
		order_number,
		min(case when event_name = 'paid' then event_timestamp end ) as paid_date
	from billing_payments_final_prep
	group by 1
)
, billing_payments_final AS (
	SELECT DISTINCT 
		op.order_number, 
		op.event_name,
		da.paid_date
	FROM billing_payments_final_prep op
	left join dates_billing_payments_final_prep da
		on op.order_number = da.order_number
	WHERE last_event = 1
)
--
-- until here is to replace the table live_reporting.billing_payments_final
--
--
, new_infra_data AS (
	SELECT DISTINCT
		op.order_number AS order_id,
		op.submitted_date_berlin_time,
		op.submitted_date_us_time,
		CASE WHEN cn.event_name ='cancelled' THEN 'CANCELLED'
			WHEN bp.paid_date IS NOT NULL THEN 'PAID'
			WHEN bp.event_name = 'failed' THEN 'FAILED FIRST PAYMENT'
			WHEN af.decision = 'approve' THEN 'APPROVED'
			WHEN af.decision = 'decline' THEN 'DECLINED'
			WHEN fd.fraud_check_decision = 'manual_review' THEN 'MANUAL REVIEW'
			ELSE 'PENDING APPROVAL' 
			END AS status,
		CASE
			 WHEN u.user_type = 'business_customer' THEN 'B2B'||' '||  op.country_name 
			 WHEN op.store_short in ('Partners Online','Partners Offline') THEN 'Partnerships'||' '|| op.country_name
			 ELSE 'Grover'||' '|| op.country_name
			END AS store_commercial,
		CASE WHEN store_commercial ILIKE '%United States%' THEN TIMEZONE('US/Eastern', timestamptz(current_timestamp))
			 ELSE TIMEZONE('Europe/Berlin', timestamptz(current_timestamp))
			 END AS current_local_timestamp,
		op.store_id::varchar
	FROM order_placed op
	LEFT JOIN billing_payments_final bp
		ON bp.order_number = op.order_number
	LEFT JOIN bi_ods.order_fraud_check fd
	   ON fd.order_number = op.order_number
	LEFT JOIN bi_ods.order_decision af
		ON af.order_id = op.order_number
	LEFT JOIN cancelled_ cn 
		ON cn.order_number = op.order_number
		AND cn.rank_cancelled_orders = 1
	LEFT JOIN stg_api_production.spree_users u
	 	ON u.id = op.user_id 	
)
	 --old infra data for computing paid date
, paid_date AS (
	 SELECT
	    order_id,
	    max(start_date) AS paid_date
	 FROM live_reporting.subscription
	 GROUP BY 1
)
, old_infra_data AS (
    SELECT DISTINCT
		a."number" AS order_id,
		convert_timezone('Europe/Berlin',s.createddate::timestamp) AS submitted_date_berlin_time,
		convert_timezone('US/Eastern',s.createddate::timestamp) AS submitted_date_us_time, --here does NOT have timezone AND they ARE IN uk timezone
		CASE
			WHEN replace(upper(coalesce(s.status, a.state)),'_',' ') IN ('CANCELLED','CANCELED') THEN 'CANCELLED'
			WHEN s.status = 'MANUAL REVIEW' AND a.state = 'declined' THEN 'DECLINED'
			ELSE replace(upper(coalesce(s.status, a.state)),'_',' ') END AS status,
		CASE
			WHEN u.user_type='business_customer' THEN 'B2B'||' '|| st.country_name
			WHEN st.store_short IN ('Partners Online','Partners Offline') THEN 'Partnerships'||' '|| st.country_name
			ELSE 'Grover'||' '||  st.country_name
			END AS store_commercial,
		CASE WHEN store_commercial ILIKE '%United States%' THEN TIMEZONE('US/Eastern', timestamptz(current_timestamp))
			 ELSE TIMEZONE('Europe/Berlin', timestamptz(current_timestamp))
			 END AS current_local_timestamp,
		COALESCE(s.store_id__c,a.store_id)::VARCHAR as store_id
	FROM stg_api_production.spree_orders a
	LEFT JOIN stg_api_production.spree_users u
		on u.id=a.user_id
	LEFT JOIN stg_salesforce.order s
		on a."number"=s.spree_order_number__c
	LEFT JOIN bi_ods.store st
		ON COALESCE(s.store_id__c,a.store_id)::VARCHAR = st.id
	WHERE
		COALESCE(CASE WHEN s.spree_customer_id__c::VARCHAR=' '
					THEN NULL ELSE s.spree_customer_id__c::VARCHAR END, 
				a.user_id::VARCHAR, '0'::varchar)::integer NOT IN (1550695 , 1192749)
		AND COALESCE(st.country_name, 'Andorra') NOT IN ('Andorra', 'United Kingdom')
		AND CASE -- filtering just SPECIFIC days
			WHEN st.country_name = 'United States' AND (
					Date_trunc('day',submitted_date_us_time) >= current_date-30 
				OR (Date_trunc('day',submitted_date_us_time)) = '2021-11-26'
				OR (Date_trunc('day',submitted_date_us_time)) = '2022-11-25'
				OR (Date_trunc('day',submitted_date_us_time)) = '2023-11-24'
				OR ((date_part('week',submitted_date_us_time ) = date_part('week',current_date)-4))
				OR ((date_part('week',submitted_date_us_time ) = date_part('week',current_date)) 
					AND (date_part('year',submitted_date_us_time ) = date_part('year',current_date)-1))
				OR ((date_part('week',submitted_date_us_time ) = date_part('week',current_date)+1) 
					AND (date_part('year',submitted_date_us_time ) = date_part('year',current_date)-1))
					) THEN 1 
			WHEN st.country_name <> 'United States' AND (
					Date_trunc('day',submitted_date_berlin_time) >= current_date-30 
				OR (Date_trunc('day',submitted_date_berlin_time)) = '2021-11-26'
				OR (Date_trunc('day',submitted_date_berlin_time)) = '2022-11-25'
				OR (Date_trunc('day',submitted_date_berlin_time)) = '2023-11-24'
				OR ((date_part('week',submitted_date_berlin_time ) = date_part('week',current_date)-4))
				OR ((date_part('week',submitted_date_berlin_time ) = date_part('week',current_date)) 
					AND (date_part('year',submitted_date_berlin_time ) = date_part('year',current_date)-1))
				OR ((date_part('week',submitted_date_berlin_time ) = date_part('week',current_date)+1) 
					AND (date_part('year',submitted_date_berlin_time ) = date_part('year',current_date)-1))
					) THEN 1 
				END = 1	
)
SELECT * 
FROM new_infra_data
	UNION ALL
SELECT * 
FROM old_infra_data
;

BEGIN TRANSACTION;

DELETE FROM live_reporting.order WHERE 1=1;

INSERT INTO live_reporting.order
SELECT * FROM tmp_live_reporting_order;

END TRANSACTION;
