DELETE FROM stg_curated.addons_order_status_change_v1
WHERE event_timestamp >= CURRENT_DATE-2;

INSERT INTO stg_curated.addons_order_status_change_v1
SELECT
    DISTINCT
	event_name
	,JSON_EXTRACT_PATH_TEXT(payload,'user_id') as customer_id
	,JSON_EXTRACT_PATH_TEXT(payload,'order_number') as order_id
	,JSON_EXTRACT_PATH_TEXT(payload,'total_amount') as amount
	,JSON_EXTRACT_ARRAY_ELEMENT_TEXT(JSON_EXTRACT_PATH_TEXT(payload,'addons'),0) as addons_
	,JSON_EXTRACT_PATH_TEXT(addons_,'name') as addon_name
	,JSON_EXTRACT_PATH_TEXT(addons_,'duration_in_months') as duration
	,JSON_EXTRACT_PATH_TEXT(addons_,'quantity') as quantity
	,JSON_EXTRACT_PATH_TEXT(addons_,'price') as price
    ,JSON_EXTRACT_PATH_TEXT(payload,'created_at')::timestamp without time zone AS event_timestamp
FROM
	s3_spectrum_kafka_topics_raw.addons_order_status_change_v1 s
WHERE CAST((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') AS TIMESTAMP) > CURRENT_DATE::DATE-3
AND JSON_EXTRACT_PATH_TEXT(payload,'created_at') >= current_date -2;
