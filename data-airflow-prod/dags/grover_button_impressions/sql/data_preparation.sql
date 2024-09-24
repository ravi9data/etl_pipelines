DROP TABLE IF EXISTS stg_events_dl.grover_button_impressions_agg;

CREATE TABLE stg_events_dl.grover_button_impressions_agg AS(
SELECT
	date_trunc('hour',TIMESTAMP 'epoch' + creation_time::INT * interval '1 second') AS reporting_date,
	store_id,
	product_sku,
	button_state,
	COUNT(1) AS impressions,
	COUNT(DISTINCT ip_address) AS unique_impressions
FROM s3_spectrum_kinesis_events_raw.grover_button_impression
WHERE
	TO_TIMESTAMP(year||month||day||hour, 'YYYYMMDDHH24')::TIMESTAMP >= '{{ti.xcom_pull(key='start_at')}}'::TIMESTAMP - interval '1 hour'
AND
	TO_TIMESTAMP(year||month||day||hour, 'YYYYMMDDHH24')::TIMESTAMP <= '{{ti.xcom_pull(key='end_at')}}'::TIMESTAMP
AND
	reporting_date >= '{{ti.xcom_pull(key='start_at')}}'::TIMESTAMP
AND
	reporting_date <= '{{ti.xcom_pull(key='end_at')}}'::TIMESTAMP
GROUP BY 1,2,3,4);
