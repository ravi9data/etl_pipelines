delete from staging.kafka_order_cancelled
where event_timestamp >= current_date-1 ;

insert into staging.kafka_order_cancelled
select
DISTINCT kafka_received_at::timestamp event_timestamp
,event_name
,JSON_EXTRACT_PATH_text(payload,'order_number') as order_number
,JSON_EXTRACT_PATH_text(payload,'user_id') as user_id
,INITCAP(REPLACE((JSON_EXTRACT_PATH_text(payload,'reason')),'_',' ')) as reason
from s3_spectrum_kafka_topics_raw.internal_order_cancelled
where cast(("year"||'-'||"month"||'-'||"day") as date) > current_date::date-2
AND event_timestamp >= current_date-2;
