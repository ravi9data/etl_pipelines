delete from stg_curated.checkout_eu_cart_created_v1
where consumed_at >= current_date-2;

insert into stg_curated.checkout_eu_cart_created_v1
select distinct
published_at ,
tracing_id,
kit_uuid,
order_id,
customer_id,
created_at,
consumed_at,
store_code
       from
        s3_spectrum_kafka_topics_raw.checkout_eu_cart_created_v1 s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
AND consumed_at >= current_date -2;
