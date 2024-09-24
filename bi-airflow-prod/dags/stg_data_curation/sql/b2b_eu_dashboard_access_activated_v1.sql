delete from staging.b2b_eu_dashboard_access_activated_v1
where consumed_at >= current_date-2;

insert into staging.b2b_eu_dashboard_access_activated_v1
select distinct
kafka_received_at,consumed_at,key,
    JSON_EXTRACT_PATH_text(payload, 'uuid') as uuid,
     JSON_EXTRACT_PATH_text(payload, 'company_id') as company_id,
      JSON_EXTRACT_PATH_text(payload, 'activated_by') as activated_by,
       JSON_EXTRACT_PATH_text(payload, 'activated_at') as activated_at,
       JSON_EXTRACT_PATH_text(payload, 'created_at') as created_at,
       JSON_EXTRACT_PATH_text(payload, 'updated_at') as updated_at
       from
        s3_spectrum_kafka_topics_raw.b2b_eu_dashboard_access_activated_v1 s
where cast((s.year||'-'||s."month"||'-'||s."day"||' 00:00:00') as timestamp) > current_date::date-3
AND consumed_at >= current_date -2;
