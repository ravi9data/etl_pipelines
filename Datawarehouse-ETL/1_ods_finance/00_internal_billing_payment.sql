drop table if exists staging.tmp_internal_billing_payments;

create  table staging.tmp_internal_billing_payments as
with c as (
    select *,JSON_EXTRACT_PATH_text(payload,'uuid') as uuid,
      row_number() over (partition by uuid,event_name order by kafka_received_at::timestamp asc) as idx2,
      row_number() over (partition by uuid,event_name order by kafka_received_at::timestamp desc) as idx,
      row_number() over (partition by uuid order by kafka_received_at::timestamp desc) as idx_latest
    from stg_curated.stg_internal_billing_payments
    where is_valid_json(payload)
)
select uuid,event_name,version,payload,kafka_received_at,consumed_at,idx as idx_max,idx_latest
from c
where idx = 1 or idx2 = 1;
