
drop table if exists tmp_scoring_customer_fraud_check_completed;

create temp table tmp_scoring_customer_fraud_check_completed
as
with cte as (
select event_name ,json_parse(payload) as p,consumed_at,kafka_received_at
from s3_spectrum_kafka_topics_raw.scoring_customer_fraud_check_completed s
where cast((s.year||'-'||s."month"||'-'||s."day") as date) > current_date::date-2
)
select event_name,
p.user_id::text,
replace(p.order_number::text,'"','') as order_number,
replace(p.decision::text,'"','') as decision,
p.completed_at::text ,
kafka_received_at::timestamp,
consumed_at::timestamp
from cte;

delete from stg_curated.scoring_customer_fraud_check_completed
using tmp_scoring_customer_fraud_check_completed t
where t.order_number = scoring_customer_fraud_check_completed.order_number;


insert into stg_curated.scoring_customer_fraud_check_completed (user_id ,order_number ,decision ,completed_at ,consumed_at ,kafka_received_at )
select user_id ,order_number ,decision ,completed_at ,consumed_at ,kafka_received_at
from tmp_scoring_customer_fraud_check_completed;
