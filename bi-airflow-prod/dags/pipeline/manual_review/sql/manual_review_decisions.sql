drop table if exists stg_external_apis.manual_review_decisions;

create table stg_external_apis.manual_review_decisions as
select
row_number() over(order by 1)  as id
,'David' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_david
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Edwin' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_edwin
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Erensu' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_erensu
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Victor' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_victor
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Madlen' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_madlen
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Federico' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_federico
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Martina' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_martina
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Ganiyu' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_ganiyu
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Ian' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_ian
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Janou' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_janou
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
UNION ALL
select
row_number() over(order by 1)  as id
,'Guney' as reviewer
,case when lower(decision) like 'appr%' then 'Approved'
    when lower(decision) like 'decli%' then 'Declined'
     else decision end as decision
,reason
,comment
,order_id
from stg_external_apis.gs_manual_review_guney
where coalesce(trim(order_id), '') <> ''
and coalesce(trim(decision), '') <> ''
;


-- Load into target table

-- For reruns on same day
delete from ods_data_sensitive.manual_review_decisions where date_trunc('day',synced_at)= trunc(getdate());

insert into ods_data_sensitive.manual_review_decisions
select
current_timestamp as synced_at,
reviewer,
decision,
reason,
comment,
order_id,
id as google_sheet_id
from stg_external_apis.manual_review_decisions;



--manual_review_decisions_historical

delete from ods_data_sensitive.manual_review_decisions_historical
where batch_date=trunc(getdate());

insert into ods_data_sensitive.manual_review_decisions_historical
    select trunc(getdate()) as batch_date,
          mr.*
from ods_data_sensitive.manual_review_decisions mr;
