drop table if exists dm_bd.store_visits_sub_orders_daily;
create table dm_bd.store_visits_sub_orders_daily as
with days_ as (
select
distinct date_trunc('day',datum)::date as fact_date, store_number,  account_name 
from public.dim_dates,
(select 
    store_number,
    account_name 
    from
    ods_production.store s
    where s.store_type like '%offline%')
where datum >= '2020-01-01' and datum <= current_date)
, b as (select s.store_number, 
date_trunc('day',s.start_date)::date as start_date,
count(s.subscription_id) as acquired_subs,
sum(s.subscription_value) as acquired_value
from master.subscription s
where s.store_type like '%offline%'
group by 1,2)
, store_visit as (select store_number,
count(case when sv.type_of_visit = 'visit' then sv.store_number end)as no_of_visits,
count(case when sv.type_of_visit = 'visit + training' then sv.store_number end)as no_of_visits_trainings,
min(sv.visit_date) as is_visited_first,
max(sv.visit_date) as is_visited_recent
from dm_bd.store_visit_dates sv
group by 1
)
, orders as (select store_number, 
date_trunc('day',o.created_date)::date as paid_date,
count(case when paid_date is not null then o.order_id end) as orders_paid,
count(case when submitted_date is not null then o.order_id end) as submitted_orders,
sum(case when submitted_date is not null then o.order_value end) as submitted_order_value
from master."order" o 
group by 1,2
)
,final_  as (select 
    distinct d.fact_date,
    d.store_number,
    d.account_name,
    acquired_subs,
    acquired_value,
    orders_paid,
    submitted_orders,
    submitted_order_value,
    (case when d.fact_date >= s.is_visited_first then true else false end) as is_visited_first,
    (case when d.fact_date >= s.is_visited_recent then true else false end) as is_visited_recent,
    no_of_visits_trainings,
    no_of_visits
from days_ d
left join b on b.start_date = d.fact_date 
and b.store_number = d.store_number
left join ods_production.store_visits sv on sv.store_number = d.store_number
left join store_visit s on s.store_number = d.store_number
left join orders os on os.store_number = d.store_number 
and d.fact_date = os.paid_date 
)
select * from final_;

GRANT SELECT ON dm_bd.store_visits_sub_orders_daily TO tableau;
