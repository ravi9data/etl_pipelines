drop table if exists dm_bd.store_visits_sub_orders;
create table dm_bd.store_visits_sub_orders as
with stores as (select distinct s.store_number,  s.zipcode, ds.bundesland, s.account_name  from ods_production.store s
left join public.dim_states ds on ds.plz = s.zipcode
where s.store_type like '%offline')
, subs_ as (select sv.store_number, count(distinct subscription_id) as acquired_subs,
count(distinct case when s2.status ='ACTIVE' then subscription_id end) as active_subs,
sum(case when s2.status ='ACTIVE' then s2.subscription_value end) as asv,
sum(s2.subscription_value) as acquired_sub_value
from stores sv
left join master.subscription s2 on s2.store_number  = sv.store_number
where s2.start_date::date > '2021-08-17'
group by 1)
, orders_ as (select sv.store_number, 
count(distinct case when o.paid_date is not null then order_id end) as paid_orders,
count(case when submitted_date is not null then o.order_id end) as submitted_orders,
sum(case when submitted_date is not null then o.order_value end) as submitted_order_value,
sum(case when o.paid_date is not null then o.order_value end) as paid_order_value
from stores sv
left join master.order o on o.store_number  = sv.store_number
where o.created_date > '2021-08-17'
group by 1)
, visited_stores_aggregation as(select distinct sv.store_number, 
count(distinct case when type_of_visit = 'visit' then sv.visit_date end) as no_of_visits,
count(distinct case when type_of_visit = 'visit + training' then sv.visit_date end) as no_of_visits_training,
count(distinct case when type_of_visit = 'SiS' then sv.visit_date end) as SiS,
count(distinct visit_date) as total_visits,
min(sv.visit_date::date) as is_visited_first,
max(sv.visit_date::date) as is_visited_recent
from dm_bd.store_visit_dates sv
group by 1)
, visited_stores as (select sv.store_number, total_visits, no_of_visits, no_of_visits_training, SiS, is_visited_first, is_visited_recent,
case when vs.total_visits > 0 then true else false end as is_visited,
 (case 
    when total_visits = 1 then 'once'
    when total_visits = 2 then 'twice'
    when total_visits >= 3 then 'more than thrice'
    end) as visit_status
from stores sv
left join visited_stores_aggregation vs on vs.store_number = sv.store_number)
, final_ as(select sv.store_number, sv.account_name, zipcode,
    bundesland, visit_status,
    total_visits, no_of_visits, no_of_visits_training, SiS,
    is_visited_first, is_visited_recent, s.acquired_subs, s.active_subs, s.asv, acquired_sub_value, 
paid_orders,
submitted_orders,
submitted_order_value,
paid_order_value,
row_number() over (
    order by paid_orders desc) as rank_by_paid_order,
coalesce(is_visited, false) as is_visited
from stores  sv
left join subs_ s on s.store_number = sv.store_number
left join visited_stores vs on vs.store_number = sv.store_number
left join orders_ o on o.store_number = sv.store_number)
select * from final_;

GRANT SELECT ON ALL TABLES IN SCHEMA dm_bd TO sahin_top;
GRANT SELECT ON dm_bd.store_visits_sub_orders TO tableau;
