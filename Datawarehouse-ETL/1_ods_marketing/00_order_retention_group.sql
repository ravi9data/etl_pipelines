--RETENTION

drop table if exists trans_dev.retention_group_1;
drop table if exists trans_dev.retention_group_2;
drop table if exists ods_production.retention_group_test;

create table trans_dev.retention_group_1 as
select distinct
o.order_id,
o.customer_id,
o.created_date as order_date,
o.acquisition_date,
o.status,
row_number() over (partition by o.customer_id, case when o.status = 'PAID' then true else false end order by o.created_date asc, o.order_id asc) as new_order_rank, --for a case when customer like has multiple orders created at the same time, probably kafka event issue
 case
  when o.order_rank = 1
   and date_diff('hour',c.created_at::timestamp,o.created_date::timestamp) > 24
   and ((o.status = 'PAID' and new_order_rank = 1) or o.status != 'PAID')
	   then 'NEW, REGISTERED >24h AGO'
	 when o.order_rank = 1
	  and date_diff('hour',c.created_at::timestamp,o.created_date::timestamp) <= 24
	  and ((o.status = 'PAID' and new_order_rank = 1) or o.status != 'PAID')
	   then 'NEW, COMPLETELY'
	 when o.order_rank > 1
	  and (o.created_date < o.acquisition_date
	       OR o.acquisition_date IS NULL
	       OR o.created_date = o.acquisition_date)
	  and ((o.status = 'PAID' and new_order_rank = 1) or o.status != 'PAID')
	   then 'NEW, PLACED ORDER BEFORE'
	 WHEN o.order_rank = 1 AND o.customer_id IS NULL
	 	THEN 'NEW, NEVER LOGGED IN'
	 end as retention_group
from ods_production."order" o
left join ods_production.customer c
 on c.customer_id=o.customer_id
order by o.created_date asc;


create table trans_dev.retention_group_2 as
with b as (
select
 o.customer_id,
 o.order_id,
 o.order_Date,
 o.retention_group,
 s.subscription_id,
 s.start_date,
 s.cancellation_date,
 s.subscription_value as subscription_amount,
 s.order_id as sub_order_id
from trans_dev.retention_group_1 o
left join ods_production.subscription s
 on o.customer_id = s.customer_id)
select
order_id,
sum(
case
 when (order_date >= start_date)
  and order_id<>sub_order_id
 then subscription_amount+0.001
 else 0
end) as mrr_active_before_t,
sum(case
 when (order_date >= start_date)
  and order_date > cancellation_date
and order_id<>sub_order_id
  then subscription_amount+0.001
 else 0
end) as mrr_cancelled_before_t
from b
group by 1
 ;


drop table if exists ods_production.order_retention_group;

create table ods_production.order_retention_group as
select distinct
test_1.order_id,
case when test_1.retention_group is not null then 'NEW'
else 'RECURRING' end as new_recurring,
coalesce(test_1.retention_group,
case
 when mrr_active_before_t = mrr_cancelled_before_t
   then 'RECURRING, REACTIVATION'
 when mrr_active_before_t > mrr_cancelled_before_t
   then 'RECURRING, UPSELL'
 end) as retention_group
from trans_dev.retention_group_1 test_1
left join trans_dev.retention_group_2 test_2
 on test_1.order_id = test_2.order_id;


--grant select on ods_production.order_retention_group to akshay_shetty;
GRANT SELECT ON ods_production.order_retention_group TO tableau;
