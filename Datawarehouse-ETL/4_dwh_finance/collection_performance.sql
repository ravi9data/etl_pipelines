drop table if exists dwh.collection_performance;

create table dwh.collection_performance as 
WITH date_sequence AS (
SELECT
DISTINCT DATUM AS dd
FROM public.dim_dates
WHERE DATUM <= CURRENT_DATE
)
,payments as (
select distinct 
sp.allocation_id as  asset_allocation_id,
sp.subscription_payment_id as payment_id, 
sp.payment_number,
sp.due_date::date as due_date,
sp.paid_date::date as paid_date,
d.dpd,
d.subscription_payment_category
from ods_production.payment_subscription sp
left join ods_production.payment_subscription_details d 
 on sp.subscription_payment_id=d.subscription_payment_id
where due_date >='2017-01-01'
)
,prep as (
select
d.dd as date_,
p.*,
  case 
   when (subscription_payment_category like ('%ARREARS%') 
    or  subscription_payment_category like ('%DELINQUENT%') 
    or  subscription_payment_category like ('%DEFAULT%')) 
   then 1 else 0 end as overdue_payments,
   case when subscription_payment_category like ('%RECOVERY%') 
   --and  subscription_payment_category not like ('%DEFAULT%')
and paid_date <= d.dd
   then 1 else 0 end as recovered_payments
from date_sequence d
left join payments p  
 on d.dd > due_date
 and d.dd <= due_date+30
where payment_id is not null and dpd > 1
order by 1)
,prep_2 as (
select
d.dd as date_,
p.*,
  case 
   when (subscription_payment_category like ('%ARREARS%') 
    or  subscription_payment_category like ('%DELINQUENT%') 
    or  subscription_payment_category like ('%DEFAULT%')) 
   then 1 else 0 end as overdue_payments,
   case when subscription_payment_category like ('%RECOVERY%') 
   --and  subscription_payment_category not like ('%DEFAULT%')
and paid_date <= d.dd
   then 1 else 0 end as recovered_payments
from date_sequence d
left join payments p  
 on d.dd > due_date+30
 and d.dd <= due_date+60
where payment_id is not null and dpd > 30
order by 1)
,prep_3 as (
select
d.dd as date_,
p.*,
  case 
   when (subscription_payment_category like ('%ARREARS%') 
    or  subscription_payment_category like ('%DELINQUENT%') 
    or  subscription_payment_category like ('%DEFAULT%')) 
   then 1 else 0 end as overdue_payments,
   case when subscription_payment_category like ('%RECOVERY%') 
   --and  subscription_payment_category not like ('%DEFAULT%')
and paid_date <= d.dd
   then 1 else 0 end as recovered_payments
from date_sequence d
left join payments p  
 on d.dd > due_date+60
 and d.dd <= due_date+90
where payment_id is not null and dpd > 60
order by 1)
,prep_4 as (
select
d.dd as date_,
p.*,
  case 
   when (subscription_payment_category like ('%ARREARS%') 
    or  subscription_payment_category like ('%DELINQUENT%') 
    or  subscription_payment_category like ('%DEFAULT%')) 
   then 1 else 0 end as overdue_payments,
   case when subscription_payment_category like ('%RECOVERY%') 
   --and  subscription_payment_category not like ('%DEFAULT%')
and paid_date <= d.dd
   then 1 else 0 end as recovered_payments
from date_sequence d
left join payments p  
 on d.dd > due_date+90
where payment_id is not null and dpd > 90
order by 1)
, a as(
select
date_,
coalesce(sum(overdue_payments),0) as overdue_payments,
coalesce(sum(recovered_payments),0) as recovered_payments
from prep
group by 1)
, b as(
select 
date_, 
coalesce(sum(overdue_payments),0) as overdue_payments,
coalesce(sum(recovered_payments),0) as recovered_payments
from prep_2
group by 1
)
, c as(
select 
date_, 
coalesce(sum(overdue_payments),0) as overdue_payments,
coalesce(sum(recovered_payments),0) as recovered_payments
from prep_3
group by 1
)
, d as(
select 
date_, 
coalesce(sum(overdue_payments),0) as overdue_payments,
coalesce(sum(recovered_payments),0) as recovered_payments
from prep_4
group by 1
)
select 
coalesce(a.date_,b.date_,c.date_,d.date_) as date_,
COALESCE(A.OVERDUE_PAYMENTS,0) as EARLY_OVERDUE,
COALESCE(A.RECOVERED_PAYMENTS,0) as EARLY_RECOVERY,
COALESCE(B.OVERDUE_PAYMENTS,0) as LATE_OVERDUE,
COALESCE(B.RECOVERED_PAYMENTS,0) as LATE_RECOVERY,
COALESCE(C.OVERDUE_PAYMENTS,0) as LATE_LATE_OVERDUE,
COALESCE(C.RECOVERED_PAYMENTS,0) as LATE_LATE_RECOVERY,
COALESCE(D.OVERDUE_PAYMENTS,0) as LATE_LATE_LATE_OVERDUE,
COALESCE(D.RECOVERED_PAYMENTS,0) as LATE_LATE_LATE_RECOVERY
from a
full outer join b 
 on a.date_=b.date_
full outer join c
 on a.date_=c.date_
full outer join d
 on a.date_=d.date_;

GRANT SELECT ON dwh.collection_performance TO debt_management_redash;
GRANT SELECT ON dwh.collection_performance TO elene_tsintsadze;
GRANT SELECT ON dwh.collection_performance TO tableau;
