drop table if exists ods_production.customer_voucher_usage;
create table ods_production.customer_voucher_usage as 
with a as (
select 
 customer_id,
 o.order_id,
 status as order_status,
 new_recurring,
  created_date,
  paid_date,
  submitted_date,
 voucher_code, 
 is_special_voucher,
 voucher_type,
 voucher_value,
 voucher_discount
from ods_production."order" o 
left join ods_production.order_retention_group r on r.order_id=o.order_id
where voucher_code is not null)
,b as (
select 
 customer_id, 
 count(distinct case when order_status ='PAID' then order_id end) as paid_orders_with_voucher,
 count(distinct case when order_status ='PAID' then voucher_code end) as unique_vouchers_redeemed,
 sum(distinct case when order_status ='PAID' then voucher_discount end) as total_voucher_discount
from a 
group by 1)
,ref as (
select distinct customer_id_mapped as customer_id 
from traffic.page_views v 
where v.page_url like '%/join%' 
and user_registration_date::date <= v.page_view_start::date+30
and user_registration_date::date >= v.page_view_start::date)
,referals as (
  select 
  voucher_code, 
  count(distinct case when paid_Date is not null then order_id end) as paid_referal_orders,
  min(distinct case when paid_Date is not null then paid_Date end) as paid_referal_date,
  count(distinct case when submitted_date is not null then order_id end) as submitted_referal_orders,
  min(distinct case when submitted_date is not null then submitted_date end) as submitted_referal_date,
  count(distinct order_id) as cart_referal_orders,
  count(distinct case when paid_Date is not null then customer_Id end) as paid_referal_customers,
  count(distinct case when submitted_date is not null then customer_id end) as submitted_referal_customers,
  count(distinct customer_id) as cart_referal_customers
  from a
  where voucher_code like ('G-%')
  group by 1
  )
,cust as (
select 
 customer_id, 
 listagg(distinct voucher_code, ' / ') as unique_vouchers_redeemed,
 listagg(distinct is_special_voucher, ' / ') as unique_special_vouchers_redeemed,
 listagg(distinct case when new_recurring = 'NEW' then voucher_code end, ' / ') as voucher_of_first_order,
 listagg(distinct case when new_recurring = 'NEW' then is_special_voucher end, ' / ') as special_voucher_of_first_order
from a 
group by 1)
select 
c.customer_id,
c.referral_code,
b.paid_orders_with_voucher,
b.unique_vouchers_redeemed,
b.total_voucher_discount,
cust.unique_special_vouchers_redeemed,
cust.special_voucher_of_first_order,
case when c.referral_code is not null then true else false end as referred_a_friend,
r.paid_referal_orders,
r.submitted_referal_orders,
r.paid_referal_date,
r.submitted_referal_date,
r.cart_referal_orders,
r.paid_referal_customers,
r.submitted_referal_customers,
r.cart_referal_customers,
case when ref.customer_id is not null then true else false end as is_aquired_via_referals
from ods_production.customer c
left join b
 on b.customer_id=c.customer_id
left join cust 
 on c.customer_id=cust.customer_id
left join referals r 
 on r.voucher_code=c.referral_code
left join ref on ref.customer_id=c.customer_id
;

GRANT SELECT ON ods_production.customer_voucher_usage TO tableau;
