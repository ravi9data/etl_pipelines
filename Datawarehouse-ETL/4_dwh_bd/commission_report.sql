drop table if exists dwh.commission_report;
create table dwh.commission_report as 

with subs as(
select distinct(sh.subscription_id) ,
sh.store_label ,
sh.store_name ,
sh.store_short ,
sh.order_id ,
sh.product_name ,
sh.rental_period ,
sh.effective_duration ,
sh.subscription_value,
sh."date" 
from master.subscription_historical sh  
where  effective_duration <= rental_period -- non-renewal subscriptions only
and sh."date" = last_day(sh."date") --eom only since historical 
)
select s.*,
sph.customer_type ,
sph.payment_id,
sph.payment_number ,
sph.paid_date ,
sph.amount_paid ,
sph.amount_refund ,
sph.amount_chargeback,
(sph.amount_paid - COALESCE (sph.amount_refund,0) - COALESCE (sph.amount_chargeback,0)) as net_paid_amount,
(net_paid_amount * 0.05) as commission
from subs s
inner join master.subscription_payment_historical sph on s.subscription_id = sph.subscription_id and s."date"  = sph."date"
and sph.status ='PAID' and date_trunc('month',sph.paid_date) = date_trunc('month',sph."date");