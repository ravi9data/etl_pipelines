--Identify all business subscriptions
--Customers could haave moved from being a normal cusstomer to a businesss customer.
--This base dataset identifies all subscriptions that were booked by a customer as a business_entity
CREATE OR REPLACE VIEW dm_risk.v_b2b_payments_report AS 
with base_ AS
(
SELECT 
subscription_id , subscription_bo_id ,s.customer_type , s.customer_id ,date, cancellation_date ,
ROW_NUMBER() over (partition by subscription_id order by date) as row_n,
ROW_NUMBER() over (partition by subscription_id order by date desc) as row_n_2
FROM 
master.subscription_historical s 
join
master.customer c2 
on c2.customer_id = s.customer_id 
and c2.customer_type = 'business_customer'
where s.customer_id is not null
--and subscription_id = 'a0B3W000005LfdSUAS'
--where subscription_id = 'F-C49H87HE'
),
cancellation_cust_type as 
(
SELECT 
s.subscription_id , s.customer_type , s.customer_id ,date, s.cancellation_date ,
ROW_NUMBER() over (partition by subscription_id order by date) as row_n
FROM 
master.subscription_historical s 
join
master.customer c2 
on c2.customer_id = s.customer_id 
and c2.customer_type = 'business_customer'
where 
s.customer_id is not null
and cancellation_date is not null
),
sub_customer_type_ranking as(
select 
a.subscription_id,
a.customer_id ,
max(case when a.row_n = 1 then a.customer_type end) as first_customer_type,
max(case when a.row_n_2 = 1 then a.customer_type end) as last_customer_type,
max(case when c.row_n = 1 then c.customer_type end) as cancellation_customer_type
from base_ a
left join
cancellation_cust_type c
on a.subscription_id = c.subscription_id
group by 1,2
),
subs_for_business_cst as
(
select
*,
case 
	when first_customer_type = 'business_customer' and last_customer_type = 'business_customer' then 'Business Sub'
	when last_customer_type = 'business_customer' and cancellation_customer_type is null then 'Business Sub'
	when cancellation_customer_type = 'business_customer' then 'Business Sub'
	when cancellation_customer_type = 'normal_customer' then 'Normal Sub'
	when first_customer_type = 'business_customer' then 'Business Sub'
	when first_customer_type = 'normal_customer' and last_customer_type = 'business_customer' then 'Transition Sub'
	when last_customer_type = 'normal_customer' then 'Normal Sub'
	when first_customer_type is null and last_customer_type = 'business_customer' then 'Business Sub'
	else 'Business Sub'
end as transition_type
from 
sub_customer_type_ranking 
--and customer_type = 'business_customer'
),
business_subscriptions_1 AS 
(
select * from subs_for_business_cst
where transition_type = 'Business Sub'
),
duplicate_subscriptions AS 
(
select subscription_id 
from business_subscriptions_1 
group by 1 
having count(1)>1
)
--There are 22 subscriptions where the customer id was changed from 188310-498783. Causing duplication in the table.
, cleaned_subscriptions AS 
(
SELECT *
FROM business_subscriptions_1 bs
WHERE NOT EXISTS (SELECT NULL FROM duplicate_subscriptions ds WHERE ds.subscription_id = bs.subscription_id)
	AND customer_id != '188310'
)
, ever_normal_customers AS 
(
SELECT DISTINCT customer_id
FROM master.customer_historical
WHERE customer_type = 'normal_customer'
),
always_bus_customers AS 
(
SELECT DISTINCT customer_id 
FROM master.customer c
WHERE c.customer_type = 'business_customer'
	AND NOT EXISTS (SELECT NULL FROM ever_normal_customers nc WHERE c.customer_id = nc.customer_id )
),
potential_missing_payments AS 
(
SELECT *
FROM master.subscription_payment sp 
WHERE EXISTS (SELECT NULL FROM always_bus_customers bc WHERE bc.customer_id = sp.customer_id)
	AND NOT EXISTS (SELECT NULL FROM tgt_dev.business_subscriptions_1 bs WHERE bs.subscription_id = sp.subscription_id)
)
, subscription_status as
(
select
subscription_id ,
status 
from 
master.subscription s 
where customer_type = 'business_customer'
union
select
subscription_bo_id  as subscription_id,
status 
from 
master.subscription s 
where customer_type = 'business_customer'
and subscription_bo_id is not null
)
, business_subscriptions_missing AS
(
SELECT 
sp.*,
ct.payment_expectation AS customer_payment_expectation,
ct.account_owner AS customer_account_owner,
ct.comments AS customer_account_comments,
c.company_name ,
c.company_status ,
c.company_type_name ,
 CASE
 	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
 	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
	WHEN c.customer_type = 'business_customer' THEN 'B2B Unknown Split'
 	ELSE 'Null'
 END AS freelancer_split,
case when o.is_pay_by_invoice is true then 1 else 0 end as is_pbi,
s.status as subscription_status
FROM potential_missing_payments sp
left join
ods_production.order o
on o.order_id = sp.order_id
LEFT JOIN dm_risk.pbi_customer_tracking ct 
	ON sp.customer_id = ct.customer_id
left join 
master.customer c 
on c.customer_id = sp.customer_id 
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name 
left join 
subscription_status s
on s.subscription_id = sp.subscription_id 
WHERE sp.customer_id NOT IN ('188310', '498783') -- the customer_id CHANGE case
)
--Create a Master Payments View for only Business Subs
, subscription_status_new as
(
select
subscription_id ,
status 
from 
master.subscription s 
where customer_type = 'business_customer'
union
select
subscription_bo_id  as subscription_id,
status 
from 
master.subscription s 
where customer_type = 'business_customer'
and subscription_bo_id is not null
)
, b2b_payments_1 AS
(
select
sp.*,
ct.payment_expectation AS customer_payment_expectation,
ct.account_owner AS customer_account_owner,
ct.comments AS customer_account_comments,
c.company_name ,
c.company_status ,
c.company_type_name ,
 CASE
 	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
 	WHEN c.customer_type = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
	WHEN c.customer_type = 'business_customer' THEN 'B2B Unknown Split'
 	ELSE 'Null'
 END AS freelancer_split,
case when o.is_pay_by_invoice is true then 1 else 0 end as is_pbi,
s.status as subscription_status_new
from
master.subscription_payment sp 
left join
ods_production.order o
on o.order_id = sp.order_id 
LEFT JOIN dm_risk.pbi_customer_tracking ct 
	ON sp.customer_id = ct.customer_id
left join 
master.customer c 
on c.customer_id = sp.customer_id 
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name 
left join 
subscription_status_new s
on s.subscription_id = sp.subscription_id 
where sp.subscription_id in (select distinct subscription_id from business_subscriptions_1)
)
select * from b2b_payments_1
union
select * from business_subscriptions_missing
WITH NO SCHEMA BINDING ; 