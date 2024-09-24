DROP TABLE IF EXISTS us_payment_data;
create temp table us_payment_data as
with first_transaction_status as 
(
SELECT 
	uuid,
	account_to  as group_id,
	type as payment_type,
	amount as amount_due,
	status as paid_status,
	t.failed_reason,
	created_at::timestamp as created_at,
	case when status = 'failed' then created_at::timestamp end as failed_date,
	row_number() over (partition by account_to order by created_at::timestamp) as row_num_f,
	row_number() over (partition by account_to order by created_at::timestamp desc) as row_num_l
	from 
	oltp_billing.transaction t
),
last_failed_date as 
(
SELECT 
	uuid,
	account_to as group_id,
	t.failed_reason as failed_reason,
	case when status = 'failed' then created_at::timestamp end as failed_date,
	row_number() over (partition by account_to order by created_at::timestamp desc) as row_num
from 
	oltp_billing.transaction t
where 
	t.status = 'failed'
)
select 
	payment_id,
	due_date ,
	COALESCE(paid_date ,case when t1.paid_status = 'success' then t1.created_at end) as paid_date,
	COALESCE(s.paid_status ,case when t1.paid_status = 'success' then 'PAID' end) as paid_status,
	s.amount_due ,
	s.amount_paid ,
	COALESCE(s.failed_date , f.failed_date) as failed_date,
	s.dpd ,
	s.country_name ,
	case when t.paid_status = 'failed' then 1 else 0 end as failed_payment,
	s.payment_type ,
	t.row_num_l as attempts_to_pay,
	s.category_name ,
	s.currency ,
	s.customer_id ,
	s.customer_type ,
	COALESCE(t1.payment_type,s.payment_method_detailed) as payment_method,
	COALESCE(s.payment_processor_message, f.failed_reason) as payment_processor_message,
	s.payment_number ,
	s.store_label ,
	s.store_name ,
	s.store_short ,
	s.subscription_payment_category ,
	s.subscription_plan ,
	s.subscription_id ,
	s2.status 
from 
	master.subscription_payment s
		left join
		master.subscription s2 
		on s.subscription_id = s2.subscription_id 
		left join
		oltp_billing.payment_order b
		on b.uuid = s.payment_id 
			left join 
			first_transaction_status t
			on t.group_id = b."group" 
			and t.row_num_f = 1
				left join 
				first_transaction_status t1
				on t1.group_id = b."group" 
				and t1.row_num_l = 1
					left join 
					last_failed_date f
					on f.group_id = b."group" 
					and f.row_num = 1
where 
	s.country_name = 'United States'
;


drop table if exists eu_payments ;
create temp table eu_payments as
select 
	payment_id,
	due_date ,
	paid_date,
	paid_status,
	s.amount_due ,
	s.amount_paid ,
	failed_date,
	s.dpd ,
	s.country_name ,
	case when failed_date is not null then 1 else 0 end as failed_payment,
	s.payment_type ,
	s.attempts_to_pay::bigint as attempts_to_pay,
	s.category_name ,
	s.currency ,
	s.customer_id ,
	s.customer_type ,
	s.payment_method_detailed as payment_method,
	s.payment_processor_message,
	s.payment_number ,
	s.store_label ,
	s.store_name ,
	s.store_short ,
	s.subscription_payment_category ,
	s.subscription_plan ,
	s.subscription_id ,
	s2.status 
from 
	master.subscription_payment s
	left join
		master.subscription s2 
		on s.subscription_id = s2.subscription_id 
where 
	s.country_name != 'United States'
	;


drop table if exists dm_risk.dc_subscription_payments;
create table dm_risk.dc_subscription_payments as
with eu_ as
(select
e.*,
case
	when payment_method in ('CreditCard','dalenys-bankcard-gateway','paypal-gateway','sepa-gateway', 'sepa-express-gateway','Adyen, Other','PayPal','bankcard-gateway','Adyen, SEPA') then 'Retry'
	when payment_method in ('ManualTransfer', 'Manual') then 'Customer Transfer - Manual Payment'
	when payment_method in ('Adyen, 1-click') then 'Debt Collection'
	else 'Other'
end as payment_method_grouped,
case when failed_payment = 1 then amount_due end as failed_amount,
case when dpd = 0 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_0,
case when dpd = 1 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_1,
case when dpd = 2 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_2,
case when dpd = 3 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_3,
case when dpd = 4 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_4,
case when dpd <= 4 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_dpd_4,
case when dpd <= 10 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_dpd_10,
case when dpd <= 30 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_dpd_30,
case when p.pre_due_test_labels_10dpd is null then 'NOT IN TEST' 
	else COALESCE(p.pre_due_test_labels_10dpd ,'NOT IN TEST') end as pre_due_test_labels_10dpd,
case when p.pre_due_test_labels_30dpd is null then 'NOT IN TEST' 
	else COALESCE(p.pre_due_test_labels_30dpd ,'NOT IN TEST') end as pre_due_test_labels_30dpd
from eu_payments e
left join
trans_dev.payment_pre_due_ab_testing p
on e.customer_id = p.customer_id 
),
us_ as
(
select  
u.*,
case
	when payment_method = 'retry' then 'Retry'
	when payment_method = 'manual_booking' then 'Customer Transfer - Manual Payment'
	when payment_method = 'manual_booking' then 'Customer Transfer - Debit'
	when payment_method = 'dc-payment' then 'Debt Collection'
	else 'Other'
end as payment_method_grouped,
case when failed_payment = 1 then amount_due end as failed_amount,
case when dpd = 0 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_0,
case when dpd = 1 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_1,
case when dpd = 2 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_2,
case when dpd = 3 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_3,
case when dpd = 4 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_day_4,
case when dpd <= 4 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_dpd_4,
case when dpd <= 10 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_dpd_10,
case when dpd <= 30 and failed_payment = 1 and paid_date is not null then 1 else 0 end as recovered_dpd_30,
case when p.pre_due_test_labels_10dpd is null then 'NOT IN TEST' 
	else COALESCE(p.pre_due_test_labels_10dpd ,'NOT IN TEST') end as pre_due_test_labels_10dpd,
case when p.pre_due_test_labels_30dpd is null then 'NOT IN TEST' 
	else COALESCE(p.pre_due_test_labels_30dpd ,'NOT IN TEST') end as pre_due_test_labels_30dpd
from  us_payment_data u
left join
trans_dev.payment_pre_due_ab_testing p
on u.customer_id = p.customer_id 
)
select * from eu_
union
select * from us_
;



/*
--dont drop and recreate
--AB Testing Lables
drop table if exists trans_dev.payment_pre_due_ab_testing;
create table trans_dev.payment_pre_due_ab_testing as
with payment_diff as (
select 
customer_id,
max(dpd) as max_dpd,
sum(case when dpd <=10 then 1 else 0 end) as is_payment_diff_lt_10,
sum(case when dpd <=30 then 1 else 0 end) as is_payment_diff_lt_30,
count(1) as total_payments
FROM 
dm_risk.dc_subscription_payments
where due_date <= current_date
and status != 'CANCELLED'
group by 1
),
random_10_dpd as(
select 
customer_id,
case when is_payment_diff_lt_10 = total_payments then 1 else 0 end as is_eligible_10dpd,
case when is_payment_diff_lt_30 = total_payments then 1 else 0 end as is_eligible_30dpd,
case when (round(random()*10000))%2=0 then 'TEST' ELSE 'CONTROL' end as test_group_10dpd
from payment_diff
where is_eligible_10dpd = 1
),
random_30_dpd as(
select 
customer_id,
case when is_payment_diff_lt_10 = total_payments then 1 else 0 end as is_eligible_10dpd,
case when is_payment_diff_lt_30 = total_payments then 1 else 0 end as is_eligible_30dpd,
case when (round(random()*10000))%2=0 then 'TEST' ELSE 'CONTROL' end as test_group_30dpd
from payment_diff
where is_eligible_30dpd = 1 and is_eligible_10dpd = 0
)
SELECT 
p.customer_id,
case 
	when d.customer_id is not null then test_group_10dpd
	else 'NOT IN TEST'
end as pre_due_test_labels_10dpd,
case 
	when d2.customer_id is not null then test_group_30dpd
	else 'NOT IN TEST'
end as pre_due_test_labels_30dpd
from 
payment_diff p
left join
random_10_dpd d
on d.customer_id = p.customer_id
left join
random_30_dpd d2
on d2.customer_id = p.customer_id
;

--QA
select 
pre_due_test_labels_30dpd,
count(1)
from 
trans_dev.payment_pre_due_ab_testing
group by 1
;

--Adding the test labels
alter table dm_risk.dc_subscription_payments
add column pre_due_test_labels_10dpd varchar(20)
default NULL;

alter table dm_risk.dc_subscription_payments
add column pre_due_test_labels_30dpd varchar(20)
default NULL;

--Inserting Labels
update  dm_risk.dc_subscription_payments
set pre_due_test_labels_10dpd = trans_dev.payment_pre_due_ab_testing.pre_due_test_labels_10dpd
FROM trans_dev.payment_pre_due_ab_testing
where trans_dev.payment_pre_due_ab_testing.customer_id = dm_risk.dc_subscription_payments.customer_id

update  dm_risk.dc_subscription_payments
set pre_due_test_labels_30dpd = trans_dev.payment_pre_due_ab_testing.pre_due_test_labels_30dpd
FROM trans_dev.payment_pre_due_ab_testing
where trans_dev.payment_pre_due_ab_testing.customer_id = dm_risk.dc_subscription_payments.customer_id



--QA
select 
pre_due_test_labels_10dpd,
count(1)
from 
dm_risk.dc_subscription_payments
group by 1
*/


--Updating table for new payments
update  dm_risk.dc_subscription_payments
set pre_due_test_labels_30dpd = 'NOT IN TEST'
where pre_due_test_labels_30dpd is null;

update  dm_risk.dc_subscription_payments
set pre_due_test_labels_10dpd = 'NOT IN TEST'
where pre_due_test_labels_10dpd is null;

GRANT SELECT ON dm_risk.dc_subscription_payments TO tableau;
