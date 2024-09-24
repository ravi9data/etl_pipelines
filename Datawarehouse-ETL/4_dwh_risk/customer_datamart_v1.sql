drop table if exists dm_risk.customer_datamart_v1 ;
create table dm_risk.customer_datamart_v1 as
with order_data as 
(	
select 
customer_id,
max(last_order_country) as last_order_country,
max(paid_order_date) as max_paid_order_date,
min(paid_order_date) as min_paid_order_date,
min(submitted_date) as first_order_submitted_date
from(
select
	distinct
	o.customer_id ,
	LAST_VALUE(o.store_country) over (partition by customer_id order by submitted_date rows between unbounded preceding and unbounded following) as last_order_country,
     case
        when o.status = 'PAID' and paid_date is not null then o.paid_date
      end as paid_order_date,
    o.submitted_date 
    from ods_production."order" o 
    )
	group by customer_id
),
asset_details as 
(
SELECT 
a.customer_id,
sum(a.initial_price) as asset_purchase_price,
sum(a.residual_value_market_price ) as ever_rented_residual_value_market_price
from 
master.asset a 
group by customer_id 
),
customer_labels_pre as
(
select 
customer_id ,
label as customer_labels ,
ROW_NUMBER () over(partition by customer_id) as row_cnt
from stg_order_approval.customer_labels2 cl2 
),
customer_labels as
(
select 
customer_id ,
customer_labels 
from
customer_labels_pre 
where row_cnt = 1
),
customer_payments as
(
select 
sp.customer_id,
max(sp.amount_subscription) as max_monthly_payment,
count(distinct 
        CASE WHEN sp.due_date <= 'now'::text::date THEN sp.subscription_payment_id
         ELSE NULL
         END) AS payment_count,
count(distinct 
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                    AND sp2.subscription_payment_category <> 'PAID_REFUNDED'::text
                    AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'::text 
                     THEN date_trunc('month', sp.paid_date)
                    ELSE NULL
                END) AS unique_payment_months,
 count(distinct 
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                    AND sp2.subscription_payment_category <> 'PAID_REFUNDED'::text
                    AND sp2.subscription_payment_category <> 'PAID_CHARGEBACK'::text 
                     THEN sp.subscription_payment_id
                    ELSE NULL
                END) AS paid_payments,
 count(distinct 
                CASE
                    WHEN sp.status IN ('FAILED','FAILED FULLY')  AND sp.failed_date is not null THEN sp.subscription_payment_id
                    ELSE NULL
                END) AS failed_payments,
 count(distinct 
                CASE
                    WHEN sp.status IN ('FAILED','FAILED FULLY')  AND sp.failed_date is not null THEN sp.amount_due
                    ELSE NULL
                END) AS amt_failed_subscriptions,
 count(distinct 
                CASE
                    WHEN sp.paid_date IS NOT NULL 
                    AND sp2.subscription_payment_category = 'PAID_REFUNDED'::text
                    AND sp2.subscription_payment_category = 'PAID_CHARGEBACK'::text  THEN sp.subscription_payment_id
                    ELSE NULL
                END) AS refunded_payments,
sum(COALESCE(sp.amount_paid, 0::numeric)) AS amount_paid,
sum(
                CASE
                    WHEN sp.due_date <= 'now'::text::date and sp.paid_date is null THEN COALESCE(sp.amount_due, 0::numeric)
                    ELSE NULL
                END) AS amount_due,
max(sp2.dpd) as max_dpd,
sum(COALESCE(sp.amount_due, 0::numeric)) - sum(COALESCE(sp.amount_paid, 0::numeric)) + sum(COALESCE(sp.chargeback_amount, 0::numeric))+ sum(COALESCE(sp.refund_amount, 0::numeric)) as outstanding_subscription_revenue
            from
 ods_production.payment_subscription sp 
left join ods_production.payment_subscription_details sp2 
on sp2.subscription_payment_id=sp.subscription_payment_id
  			 and coalesce(sp2.paid_date,'1990-05-22')=coalesce(sp.paid_date,'1990-05-22')
          WHERE true and sp.status not in ('CANCELLED')
          GROUP BY sp.customer_id 
),
active_sub_months as
(
select 
	customer_id,
max(case 
	when s.status = 'ACTIVE' then datediff('month',start_date, current_date) 
	when s.status = 'CANCELLED' then datediff('month',start_date, cancellation_date)
	end) as max_active_subscription_months,
sum(case when s.cancellation_reason_new= 'DEBT COLLECTION' then 1 else 0 end) as count_dc_subscriptions,
sum(case when s.cancellation_reason_new= 'DEBT COLLECTION' then exposure_to_default else 0 end) as asset_value_dc_subscriptions,
sum(case when s.cancellation_reason_new= 'RETURNED EARLY - Failed Payments' then 1 else 0 end) as count_returned_subscriptions,
sum(case when s.cancellation_reason_new= 'RETURNED EARLY - Failed Payments' then exposure_to_default else 0 end) as asset_value_returned_subscriptions
from master.subscription s 
group by 1
),
merkmal_flag as(
select * from(
select 
distinct
os.user_id,
case 
	when os.user_id is not null then 1
	else 0
end as merkmal_flag,
row_number()over (partition by order_id order by order_scored_at desc) as row_num 
from ods_production.order_scoring os
where order_scoring_comments ilike '%Merkmal%'
)
where row_num = 1
),
bas as
(
SELECT 
DISTINCT 
customer_id 
FROM stg_realtimebrandenburg.decision_tree_algorithm_results 
	WHERE main_reason like '%BAS'
),
model_score_raw as(
SELECT 
	user_id as customer_id,
	order_scored_at ,
	score ,
	decision_code_label ,
	approve_cutoff ,
	decline_cutoff ,
	model_name ,
	ROW_NUMBER () over(partition by user_id order by order_scored_at asc) as rank_asc,
	ROW_NUMBER () over(partition by user_id order by order_scored_at desc) as rank_desc
FROM 
	ods_production.order_scoring os 
),
ml_model_scores as 
(
	select * from model_score_raw where rank_asc = 1 or rank_desc = 1
)
SELECT 
c.customer_id ,
c.customer_acquisition_cohort ,
c.created_at ,
c.updated_at ,
--personal_id,
--cpii.birthdate ,
--cpii.age ,
--cpii.email ,
--split_part(cpii.email, '@', 2) AS email_domain,
c.billing_city ,
c.shipping_city ,
c.billing_zip ,
c.shipping_zip, 
--province,
--cpii.phone_number,
c.customer_type ,
c.company_name ,
o.last_order_country,
c.profile_status ,
c.crm_label ,
cl.customer_labels,
c.company_status ,
c.company_type_name ,
c.company_id ,
c.company_created_at ,
c.payment_count ,
cp.paid_payments,
cp.failed_payments,
cp.refunded_payments,
c.paid_subscriptions ,
c.refunded_subscriptions ,
cp.max_monthly_payment,
cp.amount_paid,
c.orders ,
c.completed_orders ,
c.paid_orders ,
c.declined_orders ,
c.max_submitted_order_date ,
o.max_paid_order_date,
o.min_paid_order_date,
o.first_order_submitted_date,
c.delivered_allocations ,
c.returned_allocations ,
asm.max_active_subscription_months,
c.initial_subscription_limit ,
c.subscription_limit_defined_date,
c.active_subscription_value ,
c.subscription_limit - c.active_subscription_value as total_risk,
c.committed_subscription_value ,
c.active_subscriptions ,
c.failed_subscriptions ,
c.chargeback_subscriptions ,
c.subscriptions ,
c.subscription_limit ,
total_risk/(case when c.subscription_limit= 0 then  0.0000001 else c.subscription_limit end) as limit_utilization,
cp.amount_due,
cp.outstanding_subscription_revenue,
cp.unique_payment_months,
c.start_date_of_first_subscription ,
c.first_subscription_acquisition_channel ,
c.first_subscription_duration ,
c.ever_rented_asset_purchase_price  as total_asset_value,
ad.ever_rented_residual_value_market_price,
cp.amt_failed_subscriptions,
asm.count_dc_subscriptions,
asm.asset_value_dc_subscriptions,
asm.count_returned_subscriptions,
asm.asset_value_returned_subscriptions,
c.max_cancellation_date ,
c.customer_scoring_result,
c.burgel_risk_category ,
c.fraud_type ,
c.trust_type ,
c.min_fraud_detected ,
c.max_fraud_detected ,
mls1.score as first_ml_model_score,
mls1.decision_code_label as first_ml_model_decision,
mls1.model_name as first_ml_model_name,
datediff('month',mls1.order_scored_at, current_date) as months_since_first_score,
mls2.score as current_ml_model_score,
mls2.decision_code_label as current_ml_model_decision,
mls2.model_name as current_ml_model_name,
datediff('month',mls2.order_scored_at, current_date) as months_since_current_score,
coalesce(m.merkmal_flag,0) as merkmal_flag,
c.rfm_segment ,
c.rfm_score ,
cs.id_check_type,
cs.id_check_state,
cp.max_dpd,
case when cp.max_dpd >=30 then 1 else 0 end as dpd30_flag,
case when cp.max_dpd >=60 then 1 else 0 end as dpd60_flag,
case when cp.max_dpd >=90 then 1 else 0 end as dpd90_flag,
c.clv,
CASE WHEN bas.customer_id IS NOT NULL THEN 1 ELSE 0 END AS customer_bas_verified
--seizure_cnt
--seizure_amt
--holder_name_match
--balance
--avg_disposable_income
--bas_result
FROM 
master.customer c 
left join ods_data_sensitive.customer_pii cpii 
on c.customer_id = cpii.customer_id 

	left join 
	order_data o
	on o.customer_id = c.customer_id

	left join customer_labels cl
	on cl.customer_id = c.customer_id

	left join 
	customer_payments cp
	on cp.customer_id = c.customer_id

	left join 
	asset_details ad
	on ad.customer_id = c.customer_id

	left join 
	active_sub_months asm
	on asm.customer_id = c.customer_id

	left join 
	merkmal_flag m
	on m.user_id = c.customer_id

	left join
	ods_production.customer_scoring cs
	on cs.customer_id = c.customer_id 

	left join bas
	on bas.customer_id = c.customer_id

	LEFT JOIN --first ml model score
	ml_model_scores mls1
	on mls1.customer_id = c.customer_id
	and mls1.rank_asc = 1

	LEFT JOIN --current/latest ml model score
	ml_model_scores mls2
	on mls2.customer_id = c.customer_id
	and mls2.rank_desc = 1
	

;
