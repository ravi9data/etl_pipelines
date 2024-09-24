drop table if exists dm_risk.order_datamart_v1;
create table dm_risk.order_datamart_v1 as
with payments as(
select 
order_id ,
listagg(distinct sp.payment_method_detailed,',') as  payment_method_detailed,
max(sp.dpd ) as max_dpd_order,
max(case when dpd > 30 then 1 else 0 end) as max_dpd30,
max(case when dpd > 60 then 1 else 0 end) as max_dpd60,
max(case when dpd > 90 then 1 else 0 end) as max_dpd90
FROM 
master.subscription_payment sp 
group by 1
),
subscription as(
select 
s.order_id ,
sum(s.outstanding_asset_value ) as outstanding_asset_value,
sum(case when status = 'ACTIVE' then s.subscription_value_euro end) as active_subscription_value,
sum(s.returned_assets) as returned_assets,
sum(s.subscription_revenue_due) as subscription_revenue_due,
sum(s.outstanding_subscription_revenue) as outstanding_subscription_revenue,
sum(s.subscription_revenue_paid) as subscription_revenue_paid,
sum(sa.outstanding_purchase_price) as outstanding_purchase_price,
sum(sa.outstanding_residual_asset_value) as outstanding_residual_asset_value
from 
master.subscription s 
left join
ods_production.subscription_assets sa 
on s.subscription_id = sa.subscription_id 
group by 1
),
order_scoring as (
select 
os.order_id ,
o2.customer_id ,
case 
	when os.decision_code_label like 'MR%' then 'Manual'
	when os.decision_code_label like 'Model%' then 'Model'
	else os.decision_code_label
end as manual_vs_auto,
os.decision_code_label as mr_auto_result,
os.onfido_trigger as onfido_asked_flag,
case when os.onfido_trigger is  not null then os.verification_state end as onfido_verification_state,
CASE WHEN os.order_scoring_comments ILIKE '%Merkmal%' THEN 'Merkmal' ELSE 'Non-Merkmal' END AS merkmal_flag,
max(CASE WHEN os.order_scoring_comments ILIKE '%Merkmal%' THEN 1 ELSE 0 end) over (partition by o2.customer_id) as ever_merkmal_flag
from 
ods_production.order_scoring os 
left join
master."order" o2 
on os.order_id = o2.order_id 
),
bas_flag as(
SELECT 
DISTINCT 
customer_id 
FROM stg_realtimebrandenburg.decision_tree_algorithm_results 
WHERE main_reason IN ('rules failed #BAS', 'rules approved #BAS')
),
asset_brands as 
(			
select 
	order_id , 
	listagg(distinct brand, ',') within group (order by brand) as order_asset_brands
	from 
ods_production.order_item o 
group by 1
)
SELECT 
o.customer_id ,
o.new_recurring ,
o.customer_type ,
o.store_id ,
o.store_type ,
o.store_name ,
o.store_label ,
o.store_short ,
o.store_country ,
o.order_id ,
o.order_journey ,
o.order_value ,
o.initial_scoring_decision ,
o.scoring_decision ,
o.scoring_reason ,
o.declined_reason ,
o.cancellation_reason ,
o.order_item_count ,
o.created_date ,
o.submitted_date ,
o.updated_date ,
o.approved_date ,
o.canceled_date ,
o.acquisition_date ,
o.status ,
o.voucher_code,
o.voucher_type,
o.voucher_value,
o.voucher_discount,
o.is_in_salesforce,
o.paid_date ,
o.marketing_channel ,
o.avg_plan_duration ,
o.current_subscription_limit ,
o.burgel_risk_category ,
o.schufa_class ,
p.payment_method_detailed ,
p.max_dpd_order,
s.outstanding_subscription_revenue as amount_overdue,
s.subscription_revenue_paid as amount_paid,
s.subscription_revenue_due as potential_amount,
p.max_dpd30,
p.max_dpd60,
p.max_dpd90,
s.outstanding_asset_value,
os.manual_vs_auto,
os.mr_auto_result,
s.active_subscription_value as total_risk,
(case when s.active_subscription_value is null then 0 end)/(case when o.current_subscription_limit is null then -999999999 end)  as limit_utilization,
os.onfido_asked_flag,
os.onfido_verification_state,
os.merkmal_flag,
os.ever_merkmal_flag,
case when b.customer_id is not null then 1 else 0 end as bas_flag,
s.returned_assets,
s.outstanding_purchase_price,
s.outstanding_residual_asset_value,
a.order_asset_brands
FROM 
master."order" o 
	left join
	payments p
	on p.order_id = o.order_id
		left join 
		subscription s
		on s.order_id = o.order_id
			left join 
			order_scoring os 
			on os.order_id = o.order_id
				left join 
				bas_flag b
				on b.customer_id = o.customer_id
					left join 
					asset_brands a
					on a.order_id = o.order_id
					;

