drop table if exists dm_risk.marketing_metrics_risk;
CREATE TABLE dm_risk.marketing_metrics_risk  AS
with orders as
(
select 
	o.order_id,
	COALESCE(o.store_country,'n/a') AS country,
	o.submitted_date ,
	o.new_recurring ,
	o.approved_date ,
	o.burgel_risk_category ,
	o.status ,
	o.schufa_class ,
	o.customer_id ,
	o.customer_type ,
	o.voucher_code ,
	o.voucher_discount ,
	o.voucher_value ,
	o.order_value ,
	o.declined_reason ,
	o.marketing_campaign ,
	o.marketing_channel ,
	o.paid_date 
FROM 
	master.order o
WHERE 
	o.submitted_date > DATE_ADD('year', -3, CURRENT_DATE)
),
order_marketing_channel as
(
select
	m.order_id ,
	m.marketing_channel ,
	m.marketing_medium ,
	m.marketing_campaign ,
	m.devicecategory ,
m.marketing_source 
FROM 
	ods_production.order_marketing_channel m
),
payments as
(
select 
	sp.order_id ,
	count(sp.subscription_id) as n_subs,
	max(case when sp.payment_number = 2 then 1 else 0 end) as n_2_payments,
	max(case when sp.payment_number = 2 and sp.failed_date is not null then 1 else 0 end) as fl_fpd,
	sum(case when sp.due_date <= current_date then sp.amount_due else 0 end) as amount_due,
	sum(case when sp.due_date <= current_date then sp.amount_paid else 0 end) as amount_paid,
	max(sp.dpd) as max_dpd_order,
	max(case when sp.dpd > 30 then 1 else 0 end) as max_dpd_30_ever,
	max(case when sp.dpd > 60 then 1 else 0 end) as max_dpd_60_ever,
	max(case when sp.dpd > 90 then 1 else 0 end) as max_dpd_90_ever
FROM 
	master.subscription_payment sp 
WHERE status != 'PLANNED'
	AND sp.due_date BETWEEN DATE_ADD('year', -3, CURRENT_DATE) AND current_date 
group by 
	sp.order_id
),
score_distribution as 
(
select 
	*
from 
	dm_risk.v_score_distribution_report vsdr 
),
bank_account_snapshot AS 
(
SELECT 
	DISTINCT customer_id -- Note that this includes ALL customers who underwent verification (passed OR failed)
FROM 
	s3_spectrum_rds_dwh_order_approval.decision_tree_algorithm_results
WHERE 
	additional_reason = 'BAS service'
),
first_touchpoint_order as
(
select
	order_id,
	first_touchpoint_30d,
	last_touchpoint_excl_direct,
	first_touchpoint_30d_mkt_source,
	first_touchpoint_mkt_medium
from 
	traffic.order_conversions
)
select
	o.order_id,
	o.country,
	o.submitted_date ,
	o.burgel_risk_category ,
	o.status ,
	o.schufa_class ,
	o.customer_id ,
	o.voucher_code ,
	o.voucher_discount ,
	o.voucher_value ,
	o.order_value ,
	o.declined_reason ,
	CASE 
	     	WHEN bas.customer_id IS NOT NULL 
	     		THEN 'BAS Verified' -- Includes all those who underwent BAS verification (passed or failed).
	     	ELSE 'Not BAS Verified' 
	     END AS customer_bas_verified,
	COALESCE(o.marketing_campaign , om.marketing_campaign) as marketing_campaign,
	COALESCE(o.marketing_channel , om.marketing_channel) as marketing_channel,
	om.marketing_medium ,
	om.devicecategory ,
	om.marketing_source ,
	o.paid_date ,
	o.approved_date ,
	p.n_2_payments,
	p.fl_fpd,
	p.amount_due,
	p.amount_paid,
	p.max_dpd_order,
	p.max_dpd_30_ever,
	p.max_dpd_60_ever,
	p.max_dpd_90_ever,
	p.n_subs,
	s.new_recurring_risk,
	o.new_recurring ,
	s.customer_type,
	s.total_orders,
	s.order_rank,
	s.completed_orders,
	s.order_journey_mapping_risk,
	s.is_approved,
	s.is_cancelled,
	s.is_paid,
	s.store_country,
	s.scoring_decision,
	s.es_experian_score_rating,
	s.es_experian_score_value,
	s.es_experian_avg_pd,
	s.es_equifax_score_rating,
	s.es_equifax_score_value,
	s.es_equifax_avg_pd,
	s.de_schufa_score_rating,
	s.de_schufa_score_value,
	s.de_schufa_avg_pd,
	s.at_crif_score_rating,
	s.at_crif_score_value,
	s.at_crif_avg_pd,
	s.nl_focum_score_rating,
	s.nl_focum_score_value,
	s.nl_focum_avg_pd,
	s.nl_experian_score_rating,
	s.nl_experian_score_value,
	s.nl_experian_avg_pd,
	s.us_precise_score_rating,
	s.us_precise_score_value,
	s.us_precise_avg_pd,
	s.us_fico_score_rating,
	s.us_fico_score_value,
	s.us_clarity_score_value,
	s.us_clarity_score_rating,
	s.us_vantage_score_value,
	s.us_vantage_score_rating,
	s.model_score,
	s.model_name,
	s.approve_cutoff,
	s.decline_cutoff,
	first_touchpoint_30d,
	last_touchpoint_excl_direct,
	first_touchpoint_30d_mkt_source,
	first_touchpoint_mkt_medium
FROM 
orders o
	left join
	order_marketing_channel om 
	on om.order_id = o.order_id

	left join 
	payments p
	on p.order_id = o.order_id
	
	left join 
	score_distribution s
	on s.order_id = o.order_id
	
	LEFT JOIN 
	bank_account_snapshot bas 
	ON o.customer_id = bas.customer_id 
	
	left join 
	first_touchpoint_order f
	on f.order_id = o.order_id
;

GRANT SELECT ON dm_risk.marketing_metrics_risk TO tableau;
