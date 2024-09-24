DROP TABLE IF EXISTS dm_risk.risk_daily_monitoring ;

CREATE TABLE dm_risk.risk_daily_monitoring AS 
WITH manual_review_eu_clean_up AS 
(
     SELECT 
        order_id,
        result,
        employee_id ,
        case WHEN employee_id = 628197 THEN 'Erik Victor  Jarl '
WHEN employee_id = 2209158 THEN 'Guney Cetin'
WHEN employee_id = 1341372 THEN 'Ganiyu Oladimeji'
WHEN employee_id = 1796815 THEN 'Danny Lu'
WHEN employee_id = 1341370 THEN 'Ian Linh Dao'
WHEN employee_id = 230342 THEN 'Carlo Restelli'
WHEN employee_id = 1309257 THEN 'Martina  Peloso'
WHEN employee_id = 472401 THEN 'Federico Branchetti'
WHEN employee_id = 634403 THEN 'Edwin Koomson'
WHEN employee_id = 1097300 THEN 'Madlen Ivanova'
WHEN employee_id = 1332490 THEN 'Angela Abate'
WHEN employee_id = 1541646 THEN 'Yan Yu'
WHEN employee_id = 2293554 THEN 'Elizaveta Iakovleva'
WHEN employee_id = 1493525 THEN 'Basri Öz'
WHEN employee_id = 1429908 THEN 'Alexis Ankrah'
WHEN employee_id = 1868337 THEN 'Janou'
WHEN employee_id = 2136452 THEN 'Thomas Olk'
WHEN employee_id = 480968 THEN 'Erensu Akdogan'
WHEN employee_id = 377898 THEN 'David Selasi Yaotse' else '99-Others' end as reviewer_name,
        result_reason,
        result_comment,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at, updated_at, created_date) as rowno
    FROM stg_curated.risk_internal_eu_manual_review_result_v1
    WHERE result IN ('APPROVED', 'DECLINED')
),
mr_id_trigger AS 
(select * from (
     SELECT 
        order_id,
        result,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at, updated_at, created_date) as rowno
    FROM stg_curated.risk_internal_eu_manual_review_result_v1
    WHERE result IN ('ID_VERIFICATION')) as t1 where rowno = 1
)
, manual_review_us_clean_up AS
(
	select
		order_id,
		status as decision,
		reason,
		comments as comment,
 		case WHEN created_by = 472401 THEN 'Federico Branchetti'
WHEN created_by = 1332490 THEN 'Angela Abate'
WHEN created_by = 1309257 THEN 'Martina  Peloso'
WHEN created_by = 1796815 THEN 'Danny Lu'else created_by end as reviewer_name,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at, updated_at, consumed_at) AS rowno
	from
		stg_curated.risk_internal_us_risk_manual_review_order_v1
)
, mr_final AS 
(
	select
		order_id,
		reviewer_name,
		"result" AS decision,
		result_reason AS reason,
		result_comment AS comment
	FROM manual_review_eu_clean_up
	WHERE rowno = 1
	UNION
	SELECT
		order_id,
		reviewer_name,
		decision,
		reason,
		comment
	FROM manual_review_us_clean_up us
	WHERE rowno = 1
		AND NOT EXISTS (SELECT NULL FROM manual_review_eu_clean_up eu WHERE eu.order_id = us.order_id)
),
order_dpd as(
     select sp.order_id ,
     count(1) as n_total_payment_order,
     count(distinct to_char(due_date,'YYYY-MM')) as cnt_unq_month_payment_order,
  	 min(sp.billing_period_start) as min_billing_start_order,
  	 sum(case when sp.status in ('PAID') then 1 else 0 end) as n_paid_order,
  	 sum(case when sp.status in ('FAILED', 'FAILED_FULLY', 'FAILED FULLY') then 1 else 0 end) as n_failed_order,
  	 max(case when sp.status !=  'PAID' then sp.dpd else 0 end) as max_dpd_failed_order,
  	 case when max(case when sp.status !=  'PAID' then sp.dpd else 0 end) >= 30 then 1 else 0 end as dpd30_flag_order,
  	 case when max(case when sp.status !=  'PAID' then sp.dpd else 0 end) >= 60 then 1 else 0 end as dpd60_flag_order,
  	 max(sp.dpd) as max_dpd_ever_order,
  	 max(case when sp.status != 'PAID' and payment_number = 2 then sp.dpd else 0 end) as max_dpd_failed_order_2nd,
  	 case when max(case when sp.status != 'PAID' and payment_number = 2 then sp.dpd else 0 end) >= 60 then 1 else 0 end as dpd60_flag_2nd_order,
  	 case when sum(case when payment_number = 2 and failed_date is not null then 1 else 0 end) >= 1 then 1 else 0 end as fpd_flag_ever_order,
  	 case when sum(case when payment_number = 2 and status in ('FAILED', 'FAILED FULLY', 'FAILED_FULLY') then 1 else 0 end) >= 1 then 1 else 0 end as fpd_flag_order,
	 ---second month payment calculation
  	 sum(case when payment_number = 2 then amount_due else 0 end) as amount_due_2nd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date and  payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_due_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +2 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_2dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +30 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_30dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 60 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_60dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 90 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_90dpd_order,
	 sum(case when  payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_asoftoday_order,
	 sum(case when payment_number = 2 then case when sp.asset_was_returned then sp.amount_due else sp.amount_paid end else 0 end) as amount_paid_2nd__asoftoday_asset_returns_order
from master.subscription_payment sp
where status not in ('PLANNED','HELD')
group by 1),
order_item_agg as (select order_id,
	   sum(price) as total_price,
	   avg(price) as avg_price,
	   count(1) as cnt_item,
	   sum(duration::float) as sum_duration,
       listagg(duration, ' ; ') within group (order by price) as agg_duration,	  
	   listagg(category, ' ; ') within group (order by price)as agg_categories,
	   listagg(subcategory, ' ; ') within group (order by price) as agg_subcategories,
	   listagg(name, ' ; ') within group (order by price) as agg_product_name,
	   avg(duration::float) as avg_duration,
	   max(case when duration = 24 then 1 else 0 end) as duration_24m,
	   max(case when duration = 18 then 1 else 0 end) as duration_18m,
	   max(case when duration = 12 then 1 else 0 end) as duration_12m,
	   max(case when duration = 6 then 1 else 0 end) as duration_6m,
	   max(case when duration = 3 then 1 else 0 end) as duration_3m,
	   max(case when duration = 1 then 1 else 0 end) as duration_1m,
	   max(case when category = 'POS' then 1 else 0 end) as category_pos,
		max(case when category = 'eMobility' then 1 else 0 end) as category_emobility,
		max(case when category = 'Home Entertainment' then 1 else 0 end) as category_home_entertainment,
		max(case when category = 'Drones' then 1 else 0 end) as category_drones,
		max(case when category = 'Fitness' then 1 else 0 end) as category_fitness,
		max(case when category = 'Phones & Tablets' then 1 else 0 end) as category_phones_tablets,
		max(case when category = 'Cameras' then 1 else 0 end) as category_cameras,
		max(case when category = 'Computers' then 1 else 0 end)  as category_computers,
		max(case when category = 'Smart Home' then 1 else 0 end) as category_smart_home,
		max(case when subcategory = 'Smartphones' then 1 else 0 end) as category_phone,
		sum(case when category = 'POS' then 1 else 0 end)  as cnt_category_pos,
        sum(case when category = 'eMobility' then 1 else 0 end)  as cnt_category_emobility,
        sum(case when category = 'Home Entertainment' then 1 else 0 end)  as cnt_category_home_entertainment,
        sum(case when category = 'Drones' then 1 else 0 end)  as cnt_category_drones,
        sum(case when category = 'Fitness' then 1 else 0 end)  as cnt_category_fitness,
        sum(case when category = 'Phones & Tablets' then 1 else 0 end)  as cnt_category_phones_tablets,
        sum(case when category = 'Cameras' then 1 else 0 end)  as cnt_category_cameras,
        sum(case when category = 'Computers' then 1 else 0 end)  as cnt_category_computers,
        sum(case when category = 'Smart Home' then 1 else 0 end)  as cnt_category_smart_home,
        sum(case when subcategory = 'Smartphones' then 1 else 0 end)  as cnt_subcategory_phone
from s3_spectrum_rds_dwh_order_approval.order_item oi 
group by 1),
risk_mr_comments_eu as (select
	customer_id, order_id, store_country_iso, outcome_message as mr_sending_comment_raw, decision_reason as mr_sending_reason_high_level,		case
	   	when left(outcome_message,11) = 'MR fallback' then 'MR fallback'
	   	when outcome_message like '%Overlimit%' then 'Overlimit'
	   	when left(outcome_message,18) = 'Outstanding Amount' then 'Outstanding Amount'
	   	when left(outcome_message, 60) = 'Dangerous Matches: nethone matches with active_subscriptions' then 'Dangerous Match, Nethone, ASV'
	   	when left(outcome_message, 58) = 'Dangerous Matches: nethone matches with outstanding_amount' then 'Dangerous Match, Nethone, Outstanding'
	   	when left(outcome_message, 53) = 'Dangerous Matches: nethone matches with negative_tags' then  'Dangerous Match, Nethone, Negative Tag'
	   	when left(outcome_message, 54) = 'Dangerous Matches: nethone matches with pending_orders' then 'Dangerous Match, Nethone, Pending Order'
	   	when left(outcome_message, 60) = 'Dangerous Matches: py_tank matches with active_subscriptions' then 'Dangerous Match, Pytank, ASV'
	   	when left(outcome_message, 58) = 'Dangerous Matches: py_tank matches with outstanding_amount' then 'Dangerous Match, Pytank, Outstanding'
	   	when left(outcome_message, 53) = 'Dangerous Matches: py_tank matches with negative_tags' then 'Dangerous Match, Pytank, Negative Tag'
	   	when left(outcome_message, 54) = 'Dangerous Matches: py_tank matches with pending_orders' then 'Dangerous Match, Pytank, Pending Order'
	   	when left(outcome_message, 59) = 'Dangerous Matches: cookie matches with active_subscriptions' then 'Dangerous Match, Cookie, ASV'
	   	when left(outcome_message, 52) = 'Dangerous Matches: cookie matches with negative_tags' then 'Dangerous Match, Cookie, Negative Tag'
	   	when left(outcome_message, 57) = 'Dangerous Matches: cookie matches with outstanding_amount' then 'Dangerous Match, Cookie, Outstanding'
	   	when left(outcome_message, 53) = 'Dangerous Matches: cookie matches with pending_orders' then 'Dangerous Match, Cookie, Pending Order'
	   	when left(outcome_message, 55) = 'Dangerous Matches: ip matches with active_subscriptions' then 'Dangerous Match, IP, ASV'
	   	when left(outcome_message, 53) = 'Dangerous Matches: ip matches with outstanding_amount' then 'Dangerous Match, IP, Outstanding'
	   	when left(outcome_message, 49) = 'Dangerous Matches: ip matches with pending_orders' then 'Dangerous Match, IP, Pending Order'
	   	when left(outcome_message, 48) = 'Dangerous Matches: ip matches with negative_tags' then 'Dangerous Match, IP, Negative Tag'
	   	when left(outcome_message, 58) = 'Dangerous Matches: phone matches with active_subscriptions' then  'Dangerous Match, Phone, ASV'
	   	when left(outcome_message, 51) = 'Dangerous Matches: phone matches with negative_tags' then 'Dangerous Match, Phone, Negative Tag'
	   	when left(outcome_message, 52) = 'Dangerous Matches: phone matches with pending_orders' then 'Dangerous Match, Phone, Pending Order'
	   	when left(outcome_message, 56) = 'Dangerous Matches: phone matches with outstanding_amount' then 'Dangerous Match, Phone, Outstanding'
	   	when left(outcome_message, 28) = 'Unsopported Shipping Country' then 'Unsopported Shipping Country'
	   	when left(outcome_message, 35) = 'Dangerous Matches: ssn matches with' then 'Dangerous Match, National ID'
	  	else outcome_message end as mr_comment_detailed,
	  	case
	   	when left(outcome_message,11) = 'MR fallback' then 'MR fallback'
	   	when outcome_message like '%Overlimit%' then 'Overlimit'
	   	when left(outcome_message,18) = 'Outstanding Amount' then 'Outstanding Amount'
	   	when left(outcome_message, 17) = 'Dangerous Matches' then 'Dangerous Match'
	   	when left(outcome_message, 28) = 'Unsopported Shipping Country' then 'Unsopported Shipping Country'
	  	else outcome_message end as mr_comment_short,
	  		   case
	   	when left(outcome_message, 18) = 'Outstanding Amount' 
	   	then 
	   	 case when outcome_message = 'Outstanding Amount' then -99999
	   	 else LEFT(RIGHT(outcome_message,LENGTH(outcome_message)-20),LENGTH(RIGHT(outcome_message,LENGTH(outcome_message)-20))-1)::Float end else null end as outstanding_amount
from
	(
	select
		customer_id, order_id, store_country_iso, outcome_namespace, outcome_message, decision_reason,
		row_number() over (partition by oa.order_id  order by oa.updated_at asc) as row_no
	from
		stg_curated.risk_eu_order_decision_intermediate_v1 oa
	where outcome_namespace = 'MANUAL_REVIEW' 
	) as t1
where
	row_no = 1) ,
eu_order_decision_final as (
select * from (
select *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at) as rowno from s3_spectrum_kafka_topics_raw.risk_eu_order_decision_final_v1
) as t1 where rowno = 1),
us_order_decision_compact as (
select * from (
select *, ROW_NUMBER() OVER (PARTITION BY order_id  ORDER BY created_at) as rowno from stg_curated.risk_us_order_decision_compact_v1
) as t1 where rowno = 1),
address_info as (
select * from (
select *, ROW_NUMBER() OVER (PARTITION BY order_number  ORDER BY created_at) as rowno from staging.kafka_order_placed_v2
) as t1 where rowno = 1)
select o.order_id as order_id_key,
       o.customer_id as customer_id_key,
       date_trunc('month', o.submitted_date)::date AS submitted_month,
       date_trunc('week', o.submitted_date)::date AS submitted_week,
       1 as submitted_flag,
	   case when o.status = 'CANCELLED' then 1 else 0 end as cancellation_flag,
	   case when o.status = 'MANUAL REVIEW' then 1 else 0 end as mr_flag,
	   case when o.status = 'PENDING APPROVAL' then 1 else 0 end as pending_approval_flag,
	   case when o.status in ('PAID', 'FAILED FIRST PAYMENT') then 1 else 0 end as approved_flag,
	   case when o.status = 'DECLINED' then 1 else 0 end as declined_flag,
	   case when o.status = 'FAILED FIRST PAYMENT' then 1 else 0 end as declined_flag,
	   case when o.status = 'PAID' then 1 else 0 end as paid_flag,
       o.paid_date,
       o.store_id,
       o.submitted_date,
       o.status,
       o.cancellation_reason,
       o.declined_reason,
       o.order_value,
       o.voucher_code,
       o.voucher_type,
       o.new_recurring,
       o.new_recurring_risk,
       o.retention_group,
       o.marketing_channel,
       o.marketing_campaign,
       o.device,
       o.store_country,
       o.store_label,
       o.customer_type,
       o.scoring_decision,
       o.scoring_reason,
       o.payment_method,
       case when mf.order_id is null then 1 else 0 end as automation_flag_v1, --from pure MR final decision, excluding MR ID trigger cases
       case when rmc.order_id is null then 1 else 0 end as automation_flag_v2, ---also contains MR id verification cases
       case when mit.order_id is not null then 1 else 0 end as mr_id_verification_trigger_flag,
       case when os.onfido_trigger is not null then 1 else 0 end as onfido_trigger_flag,
       rmc.mr_sending_reason_high_level,
       kop.shipping_address_country_iso,
       kop.shipping_address_city,
       kop.shipping_address_zipcode,
       COALESCE(reodfv.decision_reason,ruodcv.decision_reason) as engine_decline_reason,
       mf.*,
       od.*,
       cl.label,
       oia.*,
       rmc.*,
       ceiling((current_date-su.birthdate)/365.25) as age,
       os.credit_limit, 
       os.scoring_model_id, 
       os.score as ml_score, 
       os.onfido_trigger, 
       os.verification_state,
       epos.es_experian_score_rating,
	   epos.es_experian_score_value ,
	   epos.es_equifax_score_rating,
	   epos.es_equifax_score_value ,
	   epos.nl_focum_score_rating ,
	   epos.nl_focum_score_value ,
	   epos.nl_experian_score_rating ,
	   epos.nl_experian_score_value ,
	   epos.de_schufa_score_rating ,
	   epos.de_schufa_score_value ,
	   epos.at_crif_score_rating ,
	   epos.at_crif_score_value ,
	   epos.us_fico_score_rating ,
	   epos.us_fico_score_value ,
	   epos.eu_ekata_identity_risk_score ,
	   epos.eu_ekata_identity_network_score ,
	     case 
        when store_label = 'Grover - Germany online' then epos.de_schufa_score_value 
       	when store_label = 'Grover - Austria online' then epos.at_crif_score_value 
       	when store_label = 'Grover - Spain online' then epos.es_equifax_score_value  
       	when store_label = 'Grover - Netherlands online' then epos.nl_focum_score_value 
       	when store_label = 'Grover - United States online' then  epos.us_fico_score_value  
       	else null end as country_cb_score,
	   epos.global_seon_fraud_score 
from master.order o
left join mr_final mf on mf.order_id = o.order_id 
left join order_dpd od on od.order_id = o.order_id
left join order_item_agg oia on oia.order_id = o.order_id
left join risk_mr_comments_eu rmc on rmc.order_id = o.order_id
left join mr_id_trigger mit on mit.order_id = o.order_id
left join ods_data_sensitive.external_provider_order_score epos on epos.order_id = o.order_id
left join ods_production.order_scoring os on os.order_id = o.order_id
left join s3_spectrum_rds_dwh_order_approval.customer_labels3 cl on cl.customer_id = o.customer_id 
left join stg_api_production.spree_users su on su.id = o.customer_id 
left join address_info kop on kop.order_number = o.order_id
left join us_order_decision_compact ruodcv on ruodcv.order_id = o.order_id 
left join eu_order_decision_final reodfv on reodfv.order_id = o.order_id 
where o.status not in ('CART', 'ADDRESS') and o.submitted_date::date >= '2022-01-01'
