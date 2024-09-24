truncate  dm_risk.risk_daily_monitoring;

insert into dm_risk.risk_daily_monitoring
WITH manual_review_eu_clean_up AS 
(
     SELECT 
        order_id,
        result,
        employee_id ,
        su.first_name || ' ' || su.last_name as reviewer_name,
        result_reason,
        result_comment,
        ROW_NUMBER() OVER (PARTITION BY mrr.order_id ORDER BY mrr.created_at, mrr.updated_at DESC, consumed_at DESC) as rowno
    FROM s3_spectrum_kafka_topics_raw.risk_internal_eu_manual_review_result_v1 mrr
    left join stg_api_production.spree_users su on su.id = mrr.employee_id::float
    WHERE result IN ('APPROVED', 'DECLINED')
),
mr_id_trigger AS
(select * from (
     SELECT
        order_id,
        result,
        employee_id as employee_id_idv,
        su.first_name || ' ' || su.last_name as reviewer_name_idv,
        ROW_NUMBER() OVER (PARTITION BY mrr2.order_id ORDER BY mrr2.created_at, mrr2.updated_at DESC, consumed_at DESC) as rowno
    FROM s3_spectrum_kafka_topics_raw.risk_internal_eu_manual_review_result_v1 mrr2
    left join stg_api_production.spree_users su on su.id = mrr2.employee_id::float
    WHERE result IN ('ID_VERIFICATION')) as t1 where rowno = 1
)
, manual_review_us_clean_up AS
(
	select
		order_id,
		mro.status as decision,
		reason,
		comments as comment,
		created_by as employee_id,
 		case WHEN created_by = 472401 THEN 'Federico Branchetti'
			WHEN created_by = 1332490 THEN 'Angela Abate'
			WHEN created_by = 1309257 THEN 'Martina  Peloso'
			WHEN created_by = 1796815 THEN 'Danny Lu'else created_by end as reviewer_name,
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY mro.created_at, mro.updated_at DESC, consumed_at DESC) AS rowno
	from
		stg_curated.risk_internal_us_risk_manual_review_order_v1 mro
	left join stg_api_production.spree_users su on su.id = mro.created_by
)
, mr_final AS
(
	select
		order_id,
		reviewer_name,
		"result" AS decision,
		employee_id,
		result_reason AS reason,
		result_comment AS comment
	FROM manual_review_eu_clean_up
	WHERE rowno = 1
	UNION
	SELECT
		order_id,
		reviewer_name,
		decision,
		employee_id,
		reason,
		comment
	FROM manual_review_us_clean_up us
	WHERE rowno = 1
		AND NOT EXISTS (SELECT NULL FROM manual_review_eu_clean_up eu WHERE eu.order_id = us.order_id)
),
order_dpd as(
     select sp.order_id as order_id_dpd,
     count(1) as n_total_payment_order,
     count(distinct to_char(due_date,'YYYY-MM')) as cnt_unq_month_payment_order,
  	 min(sp.billing_period_start) as min_billing_start_order,
  	 sum(case when sp.status in ('PAID') then 1 else 0 end) as n_paid_order,
  	 sum(case when sp.status in ('FAILED', 'FAILED FULLY') then 1 else 0 end) as n_failed_order,
  	 max(case when sp.status !=  'PAID' then sp.dpd else 0 end) as max_dpd_failed_order,
  	 case when max(case when sp.status in ('FAILED', 'FAILED FULLY') then sp.dpd else 0 end) >= 30 then 1 else 0 end as dpd30_flag_order,
  	 case when max(case when sp.status in ('FAILED', 'FAILED FULLY') then sp.dpd else 0 end) >= 60 then 1 else 0 end as dpd60_flag_order,
  	 max(sp.dpd) as max_dpd_ever_order,
  	 max(case when sp.status in ('FAILED', 'FAILED FULLY') and payment_number = 2 then sp.dpd else 0 end) as max_dpd_failed_order_2nd,
  	 case when max(case when sp.status in ('FAILED', 'FAILED FULLY') and payment_number = 2 then sp.dpd else 0 end) >= 60 then 1 else 0 end as dpd60_flag_2nd_order,
	   case when max(case when sp.status in ('FAILED', 'FAILED FULLY') and payment_number::float in (2,3) then sp.dpd else 0 end) >= 60 then 1 else 0 end as dpd60_flag_cum_3rd_order,
	   case when max(case when sp.status in ('FAILED', 'FAILED FULLY') and payment_number::float in (2,3,4) then sp.dpd else 0 end) >= 60 then 1 else 0 end as dpd60_flag_cum_4th_order,
	   case when max(case when sp.status in ('FAILED', 'FAILED FULLY') and payment_number::float in (2,3,4,5,6) then sp.dpd else 0 end) >= 60 then 1 else 0 end as dpd60_flag_cum_6th_order,
  	 case when sum(case when payment_number = 2 and failed_date is not null then 1 else 0 end) >= 1 then 1 else 0 end as fpd_flag_ever_order,
  	 case when sum(case when payment_number = 2 and status in ('FAILED', 'FAILED FULLY') then 1 else 0 end) >= 1 then 1 else 0 end as fpd_flag_order,
	 ---second month payment calculation
	 min(case when payment_number = 2 then datediff(days, due_date , current_date) else null end) as days_since_2nd_due, ---to determine if the payment is matured to specific dpd
  	 sum(case when payment_number = 2 then amount_due else 0 end) as amount_due_2nd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date and  payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_due_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +2 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_2dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +10 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_10dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +30 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_30dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 60 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_60dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 90 and payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_90dpd_order,
	 sum(case when  payment_number = 2 then sp.amount_paid else 0 end) as amount_paid_2nd_asoftoday_order,
	 sum(case when payment_number = 2 then case when sp.asset_was_returned then sp.amount_due else sp.amount_paid end else 0 end) as amount_paid_2nd_asoftoday_asset_returns_order,
	 ---cumulative payment calculation 2-3M
	 sum(case when payment_number::float in (2,3) then amount_due else 0 end) as amount_due_cum_3rd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date and  payment_number::float in (2,3) then sp.amount_paid else 0 end) as amount_paid_cum_3rd_due_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +2 and payment_number::float in (2,3) then sp.amount_paid else 0 end) as amount_paid_cum_3rd_2dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +10 and payment_number::float in (2,3) then sp.amount_paid else 0 end) as amount_paid_cum_3rd_10dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +30 and payment_number::float in (2,3) then sp.amount_paid else 0 end) as amount_paid_cum_3rd_30dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 60 and payment_number::float in (2,3) then sp.amount_paid else 0 end) as amount_paid_cum_3rd_60dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 90 and payment_number::float in (2,3) then sp.amount_paid else 0 end) as amount_paid_cum_3rd_90dpd_order,
	 sum(case when  payment_number::float in (2,3) then sp.amount_paid else 0 end) as amount_paid_cum_3rd_asoftoday_order,
	 sum(case when payment_number::float in (2,3) then case when sp.asset_was_returned then sp.amount_due else sp.amount_paid end else 0 end) as amount_paid_cum_3rd_asoftoday_asset_returns_order,
	 ---cumulative payment calculation 2-4M
	 sum(case when payment_number::float in (2,3,4) then amount_due else 0 end) as amount_due_cum_4th_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date and  payment_number::float in (2,3,4) then sp.amount_paid else 0 end) as amount_paid_cum_4th_due_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +2 and payment_number::float in (2,3,4) then sp.amount_paid else 0 end) as amount_paid_cum_4th_2dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +10 and payment_number::float in (2,3,4) then sp.amount_paid else 0 end) as amount_paid_cum_4th_10dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +30 and payment_number::float in (2,3,4) then sp.amount_paid else 0 end) as amount_paid_cum_4th_30dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 60 and payment_number::float in (2,3,4) then sp.amount_paid else 0 end) as amount_paid_cum_4th_60dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 90 and payment_number::float in (2,3,4) then sp.amount_paid else 0 end) as amount_paid_cum_4th_90dpd_order,
	 sum(case when  payment_number::float in (2,3,4) then sp.amount_paid else 0 end) as amount_paid_cum_4th_asoftoday_order,
	 sum(case when payment_number::float in (2,3,4) then case when sp.asset_was_returned then sp.amount_due else sp.amount_paid end else 0 end) as amount_paid_cum_4th_asoftoday_asset_returns_order,
	 ---cumulative payment calculation 2-6M
	 sum(case when payment_number::float in (2,3,4,5,6) then amount_due else 0 end) as amount_due_cum_6th_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date and  payment_number::float in (2,3,4,5,6) then sp.amount_paid else 0 end) as amount_paid_cum_6th_due_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +2 and payment_number::float in (2,3,4,5,6) then sp.amount_paid else 0 end) as amount_paid_cum_6th_2dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +10 and payment_number::float in (2,3,4,5,6) then sp.amount_paid else 0 end) as amount_paid_cum_6th_10dpd_order,
     sum(case when  sp.paid_date::date <= sp.due_date::date +30 and payment_number::float in (2,3,4,5,6) then sp.amount_paid else 0 end) as amount_paid_cum_6th_30dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 60 and payment_number::float in (2,3,4,5,6) then sp.amount_paid else 0 end) as amount_paid_cum_6th_60dpd_order,
	 sum(case when  sp.paid_date::date <= sp.due_date::date + 90 and payment_number::float in (2,3,4,5,6) then sp.amount_paid else 0 end) as amount_paid_cum_6th_90dpd_order,
	 sum(case when  payment_number::float in (2,3,4,5,6) then sp.amount_paid else 0 end) as amount_paid_cum_6th_asoftoday_order,
	 sum(case when payment_number::float in (2,3,4,5,6) then case when sp.asset_was_returned then sp.amount_due else sp.amount_paid end else 0 end) as amount_paid_cum_6th_asoftoday_asset_returns_order
from master.subscription_payment sp
where status not in ('PLANNED','HELD')
and sp.due_date <= current_date
group by 1),
----
order_item_agg as (select order_id as order_id_item_agg,
	   sum(total_price) as total_price,
	   avg(total_price) as avg_price,
	   sum(quantity) as cnt_item,
	   sum(plan_duration::float) as sum_duration,
       listagg(plan_duration, ' ; ') within group (order by price) as agg_duration,
	   listagg(category_name, ' ; ') within group (order by price)as agg_categories,
	   listagg(subcategory_name, ' ; ') within group (order by price) as agg_subcategories,
	   listagg(product_name , ' ; ') within group (order by price) as agg_product_name,
	   avg(plan_duration::float) as avg_duration,
	   max(plan_duration::float) as max_duration,
	   max(case when plan_duration = 24 then 1 else 0 end) as duration_24m,
	   max(case when plan_duration = 18 then 1 else 0 end) as duration_18m,
	   max(case when plan_duration = 12 then 1 else 0 end) as duration_12m,
	   max(case when plan_duration = 6 then 1 else 0 end) as duration_6m,
	   max(case when plan_duration = 3 then 1 else 0 end) as duration_3m,
	   max(case when plan_duration = 1 then 1 else 0 end) as duration_1m,
	   max(case when category_name = 'Audio & Music' then 1 else 0 end) as category_audio_music,
	   max(case when category_name = 'Cameras' then 1 else 0 end) as category_cameras,
	   max(case when category_name = 'Computers' then 1 else 0 end) as category_computers,
	   max(case when category_name = 'Drones' then 1 else 0 end) as category_drones,
	   max(case when category_name = 'eMobility' then 1 else 0 end) as category_emobility,
	   max(case when category_name = 'Fitness' then 1 else 0 end) as category_fitness,
	   max(case when category_name = 'Gaming & VR' then 1 else 0 end) as category_gaming_vr,
	   max(case when category_name = 'MDM Add-Ons' then 1 else 0 end) as category_mdm_add_ons,
	   max(case when category_name = 'Phones & Tablets' then 1 else 0 end) as category_phones_tablets,
	   max(case when category_name = 'POS' then 1 else 0 end) as category_pos,
	   max(case when category_name = 'Smart Home' then 1 else 0 end) as category_smart_home,
	   max(case when category_name = 'TV and Projectors' then 1 else 0 end) as category_tv_and_projectors,
	   max(case when category_name = 'Wearables' then 1 else 0 end) as category_wearables,
	   max(case when subcategory_name = 'Smartphones' then 1 else 0 end) as subcategory_smartphones,
	   max(case when subcategory_name = 'Gaming Computers' then 1 else 0 end) as subcategory_gaming_computers,
	   max(case when subcategory_name = 'Laptops' then 1 else 0 end) as subcategory_laptops,
	   max(case when subcategory_name = 'TV' then 1 else 0 end) as subcategory_tv,
	   max(case when subcategory_name = 'Virtual Reality' then 1 else 0 end) as subcategory_virtual_reality,
	   max(case when subcategory_name = 'Gaming Consoles' then 1 else 0 end) as subcategory_gaming_consoles,
	   sum(case when category_name = 'Audio & Music' then quantity else 0 end) as cnt_category_audio_music,
	   sum(case when category_name = 'Cameras' then quantity else 0 end) as cnt_category_cameras,
	   sum(case when category_name = 'Computers' then quantity else 0 end) as cnt_category_computers,
	   sum(case when category_name = 'Drones' then quantity else 0 end) as cnt_category_drones,
	   sum(case when category_name = 'eMobility' then quantity else 0 end) as cnt_category_emobility,
	   sum(case when category_name = 'Fitness' then quantity else 0 end) as cnt_category_fitness,
	   sum(case when category_name = 'Gaming & VR' then quantity else 0 end) as cnt_category_gaming_vr,
	   sum(case when category_name = 'MDM Add-Ons' then quantity else 0 end) as cnt_category_mdm_add_ons,
	   sum(case when category_name = 'Phones & Tablets' then quantity else 0 end) as cnt_category_phones_tablets,
	   sum(case when category_name = 'POS' then quantity else 0 end) as cnt_category_pos,
	   sum(case when category_name = 'Smart Home' then quantity else 0 end) as cnt_category_smart_home,
	   sum(case when category_name = 'TV and Projectors' then quantity else 0 end) as cnt_category_tv_and_projectors,
	   sum(case when category_name = 'Wearables' then quantity else 0 end) as cnt_category_wearables,
	   sum(case when subcategory_name = 'Smartphones' then quantity else 0 end) as cnt_subcategory_smartphones,
	   sum(case when subcategory_name = 'Gaming Computers' then quantity else 0 end) as cnt_subcategory_gaming_computers,
	   sum(case when subcategory_name = 'Laptops' then quantity else 0 end) as cnt_subcategory_laptops,
	   sum(case when subcategory_name = 'TV' then quantity else 0 end) as cnt_subcategory_tv,
	   sum(case when subcategory_name = 'Virtual Reality' then quantity else 0 end) as cnt_subcategory_virtual_reality,
	   sum(case when subcategory_name = 'Gaming Consoles' then quantity else 0 end) as cnt_subcategory_gaming_consoles
from ods_production.order_item oi
group by 1),
---
order_subs as (
select order_id as order_id_subs,
	   count(1) as cnt_subscriptions,
	   min(first_asset_delivery_date) as min_asset_delivery_date,
	   min(first_subscription_start_date::date) as cust_first_subs_start_date,
	   sum(case when cancellation_reason_new = 'FAILED DELIVERY' then 1 else 0 end) as cnt_subs_failed_delivery,
	   sum(case when cancellation_reason_new in ('SOLD 1-EUR', 'SOLD EARLY') then 1 else 0 end) as cnt_subs_sold,
	   sum(delivered_assets) as sum_delivered_assets,
	   sum(returned_assets) as sum_returned_assets,
	   sum(outstanding_assets) as sum_outstanding_assets,
	   case when sum_delivered_assets > 0 AND sum_delivered_assets = sum_returned_assets then 1 else 0 end as asset_all_returned,
	   sum(committed_sub_value) as sum_committed_sub_value,
	   sum(avg_asset_purchase_price) as sum_avg_asset_purc_price,
	   max(case when datediff(days, start_date , cancellation_date) <= 15 then 1 else 0 end) as flag_cancelled_in_15days,
	   sum(case when datediff(days, start_date , cancellation_date) <= 15 then 1 else 0 end) as sum_cancelled_in_15days
from master.subscription s
group by 1
),
---
risk_mr_comments_eu as (select
	customer_id, order_id as risk_mr_order_id, store_country_iso, outcome_message as mr_sending_comment_raw, decision_reason as mr_decision_sending_reason_high_level,
	case
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
	row_no = 1),
eu_order_decision_final as
(
select *,
	ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY created_at) as rowno
from stg_curated.stg_risk_eu_order_decision_final_v1
)
,
us_order_decision_compact as
(
select *,
	ROW_NUMBER() OVER (PARTITION BY order_id  ORDER BY created_at) as rowno
from stg_curated.risk_us_order_decision_compact_v1
)
, address_info as
(
select *,
	ROW_NUMBER() OVER (PARTITION BY order_number  ORDER BY created_at) as rowno
from staging.kafka_order_placed_v2
)
select ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.submitted_date asc) as rowno,
       o.order_id as order_id_key,
       o.customer_id as customer_id_key,
       date_trunc('month', o.submitted_date)::date AS submitted_month,
       date_trunc('week', o.submitted_date)::date AS submitted_week,
       CASE WHEN so."number" IS NOT NULL THEN 1 ELSE 0 END AS is_old_infra_order,
       1 as submitted_flag,
	   case when o.status = 'CANCELLED' then 1 else 0 end as cancellation_flag,
	   case when o.status = 'MANUAL REVIEW' then 1 else 0 end as mr_flag,
	   case when o.status = 'PENDING APPROVAL' then 1 else 0 end as pending_approval_flag,
	   CASE WHEN oj.order_journey_mapping_risk IN ('Paid', 'Pending Payment', 'Post Approval Cancellations', 'FFP')
		THEN 1 ELSE 0 END AS approved_flag,
	   CASE WHEN oj.order_journey_mapping_risk IN ('Declined', 'Declined - BAS/Onfido related')
		THEN 1 ELSE 0 END AS declined_flag, -- This mapping is equivalent TO Status = Declined. But good TO use same SOURCE.
	   case when o.status = 'FAILED FIRST PAYMENT' then 1 else 0 end as first_failed_payment_flag,
	   case when o.status = 'PAID' then 1 else 0 end as paid_flag,
       o.paid_date::timestamp AS paid_date,
       o.store_id,
       o.submitted_date::timestamp AS submitted_date,
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
       case when rmc.risk_mr_order_id is null then 1 else 0 end as automation_flag_v2, ---also contains MR id verification cases
       case when mit.order_id is not null then 1 else 0 end as mr_id_verification_trigger_flag,
       case when os.onfido_trigger is not null then 1 else 0 end as onfido_trigger_flag,
	   mit.employee_id_idv,
	   mit.reviewer_name_idv,
       kop.shipping_address_country_iso,
       kop.shipping_address_city,
       kop.shipping_address_zipcode,
       COALESCE(reodfv.decision_reason,ruodcv.decision_reason) as engine_decline_reason,
       mf.order_id,
	   mf.reviewer_name,
	   mf.employee_id,
	   mf.decision,
	   mf.reason,
	   mf.comment,
      od.*,
       cl.label,
       oia.*,
       os2.*,
       rmc.*,
       case when su.birthdate is null then null else ceiling((current_date-su.birthdate)/365.25) end as age,
       os.credit_limit,
       os.scoring_model_id,
       os.score as ml_score,
       case
       	 when os.score is null then '98- NULL'
	     WHEN os.score <= 0.05 then '01- (0,0.05]'
	WHEN os.score <= 0.10 then '02- (0.05,0.10]'
	WHEN os.score <= 0.15 then '03- (0.10,0.15]'
	WHEN os.score <= 0.20 then '04- (0.15,0.20]'
	WHEN os.score <= 0.25 then '05- (0.20,0.25]'
	WHEN os.score <= 0.30 then '06- (0.25,0.30]'
	WHEN os.score <= 0.35 then '07- (0.30,0.35]'
	WHEN os.score <= 0.40 then '08- (0.35,0.40]'
	WHEN os.score <= 0.45 then '09- (0.40,0.45]'
	WHEN os.score <= 0.50 then '10- (0.45,0.50]'
	WHEN os.score <= 0.55 then '11- (0.50,0.55]'
	WHEN os.score <= 0.60 then '12- (0.55,0.60]'
	WHEN os.score <= 0.65 then '13- (0.60,0.65]'
	WHEN os.score <= 0.70 then '14- (0.65,0.70]'
	WHEN os.score <= 0.75 then '15- (0.70,0.75]'
	WHEN os.score <= 0.80 then '16- (0.75,0.80]'
	WHEN os.score <= 0.85 then '17- (0.80,0.85]'
	WHEN os.score <= 0.90 then '18- (0.85,0.90]'
	WHEN os.score <= 0.95 then '19- (0.90,0.95]'
	WHEN os.score > 0.95 then '20- (0.95,1]'
	else '99- Error' end as ml_bucket1,
	  case
	 when os.score is null then '98- NULL'
	 WHEN os.score <= 0.10 then '01- (0,0.10]'
	WHEN os.score <= 0.20 then '02- (0.10,0.20]'
	WHEN os.score <= 0.30 then '03- (0.20,0.30]'
	WHEN os.score <= 0.40 then '04- (0.30,0.40]'
	WHEN os.score <= 0.50 then '05- (0.40,0.50]'
	WHEN os.score <= 0.60 then '06- (0.50,0.60]'
	WHEN os.score <= 0.70 then '07- (0.60,0.70]'
	WHEN os.score <= 0.80 then '08- (0.70,0.80]'
	WHEN os.score <= 0.90 then '09- (0.80,0.90]'
	WHEN os.score > 0.9 then '10- (0.90,1]'
	  else '99- Error' end as ml_bucket2,
       os.onfido_trigger,
       os.verification_state,
       epos.es_delphi_id ,
       epos.es_experian_score_rating,
	   epos.es_experian_score_value ,
	   epos.es_equifax_id ,
	   epos.es_equifax_score_rating,
	   epos.es_equifax_score_value ,
	   epos.nl_focum_id ,
	   epos.nl_focum_score_rating ,
	   epos.nl_focum_score_value ,
	   epos.nl_experian_id ,
	   epos.nl_experian_score_rating ,
	   epos.nl_experian_score_value ,
	   epos.de_schufa_id ,
	   epos.de_schufa_is_person_known ,
	   epos.de_schufa_score_rating ,
	   epos.de_schufa_score_value ,
	   epos.at_crif_id ,
	   epos.at_crif_score_rating ,
	   epos.at_crif_score_value ,
	   epos.us_fico_score_rating ,
	   epos.us_fico_score_value ,
	   epos.eu_ekata_id ,
	   epos.eu_ekata_identity_risk_score ,
	   case when epos.eu_ekata_identity_risk_score is null then '98- NULL'
	   WHEN epos.eu_ekata_identity_risk_score <= 50 then '01- (0,50]'
		WHEN epos.eu_ekata_identity_risk_score <= 100 then '02- (50,100]'
		WHEN epos.eu_ekata_identity_risk_score <= 150 then '03- (100,150]'
		WHEN epos.eu_ekata_identity_risk_score <= 200 then '04- (150,200]'
		WHEN epos.eu_ekata_identity_risk_score <= 250 then '05- (200,250]'
		WHEN epos.eu_ekata_identity_risk_score <= 300 then '06- (250,300]'
		WHEN epos.eu_ekata_identity_risk_score <= 350 then '07- (300,350]'
		WHEN epos.eu_ekata_identity_risk_score <= 400 then '08- (350,400]'
		WHEN epos.eu_ekata_identity_risk_score <= 450 then '09- (400,450]'
		WHEN epos.eu_ekata_identity_risk_score > 450 then '10- (450,500]'
	  else '99- Error' end as ekata_idrisk_bucket,
	   epos.eu_ekata_identity_network_score ,
	   case
	   when epos.eu_ekata_identity_network_score is null then '98- NULL'
	   WHEN epos.eu_ekata_identity_network_score <= 0.05 then '01- (0,0.05]'
		WHEN epos.eu_ekata_identity_network_score <= 0.10 then '02- (0.05,0.10]'
		WHEN epos.eu_ekata_identity_network_score <= 0.15 then '03- (0.10,0.15]'
		WHEN epos.eu_ekata_identity_network_score <= 0.20 then '04- (0.15,0.20]'
		WHEN epos.eu_ekata_identity_network_score <= 0.25 then '05- (0.20,0.25]'
		WHEN epos.eu_ekata_identity_network_score <= 0.30 then '06- (0.25,0.30]'
		WHEN epos.eu_ekata_identity_network_score <= 0.35 then '07- (0.30,0.35]'
		WHEN epos.eu_ekata_identity_network_score <= 0.40 then '08- (0.35,0.40]'
		WHEN epos.eu_ekata_identity_network_score <= 0.45 then '09- (0.40,0.45]'
		WHEN epos.eu_ekata_identity_network_score <= 0.50 then '10- (0.45,0.50]'
		WHEN epos.eu_ekata_identity_network_score <= 0.55 then '11- (0.50,0.55]'
		WHEN epos.eu_ekata_identity_network_score <= 0.60 then '12- (0.55,0.60]'
		WHEN epos.eu_ekata_identity_network_score <= 0.65 then '13- (0.60,0.65]'
		WHEN epos.eu_ekata_identity_network_score <= 0.70 then '14- (0.65,0.70]'
		WHEN epos.eu_ekata_identity_network_score <= 0.75 then '15- (0.70,0.75]'
		WHEN epos.eu_ekata_identity_network_score <= 0.80 then '16- (0.75,0.80]'
		WHEN epos.eu_ekata_identity_network_score <= 0.85 then '17- (0.80,0.85]'
		WHEN epos.eu_ekata_identity_network_score <= 0.90 then '18- (0.85,0.90]'
		WHEN epos.eu_ekata_identity_network_score <= 0.95 then '19- (0.90,0.95]'
		WHEN epos.eu_ekata_identity_network_score > 0.95 then '20- (0.95,1]'
		else '99- Error' end as ekata_idnetwork_bucket1,
		case when epos.eu_ekata_identity_network_score is null then '98- NULL'
		WHEN epos.eu_ekata_identity_network_score <= 0.10 then '01- (0,0.10]'
		WHEN epos.eu_ekata_identity_network_score <= 0.20 then '02- (0.10,0.20]'
		WHEN epos.eu_ekata_identity_network_score <= 0.30 then '03- (0.20,0.30]'
		WHEN epos.eu_ekata_identity_network_score <= 0.40 then '04- (0.30,0.40]'
		WHEN epos.eu_ekata_identity_network_score <= 0.50 then '05- (0.40,0.50]'
		WHEN epos.eu_ekata_identity_network_score <= 0.60 then '06- (0.50,0.60]'
		WHEN epos.eu_ekata_identity_network_score <= 0.70 then '07- (0.60,0.70]'
		WHEN epos.eu_ekata_identity_network_score <= 0.80 then '08- (0.70,0.80]'
		WHEN epos.eu_ekata_identity_network_score <= 0.90 then '09- (0.80,0.90]'
		WHEN epos.eu_ekata_identity_network_score > 0.9 then '10- (0.90,1]'
		else '99- Error' end as ekata_idnetwork_bucket2,
	     case
        when store_label = 'Grover - Germany online' then epos.de_schufa_score_value
       	when store_label = 'Grover - Austria online' then epos.at_crif_score_value
       	when store_label = 'Grover - Spain online' then epos.es_equifax_score_value
       	when store_label = 'Grover - Netherlands online' then epos.nl_focum_score_value
       	when store_label = 'Grover - United States online' then  epos.us_fico_score_value
       	else null end as country_cb_score,
       	epos.global_seon_id ,
	   epos.global_seon_fraud_score ,
	   case when epos.global_seon_fraud_score is null then '98-NULL'
WHEN epos.global_seon_fraud_score  is null then 'NULL'
WHEN epos.global_seon_fraud_score  <= 1 THEN '<=01'
WHEN epos.global_seon_fraud_score  <= 2 THEN '<=02'
WHEN epos.global_seon_fraud_score  <= 3 THEN '<=03'
WHEN epos.global_seon_fraud_score  <= 4 THEN '<=04'
WHEN epos.global_seon_fraud_score  <= 5 THEN '<=05'
WHEN epos.global_seon_fraud_score  <= 6 THEN '<=06'
WHEN epos.global_seon_fraud_score  <= 7 THEN '<=07'
WHEN epos.global_seon_fraud_score  <= 8 THEN '<=08'
WHEN epos.global_seon_fraud_score  <= 9 THEN '<=09'
WHEN epos.global_seon_fraud_score  <= 10 THEN '<=10'
WHEN epos.global_seon_fraud_score  <= 11 THEN '<=11'
WHEN epos.global_seon_fraud_score  <= 12 THEN '<=12'
WHEN epos.global_seon_fraud_score  <= 13 THEN '<=13'
WHEN epos.global_seon_fraud_score  <= 14 THEN '<=14'
WHEN epos.global_seon_fraud_score  <= 15 THEN '<=15'
WHEN epos.global_seon_fraud_score  > 15 THEN '>15' else '99- Error' end as seon_fraud_bucket
from master.order o
left join mr_final mf on mf.order_id = o.order_id
left join order_dpd od on od.order_id_dpd = o.order_id
left join order_item_agg oia on oia.order_id_item_agg = o.order_id
left join order_subs os2 on os2.order_id_subs = o.order_id
left join risk_mr_comments_eu rmc on rmc.risk_mr_order_id = o.order_id
left join mr_id_trigger mit on mit.order_id = o.order_id
left join ods_data_sensitive.external_provider_order_score epos on epos.order_id = o.order_id
left join ods_production.order_scoring os on os.order_id = o.order_id
left join s3_spectrum_rds_dwh_order_approval.customer_labels3 cl on cl.customer_id = o.customer_id
left join stg_api_production.spree_users su on su.id = o.customer_id
left join address_info kop on kop.order_number = o.order_id AND kop.rowno = 1
left join us_order_decision_compact ruodcv on ruodcv.order_id = o.order_id AND ruodcv.rowno = 1
left join eu_order_decision_final reodfv on reodfv.order_id = o.order_id AND reodfv.rowno = 1
LEFT JOIN ods_production.order_journey oj ON oj.order_id = o.order_id
LEFT JOIN stg_api_production.spree_orders so ON so."number" = o.order_id
where o.status not in ('CART', 'ADDRESS') and o.submitted_date::date >= '2022-01-01';

grant select on dm_risk.risk_daily_monitoring to tableau;
