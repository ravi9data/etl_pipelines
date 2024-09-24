create or replace view dm_b2b.b2b_core_metrics as 
with company_info as (
		select
		distinct c2.customer_id, 
		c2.company_id, 
		c2.company_name,
		c2.created_at::date as created_at, 
		u.full_name as account_owner,
		c2.company_type_name,
		case 
			when u.full_name in ('Kimberley Truong', 'Joel Bartz', 'Gabriel Toldi') then u.full_name --inbound 
			when u.full_name in ('Stefan Ohneberg', 'Robert Perschon', 'Jordy van Mil') then u.full_name --outbound 
			when u.full_name in ('Mirella Recchiuti', 'Joshua Simon','Brian Chung') then u.full_name --international
			when u.full_name in ('Philip Nasskau', 'Roman Spinner', 'Thomas Vandermuntert', 'Andre Abranches') then u.full_name --KAM
			WHEN u.full_name IN ('Hamza Gilani') THEN u.full_name --NEW businessex
			WHEN u.full_name IN ( 'Sandra Picallo','Maria Serrano') THEN u.full_name --Europe (non DE)
			when u.full_name = 'Julian Öztoprak' then u.full_name --Grover/null_segment
		 else 'Self-service' end as account_owner_manual,
 		case 
 			when account_owner_manual = 'Self-service' then account_owner_manual
 			when u.segment = 'International' then 'US' --changing the label name
 			when account_owner_manual = 'Julian Öztoprak' then 'Grover' --adding segment name
 			else u.segment end as segment_,
			coalesce(fm.is_freelancer, 0) as is_freelancer, 
			c2.start_date_of_first_subscription::date as start_date_of_first_subscription
from master.customer c2 
	left join ods_b2b.account a 
		on c2.customer_id = a.customer_id
	left join ods_b2b."user" u
		on u.user_id = a.account_owner
	left join dm_risk.b2b_freelancer_mapping fm
		ON c2.company_type_name = fm.company_type_name
   where customer_type  = 'business_customer'
	)	
	, fact_date as (
	SELECT DISTINCT 
	 d.datum
	 ,CASE	
		 WHEN d.week_day_number = 7 THEN 1
		 WHEN d.datum = CURRENT_DATE THEN 1
		 ELSE 0 
	 END AS day_is_end_of_week,
	 CASE	
		 WHEN d.day_is_last_of_month =1 or d.datum = CURRENT_DATE-1 THEN 1 --Monthly info should be yesterday
		 ELSE 0 
	 END AS day_is_last_of_month
	 ,ci.customer_id
	 ,ci.start_date_of_first_subscription
	 ,case when ci.is_freelancer then 'Freelancer' else 'Non-freelancers' end as is_freelancer
	 ,ci.segment_ as segment
	 ,case 
			when ci.start_date_of_first_subscription is null or datum::date < ci.start_date_of_first_subscription::date then 'No orders'
			when date_Trunc('week', ci.start_date_of_first_subscription)::date = date_trunc('week',datum)::date then 'New customer'
			when date_Trunc('quarter', ci.start_date_of_first_subscription)::date = date_trunc('quarter', datum)::date then 'Upsell new'
		else 'Upsell existing'
		end as new_upsell
		FROM public.dim_dates d
			left join company_info ci 
			on datum >= ci.created_at::date 
	where datum <= CURRENT_DATE
	 AND datum >= DATE_TRUNC('MONTH', DATEADD('MONTH', -13, CURRENT_DATE))
	 )
	 , active_info as (SELECT distinct
	d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 ,d.segment
 ,d.new_upsell
 ,SUM(COALESCE(active_subscriptions,0)) AS active_subscriptions
 ,SUM(COALESCE(active_subscription_value,0)) AS active_subscription_value
 ,SUM(COALESCE(active_committed_subscription_value,0)) AS active_committed_subscription_value
  ,SUM(COALESCE(active_subscription_value,0)) * 12 AS annualized_asv
FROM fact_date d
  LEFT JOIN ods_finance.active_subscriptions_overview s
    ON d.datum = s.fact_date  
   AND d.customer_id = s.customer_id
GROUP BY 1,2,3,4,5,6,7)
, new_subs AS (
SELECT 
	d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 , d.segment
 , d.new_upsell
 ,COUNT(DISTINCT s.subscription_id) AS new_subscriptions
 ,SUM(s.subscription_value) AS new_subscription_value
 ,SUM(s.committed_sub_value + s.additional_committed_sub_value) AS committed_subscription_revenue
FROM fact_date d
LEFT JOIN master.subscription s
  ON d.datum = s.start_date::DATE
 AND d.customer_id = s.customer_id
GROUP BY 1,2,3,4,5,6,7
)
, cancellation AS (
SELECT 
  d.datum
 ,d.day_is_last_of_month
 ,d.day_is_end_of_week
 ,d.customer_id
 ,d.is_freelancer
 , d.segment
 , d.new_upsell
 ,COUNT(DISTINCT s.subscription_id) AS cancelled_subscriptions
 ,SUM(s.subscription_value) AS cancelled_subscription_value
FROM fact_date d 
  LEFT JOIN master.subscription s
    ON d.datum =  s.cancellation_date::DATE
   AND d.customer_id = s.customer_id
GROUP BY 1,2,3,4,5,6,7)
SELECT DISTINCT
  c.datum as fact_date
 ,c.day_is_last_of_month
 ,c.day_is_end_of_week
 ,c.is_freelancer
 ,c.segment
 ,c.new_upsell
 , COALESCE(SUM(active_subscriptions),0) AS active_subscriptions
 ,COALESCE(SUM(active_subscription_value),0) AS active_subscription_value
 ,COALESCE(SUM( active_committed_subscription_value),0) AS active_committed_subscription_value
  ,COALESCE(SUM(annualized_asv),0) AS annualized_asv
  ,coalesce(sum(new_subscriptions),0) as new_subscriptions
 ,coalesce(sum(new_subscription_value),0) as new_subscription_value
 ,coalesce(sum(committed_subscription_revenue),0) as committed_subscription_value_new
 ,coalesce(sum(cancelled_subscriptions),0) as cancelled_subscriptions
 ,coalesce(sum(cancelled_subscription_value),0) as cancelled_subscription_value  
FROM fact_date c
  LEFT JOIN active_info ai
    ON ai.datum = c.datum
   AND ai.customer_id = c.customer_id
  LEFT JOIN new_subs ns
    ON ns.datum = c.datum
   AND ns.customer_id = c.customer_id
    LEFT JOIN cancellation can
    ON can.datum = c.datum
   AND can.customer_id = c.customer_id
   group by 1,2,3,4,5,6
   with no schema binding;
     
