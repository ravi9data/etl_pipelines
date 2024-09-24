DROP TABLE IF EXISTS master_referral.loyalty_customer;
CREATE TABLE master_referral.loyalty_customer AS 
WITH src AS (
	SELECT 
			host_id::int AS customer_id,
			'referral_host' AS event_name,
			host_referred_date::timestamp AS event_timestamp,
			host_country AS country
	FROM master_referral.host_guest_mapping hgm 
	UNION 
	SELECT 
			guest_id::int AS customer_id,
			'referral_guest' AS event_name,
			guest_created_date::timestamp AS event_timestamp,
			guest_country  AS country
	FROM master_referral.host_guest_mapping hgm 
),src_idx AS (
	SELECT *,ROW_NUMBER()OVER (PARTITION BY customer_id ORDER BY event_timestamp) idx
	FROM src
	)-- WHERE customer_id='1940717'
,Loyalty_customer AS (
	SELECT 
	customer_id,
	event_timestamp as first_event_timestamp,
	event_name as first_event_name,
	 country AS country,
	count(DISTINCT hg.guest_id) AS number_of_referral
		FROM src_idx s
		LEFT JOIN master_referral.host_guest_mapping hg
		ON hg.host_id=s.customer_id
	WHERE idx=1
	GROUP BY 1,2,3,4
)
,active_subsciption_value AS(
	SELECT
		fact_date,
		customer_id,
		COALESCE(sum(active_subscriptions), 0) AS active_subscriptions_before_referral,
		COALESCE(sum(active_subscription_value), 0) AS active_subscription_value_before_referral,
		COALESCE(sum(active_committed_subscription_value), 0) AS active_committed_subscription_value_before_referral
	FROM
		ods_finance.active_subscriptions_overview aso
	WHERE
		fact_date >= '2022-01-01'
		--and customer_id='99042'
	GROUP BY
		1,2)
, subscriptions as (
	select 
		s.customer_id,
		first_subscription_start_date,
		first_event_timestamp,
		s.subscriptions_per_customer,
		round(Avg(rental_period),2) AS avg_referral_rental_period,
		max(CASE WHEN cancellation_date<first_event_timestamp THEN cancellation_date END ) AS max_cancellation_date_before_referral,
		min(CASE WHEN start_date::Date>=first_event_timestamp::date THEN start_date END ) AS first_subscription_start_date_after_referral,
		round(count(DISTINCT s.subscription_id ),2) as  total_subscriptions,
		round(count(DISTINCT case when s.start_date<first_event_timestamp then s.subscription_id end),2) as  total_subscription_before_referral,
		round(count(DISTINCT case when s.start_date>first_event_timestamp then s.subscription_id end),2) as  total_subscriptions_after_referral,
		round(count(case when  status ='ACTIVE' then s.subscription_id end),2) AS active_subscriptions,
		round(count(case when s.start_date>first_event_timestamp and status ='ACTIVE' then s.subscription_id end),2) AS active_subscriptions_after_referral,
		round(sum(DISTINCT s.subscription_value ),2) as  total_subscription_value,
		round(sum(case when s.start_date<first_event_timestamp then s.subscription_value end ),2) as  total_subscription_value_before_referral,
		round(sum(case when s.start_date>first_event_timestamp then s.subscription_value end),2) as  total_subscription_value_after_referral,
		round(sum(case when  status ='ACTIVE' then s.subscription_value end),2) AS active_subscription_value,
		round(sum(case when status ='ACTIVE' AND s.start_date>first_event_timestamp then s.subscription_value end),2) AS active_subscription_value_after_referral
	FROM (
	SELECT
		customer_id,
		subscription_value_euro AS subscription_value,
		subscriptions_per_customer,
		subscription_id,
		rental_period,
		cancellation_date,
		committed_sub_value,
		start_date,
		status,
			FIRST_VALUE (start_date) OVER (PARTITION BY customer_id
	ORDER BY
		start_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as first_subscription_start_date
FROM
	ods_production.subscription) s 
	LEFT JOIN loyalty_customer lc 
	ON s.customer_id=lc.customer_id
	WHERE first_event_timestamp IS NOT NULL 
	--AND lc.customer_id='903826'
	group by 1,2,3,4
)
,orders_pre AS (
	SELECT 
		o.customer_id,
		o.order_id,
		o.created_date,
		o.approved_date,
		o.submitted_date,
		o.paid_date,
		c.first_event_name,
		c.first_event_timestamp
		,ROW_NUMBER()OVER(PARTITION BY c.customer_id ORDER BY o.created_date) AS rank_created_order
	    ,CASE WHEN submitted_date IS NOT NULL THEN ROW_NUMBER()OVER(PARTITION BY c.customer_id ORDER BY submitted_Date) END AS rank_submitted_order
	    ,CASE WHEN paid_date IS NOT NULL THEN ROW_NUMBER()OVER(PARTITION BY c.customer_id ORDER BY paid_date) END AS rank_paid_order
	    ,CASE WHEN o.created_date::Date>=first_event_timestamp::Date
	    	THEN ROW_NUMBER()OVER(PARTITION BY 
	    				CASE WHEN o.created_date::Date>=first_event_timestamp::Date
	    							THEN c.customer_id ELSE 0 end ORDER BY created_date) 
	    	end
	    AS rank_created_order_after_referral
	   ,CASE WHEN o.submitted_date::DAte>=first_event_timestamp::DAte
	    	THEN ROW_NUMBER()OVER(PARTITION BY 
	    				CASE WHEN o.submitted_date::DAte>=first_event_timestamp::DAte
	    							THEN c.customer_id ELSE 0 end ORDER BY o.submitted_date) 
	    	end
	    AS rank_submitted_order_referral
	   ,CASE WHEN o.approved_date::Date>=first_event_timestamp::Date
	    	THEN ROW_NUMBER()OVER(PARTITION BY 
	    				CASE WHEN o.approved_date::Date>=first_event_timestamp::Date
	    							THEN c.customer_id ELSE 0 end ORDER BY approved_date) 
	    	end
	    AS rank_approved_order_after_referral
	   ,CASE WHEN o.paid_date::DAte>=first_event_timestamp::DAte
	    	THEN ROW_NUMBER()OVER(PARTITION BY 
	    				CASE WHEN o.paid_date::DAte>=first_event_timestamp::DAte
	    							THEN c.customer_id ELSE 0 end ORDER BY o.paid_date) 
	    	end
	    AS rank_paid_order_referral
	FROM ods_production.ORDER o
	left JOIN Loyalty_customer c 
	ON c.customer_id=o.customer_id 
)
, orders AS (
	SELECT 
		DISTINCT 
			o.customer_id,
			min(CASE WHEN rank_submitted_order_referral=1 THEN order_id END ) as first_submitted_order_after_referral,
			min(CASE WHEN rank_submitted_order_referral=1 THEN submitted_date END ) as first_submitted_order_date_after_referral,
			min(CASE WHEN rank_created_order_after_referral=1 THEN order_id END ) as first_created_order_after_referral,
			min(CASE WHEN rank_created_order_after_referral=1 THEN created_date END ) as first_created_order_date_after_referral,
			min(CASE WHEN rank_paid_order_referral=1 THEN order_id END ) as first_paid_order_after_referral,
			min(CASE WHEN rank_paid_order_referral=1 THEN paid_date END ) as first_paid_order_date_after_referral
		FROM orders_pre o
		GROUP BY 1
	 	)
SELECT 
	c.customer_id,
	c.created_at AS user_created_date,
	lc.first_event_timestamp AS first_referral_event,
	lc.first_event_name,
	c.customer_type,
	country,
	number_of_referral,
	--first_created_order_after_referral,
	--first_submitted_order_after_referral,
	--first_paid_order_after_referral,
	first_created_order_date_after_referral,
	first_submitted_order_date_after_referral,
	first_paid_order_date_after_referral,
	--customer_journey,
	first_subscription_start_date,
	r.created_at AS guest_item_returned_date,
	max_cancellation_date_before_referral AS max_cancellation_date_before_referral,
	first_subscription_start_date_after_referral AS first_subscription_start_date_after_referral,
	CASE
		WHEN first_subscription_start_date::date >= first_referral_event::Date
		AND lc.first_event_name = 'referral_guest' THEN TRUE
	END AS new_user,
	avg_referral_rental_period AS avg_card_rental_period,
	--total sub before/after
	COALESCE(total_subscriptions,0) as total_subscriptions ,
	COALESCE(total_subscription_before_referral,0) as  total_subscription_before_referral,
	COALESCE(total_subscriptions_after_referral,0) as  total_subscriptions_after_referral,
	--active sub before/after
	COALESCE(active_subscriptions,0) as active_subscriptions,
	COALESCE(asv.active_subscriptions_before_referral,0) AS active_subscriptions_before_referral,
	COALESCE(active_subscriptions_after_referral,0) AS active_subscriptions_after_referral ,
	----total SV before/after
	COALESCE(total_subscription_value,0) as  total_subscription_value,
	COALESCE(total_subscription_value_before_referral,0) as  total_subscription_value_before_referral,
	COALESCE(total_subscription_value_after_referral,0) as  total_subscription_value_after_referral,
	--ASV before/after
	COALESCE(active_subscription_value,0) as  active_subscription_value,
	COALESCE(asv.active_subscription_value_before_referral,0) AS active_subscription_value_before_referral,
	COALESCE(active_subscription_value_after_referral,0) AS active_subscription_value_after_referral,
	--total CSV before/after
	--COALESCE(acquired_committed_referral_subscription_value,0) as  acquired_committed_referral_subscription_value
	--Active CSV before/after
	--COALESCE(active_committed_subscription_value_before_referral,0) AS active_committed_subscription_value_before_referral,
	--COALESCE(active_committed_referral_subscription_value,0) as  active_committed_referral_subscription_value
	CASE WHEN cc.customer_id IS NOT NULL THEN TRUE ELSE FALSE END is_card_user
FROM
	ods_production.customer c
LEFT JOIN loyalty_customer lc 
	ON lc.customer_id = c.customer_id
LEFT JOIN active_subsciption_value asv
	ON lc.customer_id = asv.customer_id
	AND lc.first_event_timestamp::date = asv.fact_date
LEFT JOIN subscriptions s 
	ON lc.customer_id = s.customer_id
LEFT JOIN stg_curated.referral_eu_guest_item_returned_v1 r 
	ON c.customer_id=r.customer_id::int
	on c.customer_id=cc.customer_id
LEFT JOIN orders o 
	ON c.customer_id=o.customer_id
WHERE
	  lc.first_event_name IS NOT NULL 
	AND customer_type='normal_customer';
    
GRANT SELECT ON master_referral.loyalty_customer TO tableau;
