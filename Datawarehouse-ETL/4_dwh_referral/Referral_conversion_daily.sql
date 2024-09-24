CREATE OR REPLACE VIEW dm_referral.v_referral_conversion_rate_daily AS 
WITH page_view_base AS (
	SELECT 
		DISTINCT 
		anonymous_id AS cust_id,
		page_view_start_local,
		page_view_end_local,
		page_url,
		store_name AS country,
		device
	FROM traffic.page_views pv 
	--where  left(customer_id,4) ~ '^[0-9]{4}' OR customer_id IS nu
	)
,hosts_base AS (
	SELECT 
		* 
	FROM page_view_base
		where page_url LIKE '%referrals' OR page_url LIKE '%referral-code')
, hosts_pre as (
	SELECT 
		*,
		ROW_NUMBER()OVER(PARTITION BY cust_id ORDER BY page_view_start_local ) idx 
	FROM hosts_base )
,hosts AS (
	SELECT 
		date_trunc('day',page_view_start_local)::date AS page_view_start,
		country,
		count(cust_id) AS total_host_visits
	FROM hosts_pre WHERE idx=1
	GROUP BY 1,2
)
,guest_base AS (
	SELECT *,date_trunc('day',page_view_start_local)::date AS page_month
	FROM page_view_base
	where page_url LIKE '%referred%' OR page_url LIKE '%referral-code/%')
, guest_pre as (
	SELECT *,
	ROW_NUMBER()OVER(PARTITION BY cust_id ORDER BY page_view_start_local ) idx FROM guest_base )
,guests AS (
SELECT
	date_trunc('day',page_view_start_local)::date AS page_view_start,
	country,
	count(cust_id) AS total_guest_visits
FROM guest_pre WHERE idx=1
GROUP BY 1,2)
,first_sub_order AS (
	SELECT 
		date_trunc('day',submitted_date)::date AS submitted_month,
		country,
		first_referral_event,
		count(DISTINCT order_id ) AS first_submitted_orders 
	FROM master_referral.loyalty_order lo 
	WHERE is_first_submitted_order_after_referral_for_ref_user=1
	AND first_referral_event='referral_guest'
	GROUP BY 1,2,3)
,first_app_order AS (
	SELECT 
		date_trunc('day',approved_date)::date AS approved_Month,
		country,
		first_referral_event,
		count(DISTINCT order_id ) AS first_Approved_orders
		FROM master_referral.loyalty_order lo 
	WHERE is_first_approved_order_after_referral_for_ref_user =1 
	AND first_referral_event='referral_guest'
	GROUP BY 1,2,3)
SELECT 
	h.page_view_start,
	h.country,
	Coalesce(h.total_host_visits,0) AS total_host_visits,
	Coalesce(g.total_guest_visits,0) AS total_guest_visits,
	Coalesce(first_submitted_orders,0) AS first_submitted_orders,
	Coalesce(first_Approved_orders,0) AS  first_Approved_orders
FROM hosts h
LEFT JOIN guests g
	ON h.page_view_start=g.page_view_start
	AND h.country=g.country
LEFT JOIN first_sub_order fso 
	ON h.page_view_start=fso.submitted_month
	AND h.country=fso.country
LEFT JOIN first_app_order Fao 
	ON h.page_view_start=fao.approved_month
	AND h.country=fao.country
WHERE h.country NOT IN ('Media Markt','iRobot','Gravis Online','Conrad','Saturn')
WITH NO SCHEMA BINDING;