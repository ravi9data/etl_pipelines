CREATE OR REPLACE VIEW dm_referral.v_referral_conversion_rate AS 
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
		date_trunc('month',page_view_start_local)::date AS page_view_start,
		country,
		count(cust_id) AS total_host_visits
	FROM hosts_pre WHERE idx=1
	GROUP BY 1,2
)
,guest_base AS (
	SELECT *,date_trunc('month',page_view_start_local)::date AS page_month
	FROM page_view_base
	where page_url LIKE '%referred%' OR page_url LIKE '%referral-code/%')
, guest_pre as (
	SELECT *,
	ROW_NUMBER()OVER(PARTITION BY cust_id ORDER BY page_view_start_local ) idx FROM guest_base )
,guests AS (
SELECT
	date_trunc('month',page_view_start_local)::date AS page_view_start,
	country,
	count(cust_id) AS total_guest_visits
FROM guest_pre WHERE idx=1
GROUP BY 1,2)
,first_sub_order AS (
	SELECT 
		date_trunc('month',submitted_date)::date AS submitted_month,
		country,
		first_referral_event,
		count(DISTINCT order_id ) AS first_submitted_orders 
	FROM master_referral.loyalty_order lo 
	WHERE is_first_submitted_order_after_referral_for_ref_user=1
	AND first_referral_event='referral_guest'
	GROUP BY 1,2,3)
,first_app_order AS (
	SELECT 
		date_trunc('month',approved_date)::date AS approved_Month,
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


CREATE OR REPLACE VIEW dm_referral.v_loyalty_referrals AS(
WITH all_guests_pre AS (
SELECT host_id,count(DISTINCT guest_id) guest_count FROM master_referral.host_guest_mapping hgm 
GROUP BY 1)
, All_guests AS (
SELECT guest_count,count(host_id),'All Guests' AS order_label FROM all_guests_pre
GROUP BY 1)
, Submitted_guests_pre AS (
SELECT host_id,count(DISTINCT guest_id) guest_count FROM master_referral.host_guest_mapping hgm 
WHERE submitted_date IS NOT NULL 
GROUP BY 1)
, Submitted_guests AS (
SELECT guest_count,count(host_id),'Submitted Guests' AS order_label FROM Submitted_guests_pre
GROUP BY 1)
, Approved_guests_pre AS (
SELECT host_id,count(DISTINCT guest_id) guest_count FROM master_referral.host_guest_mapping hgm 
WHERE approved_date IS NOT NULL 
GROUP BY 1)
, Approved_guests AS (
SELECT guest_count,count(host_id),'Approved Guests' AS order_label FROM Approved_guests_pre
GROUP BY 1)
, Paid_guests_pre AS (
SELECT host_id,count(DISTINCT guest_id) guest_count FROM master_referral.host_guest_mapping hgm 
WHERE paid_date IS NOT NULL 
GROUP BY 1)
, Paid_guests AS (
SELECT guest_count,count(host_id),'Paid Guests' AS order_label FROM Paid_guests_pre
GROUP BY 1)
SELECT * FROM all_guests 
UNION
SELECT * FROM submitted_guests
UNION 
SELECT * FROM approved_guests
UNION 
SELECT * FROM paid_guests
)
WITH NO SCHEMA binding;