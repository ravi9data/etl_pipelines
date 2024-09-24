CREATE OR REPLACE VIEW dm_marketing.v_braze_report_campaign_timeseries AS 
WITH campaign_summary_raw AS
(
----------------------------------------------------------------------------------------------------------------------------------------------
--- === IMPORTANT - Use [converted_order/influenced_order = 1] for anything related to orders to avoid counting individual orders multiple times === --- 
----------------------------------------------------------------------------------------------------------------------------------------------
SELECT
	campaign_id,
	source_type,
	signup_country,
	new_recurring,
	customer_type,
	crm_label_braze,
	DATE_TRUNC('week', sent_date)::date AS sent_week,
	--- MEDIUM STATS ---
	-- <Unique> ---
	COUNT(DISTINCT CASE WHEN is_sent > 0 THEN id_campaign_hash END) AS events_sent_unique,
	COUNT(DISTINCT CASE WHEN is_opened > 0 THEN id_campaign_hash END) AS events_opened_unique,
	COUNT(DISTINCT CASE WHEN is_clicked > 0 THEN id_campaign_hash END) AS events_clicked_unique,
	COUNT(DISTINCT CASE WHEN is_unsubscribed > 0 THEN id_campaign_hash END) AS events_unsubscribes_unique,
	COUNT(DISTINCT CASE WHEN is_bounced > 0 THEN id_campaign_hash END) AS events_bounces_unique,
	-- <Overall / Total> ---
	COUNT(DISTINCT CASE WHEN is_sent_overall > 0 THEN id_campaign_hash END) AS items_sent_total,
	COUNT(DISTINCT CASE WHEN is_opened_overall > 0 THEN id_campaign_hash END) AS items_opened_total,
	COUNT(DISTINCT CASE WHEN is_clicked_overall > 0 THEN id_campaign_hash END) AS items_clicked_total,
	--- ORDER STATS --- 
	COUNT(DISTINCT CASE WHEN influenced_order = 1 THEN order_id END) AS influenced_orders,
	COUNT(DISTINCT CASE WHEN converted_order = 1 THEN order_id END) AS converted_orders, 
	COUNT(DISTINCT CASE WHEN converted_order = 1 AND paid_order = 1 THEN order_id END) AS converted_orders_paid,
	COUNT(DISTINCT CASE WHEN converted_order = 1 AND approved_order = 1 THEN order_id END) AS converted_orders_approved, 
	--- ORDER STATS --- Revenue-based
	ROUND(COALESCE(SUM(CASE WHEN converted_order = 1 THEN total_committed_subs_value_eur END),0)) AS total_committed_subs_value_eur
FROM dm_marketing.braze_campaign_events_all
GROUP BY 1,2,3,4,5,6,7
),
customers_reached AS
(
SELECT
	campaign_id,
	source_type,
	DATE_TRUNC('week', sent_date)::date AS sent_week,
	count(DISTINCT customer_id) AS customers_reached
FROM dm_marketing.braze_campaign_events_all
WHERE COALESCE(is_delivered, is_sent) = 1
GROUP BY 1,2,3
)
, unique_data_braze AS (
	SELECT DISTINCT 
		id_campaign_hash,
		campaign_id,
		source_type,
		sent_date,
		seconds_til_first_open,
		seconds_til_first_click
	FROM dm_marketing.braze_campaign_events_all
)		
,
median_sto AS
(
SELECT 
	campaign_id, 
	source_type,
	DATE_TRUNC('week', sent_date)::date AS sent_week,
	MEDIAN(seconds_til_first_open) AS median_seconds_til_first_open,
	COUNT(seconds_til_first_open) AS median_opentime_sample_size
FROM unique_data_braze
WHERE source_type != 'In-App Message'
GROUP BY 1,2,3
),
median_stc AS
(
SELECT 
	campaign_id, 
	source_type,
	DATE_TRUNC('week', sent_date)::date AS sent_week, 
	MEDIAN(seconds_til_first_click) AS median_seconds_til_first_click,
	count(seconds_til_first_click) AS median_clicktime_sample_size
FROM unique_data_braze
WHERE source_type != 'Push Notification'
GROUP BY 1,2,3
),
campaign_length AS
(
SELECT
	campaign_id,
	DATEDIFF('day', MIN(sent_date), MAX(sent_date)) + 1 AS campaign_length_days
FROM unique_data_braze
WHERE campaign_id IS NOT NULL
GROUP BY 1
),
campaign_names AS
(
SELECT DISTINCT campaign_id, campaign_name 
FROM dm_marketing.braze_campaign_mapping bcm 
),
campaign_tags AS
(
	SELECT DISTINCT campaign_id, REPLACE(TRANSLATE(tags, '[]', ''), '"', '')::VARCHAR(100) AS campaign_tags
	FROM stg_external_apis.braze_campaign_details bcd 
)
SELECT DISTINCT
	cn.campaign_name,
	csr.*,
	cr.customers_reached, -- campaign/sourcetype/sentweek level
	ROUND(mo.median_seconds_til_first_open,1) AS median_seconds_til_first_open,
	mo.median_opentime_sample_size,
	ROUND(mc.median_seconds_til_first_click,1) AS median_seconds_til_first_click,
	mc.median_clicktime_sample_size,
	cl.campaign_length_days,
	LOWER(ct.campaign_tags) AS campaign_tags
FROM campaign_summary_raw csr
LEFT JOIN customers_reached cr
	ON cr.campaign_id = csr.campaign_id
	AND cr.source_type = csr.source_type
	AND cr.sent_week = csr.sent_week
LEFT JOIN median_sto mo
	ON mo.campaign_id = csr.campaign_id 
	AND mo.source_type = csr.source_type
	AND mo.sent_week = csr.sent_week
LEFT JOIN median_stc mc
	ON mc.campaign_id = csr.campaign_id 
	AND mc.source_type = csr.source_type
	AND mc.sent_week = csr.sent_week
LEFT JOIN campaign_names cn ON cn.campaign_id = csr.campaign_id
LEFT JOIN campaign_length cl ON cl.campaign_id = csr.campaign_id
LEFT JOIN campaign_tags ct ON ct.campaign_id = csr.campaign_id
WHERE cn.campaign_name IS NOT NULL  
WITH NO SCHEMA BINDING; 
