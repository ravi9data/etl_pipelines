CREATE OR REPLACE VIEW dm_marketing.v_braze_report_campaign_statistics AS 
-- Retrieve Campaign-level Category and Product information (from order-level).
WITH category_counts AS 
(
	SELECT campaign_id, 
		category_name, 
		count(*) as cnt,
	    row_number() over (partition by campaign_id order by count(*) desc) as seqnum
	FROM dm_marketing.braze_campaign_events_all
	WHERE converted_order = 1
	GROUP BY campaign_id, category_name
),
category_summary AS    -- Most common ordered product category per campaign id. (Note: Redshift doesn't have a MODE function, this is the work-around)
(
	SELECT 
		campaign_id , 
		category_name AS main_order_category
	FROM  category_counts 
	WHERE seqnum = 1
),
product_list_counts AS
(
	SELECT 
		campaign_id, 
		product_name, 
		count(*) as cnt,
		row_number() over (partition by campaign_id order by count(*) desc) as seqnum
	FROM dm_marketing.braze_campaign_events_all
	WHERE converted_order = 1
	GROUP BY campaign_id, product_name
),
product_summary_raw AS    -- Top 3 Product Names by Order Count.
(
	SELECT
		campaign_id , 
		product_name, 
		seqnum
	FROM product_list_counts 
	WHERE seqnum <= 3
),
product_summary AS   -- Aggregating the 3 most common (most expensive) products ordered per campaign.
(
	SELECT 
		campaign_id, 
		LISTAGG(DISTINCT product_name,',') WITHIN GROUP (ORDER BY seqnum ASC) AS product_list
	FROM product_summary_raw
	GROUP BY 1
),
campaign_summary_raw AS
(
----------------------------------------------------------------------------------------------------------------------------------------------
--- === IMPORTANT - Use [converted_order/influenced_order = 1] for anything related to orders to avoid counting individual order id's multiple times === --- 
----------------------------------------------------------------------------------------------------------------------------------------------
	SELECT
		campaign_id,
		signup_country,
		new_recurring,
		source_type,
		customer_type,
		crm_label_braze,
		os,
		--- CAMPAIGN DATES ---
		MIN(sent_date)::date AS sent_date_first,
		MAX(sent_date)::date AS sent_date_last,
		--- MEDIUM STATS ---
		-- <Unique> ---
		COUNT(DISTINCT CASE WHEN is_sent > 0 THEN id_campaign_hash END) AS items_sent_unique,
		COUNT(DISTINCT CASE WHEN is_delivered > 0 THEN id_campaign_hash END) AS items_delivered_unique,
		COUNT(DISTINCT CASE WHEN is_opened > 0 THEN id_campaign_hash END) AS items_opened_unique,
		COUNT(DISTINCT CASE WHEN is_clicked > 0 THEN id_campaign_hash END) AS items_clicked_unique,
		COUNT(DISTINCT CASE WHEN is_unsubscribed > 0 THEN id_campaign_hash END) AS items_unsubscribes_unique,
		COUNT(DISTINCT CASE WHEN is_bounced > 0 THEN id_campaign_hash END) AS items_bounces_unique,
		-- <Overall / Total> ---
		COUNT(DISTINCT CASE WHEN is_sent_overall > 0 THEN id_campaign_hash END) AS items_sent_overall,
		COUNT(DISTINCT CASE WHEN is_delivered_overall > 0 THEN id_campaign_hash END) AS items_delivered_overall,
		COUNT(DISTINCT CASE WHEN is_opened_overall > 0 THEN id_campaign_hash END) AS items_opened_overall,
		COUNT(DISTINCT CASE WHEN is_clicked_overall > 0 THEN id_campaign_hash END) AS items_clicked_overall,
		COUNT(DISTINCT CASE WHEN is_unsubscribed_overall > 0 THEN id_campaign_hash END) AS items_unsubscribes_overall,
		COUNT(DISTINCT CASE WHEN is_bounced_overall > 0 THEN id_campaign_hash END) AS items_bounces_overall,
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
	count(DISTINCT customer_id) AS customers_reached
FROM dm_marketing.braze_campaign_events_all
WHERE COALESCE(is_delivered, is_sent) = 1
GROUP BY 1,2
)
, unique_data_braze AS (
	SELECT DISTINCT 
		id_campaign_hash,
		campaign_id,
		source_type,
		seconds_til_first_open,
		seconds_til_first_click
	FROM dm_marketing.braze_campaign_events_all
),	
median_sto AS
(
	SELECT 
		campaign_id, 
		source_type, 
		MEDIAN(seconds_til_first_open) AS median_seconds_til_first_open	
	FROM unique_data_braze
	GROUP BY 1,2
),
median_stc AS
(
	SELECT 
		campaign_id, 
		source_type, 
		MEDIAN(seconds_til_first_click) AS median_seconds_til_first_click	
	FROM unique_data_braze
	GROUP BY 1,2
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
	cr.customers_reached, -- campaign/sourcetype level
	msto.median_seconds_til_first_open,
	mstc.median_seconds_til_first_click,
	cs.main_order_category,
	ps.product_list,
	LOWER(ct.campaign_tags) AS campaign_tags
FROM campaign_summary_raw csr
LEFT JOIN customers_reached cr
	ON cr.campaign_id = csr.campaign_id
	AND cr.source_type = csr.source_type
LEFT JOIN median_sto msto 
	ON msto.campaign_id = csr.campaign_id
	AND msto.source_type = csr.source_type
LEFT JOIN median_stc mstc 
	ON mstc.campaign_id = csr.campaign_id
	AND mstc.source_type = csr.source_type
LEFT JOIN category_summary cs
	ON cs.campaign_id = csr.campaign_id
LEFT JOIN product_summary ps
	ON ps.campaign_id = csr.campaign_id
LEFT JOIN campaign_names cn
	ON cn.campaign_id = csr.campaign_id
LEFT JOIN campaign_tags ct
	ON ct.campaign_id = csr.campaign_id
WHERE cn.campaign_name IS NOT NULL
WITH NO SCHEMA BINDING; 
