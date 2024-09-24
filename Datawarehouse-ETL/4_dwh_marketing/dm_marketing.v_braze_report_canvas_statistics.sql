
CREATE OR REPLACE VIEW dm_marketing.v_braze_report_canvas_statistics AS
WITH canvas_preaggregation AS
(
	SELECT
		canvas_id,
		canvas_name,
		canvas_variation_id,
		canvas_variation_name,
		canvas_step_id ,
		canvas_step_name,
		source_type,
		signup_country,
		new_recurring,
		customer_type,
		crm_label_braze,
		-- Communication item stats
		MIN(sent_date) AS first_sent_date,
		MAX(sent_date) AS last_sent_date,
		COUNT(DISTINCT CASE WHEN is_sent > 0 THEN id_canva_hash END) AS sent_unique,
		COUNT(DISTINCT CASE WHEN is_sent_overall > 0 THEN id_canva_hash END) AS sent_overall,
		COUNT(DISTINCT CASE WHEN is_delivered > 0 THEN id_canva_hash END) AS delivered_unique,
		COUNT(DISTINCT CASE WHEN is_delivered_overall > 0 THEN id_canva_hash END) AS delivered_overall,
		COUNT(DISTINCT CASE WHEN is_opened > 0 THEN id_canva_hash END) AS opened_unique,
		COUNT(DISTINCT CASE WHEN is_opened_overall > 0 THEN id_canva_hash END) AS opened_overall,	
		COUNT(DISTINCT CASE WHEN is_clicked > 0 THEN id_canva_hash END) AS clicked_unique,
		COUNT(DISTINCT CASE WHEN is_clicked_overall > 0 THEN id_canva_hash END) AS clicked_overall,
		COUNT(DISTINCT CASE WHEN is_unsubscribed > 0 THEN id_canva_hash END) AS unsubscribed_unique,
		COUNT(DISTINCT CASE WHEN is_unsubscribed_overall > 0 THEN id_canva_hash END) AS unsubscribed_overall,
		COUNT(DISTINCT CASE WHEN is_bounced > 0 THEN id_canva_hash END) AS bounces_unique,
		COUNT(DISTINCT CASE WHEN is_bounced_overall > 0 THEN id_canva_hash END) AS bounces_overall,
		--- ORDER STATS --- 
		COUNT(DISTINCT CASE WHEN influenced_order = 1 THEN order_id END) AS influenced_orders,
		COUNT(DISTINCT CASE WHEN converted_order = 1 THEN order_id END) AS converted_orders, 
		COUNT(DISTINCT CASE WHEN converted_order = 1 AND paid_order = 1 THEN order_id END) AS converted_orders_paid,
		COUNT(DISTINCT CASE WHEN converted_order = 1 AND approved_order = 1 THEN order_id END) AS converted_orders_approved, 
		--- ORDER STATS --- Revenue-based
		ROUND(COALESCE(SUM(CASE WHEN converted_order = 1 THEN total_committed_subs_value_eur END),0)) AS total_committed_subs_value_eur
	FROM dm_marketing.braze_canvas_events_all
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
, customers_reached AS
(
SELECT
	canvas_id,
	canvas_step_id,
	count(DISTINCT customer_id) AS customers_reached
FROM dm_marketing.braze_canvas_events_all
WHERE COALESCE(is_delivered, is_sent) = 1
GROUP BY 1,2
)
, canvas_tags AS
(
	SELECT DISTINCT canvas_id, REPLACE(TRANSLATE(lower(tags), '[]', ''), '"', '')::VARCHAR(100) AS canvas_tags
	FROM stg_external_apis.braze_canvas_details bcd
	WHERE draft = 0 -- Removes draft Canvas versions which may have different campaign tags. 
)
-- Retrieve Canvas-level Category and Product information (from order-level).
, category_counts AS 
(
	SELECT canvas_id, 
		category_name, 
		count(*) as cnt,
	    row_number() over (partition by canvas_id order by count(*) desc) as seqnum
	FROM dm_marketing.braze_canvas_events_all
	WHERE converted_order = 1
	GROUP BY canvas_id, category_name
),
category_summary AS    -- Most common ordered product category per canvas id.
(
	SELECT 
		canvas_id , 
		category_name AS main_order_category
	FROM  category_counts 
	WHERE seqnum = 1
),
product_list_counts AS
(
	SELECT 
		canvas_id, 
		product_name, 
		count(*) as cnt,
		row_number() over (partition by canvas_id order by count(*) desc) as seqnum
	FROM dm_marketing.braze_canvas_events_all
	WHERE converted_order = 1
	GROUP BY canvas_id, product_name
),
product_summary_raw AS    -- Top 3 Product Names by Order Count.
(
	SELECT
		canvas_id , 
		product_name, 
		seqnum
	FROM product_list_counts 
	WHERE seqnum <= 3
),
product_summary AS   -- Aggregating the 3 most common (most expensive) products ordered per canvas..
(
	SELECT 
		canvas_id, 
		LISTAGG(DISTINCT product_name,', ') WITHIN GROUP (ORDER BY seqnum ASC) AS product_list
	FROM product_summary_raw
	GROUP BY 1
)
SELECT
	c.*,
	COALESCE(ct.canvas_tags, '') AS canvas_tags,
	cr.customers_reached,
	CASE 
		WHEN MAX(c.last_sent_date) OVER (PARTITION BY c.canvas_id) > DATE_ADD('day', -3, CURRENT_DATE) 
			THEN 1
		ELSE 0
	END::TEXT AS is_active, -- CREATE is_active Flag TO indicate WHERE Canvas communications may be ongoing & totals unlikely TO be accurate.
	cs.main_order_category,
	ps.product_list
FROM canvas_preaggregation c
LEFT JOIN customers_reached cr -- canvas_id/step_id level
	ON cr.canvas_id = c.canvas_id
	AND cr.canvas_step_id = c.canvas_step_id
LEFT JOIN category_summary cs
	ON cs.canvas_id = c.canvas_id
LEFT JOIN product_summary ps
	ON ps.canvas_id = c.canvas_id
LEFT JOIN canvas_tags ct
	ON ct.canvas_id = c.canvas_id
WITH NO SCHEMA BINDING ; 