CREATE OR REPLACE VIEW dm_marketing.v_braze_report_productsku_summary AS
WITH submitted_order_items AS
(
SELECT
	oi.order_id,
	oi.product_sku,
	SUM(quantity) AS number_subscriptions
FROM ods_production.order_item oi
LEFT JOIN master."order" o ON o.order_id = oi.order_id
WHERE o.submitted_date IS NOT NULL 
GROUP BY 1,2
),
product_summary_grouped_campaign AS (
SELECT
	'Campaign' AS type_event,
	f.campaign_id AS campaign_canvas_id,
	oi.product_sku,
	f.signup_country,
	f.new_recurring,
	f.customer_type,
	f.crm_label_braze,
	DATE_TRUNC('week', f.sent_date)::date AS sent_week,
	COUNT(DISTINCT f.order_id) AS num_orders,
	SUM(oi.number_subscriptions) AS num_subscriptions
FROM dm_marketing.braze_campaign_events_all f
LEFT JOIN submitted_order_items oi USING(order_id)
WHERE campaign_id IS NOT NULL 
	AND product_sku IS NOT NULL
	AND converted_order = 1
GROUP BY 1,2,3,4,5,6,7,8
),
product_summary_grouped_canvas AS (
	SELECT 
		'Canvas' AS type_event,
		b.canvas_id  AS campaign_canvas_id,
		oi.product_sku,
		b.signup_country,
		b.new_recurring,
		b.customer_type,
		b.crm_label_braze,
		DATE_TRUNC('week', b.sent_date)::date AS sent_week,
		COUNT(DISTINCT b.order_id) AS num_orders,
		SUM(oi.number_subscriptions) AS num_subscriptions
	FROM dm_marketing.braze_canvas_events_all b
	LEFT JOIN submitted_order_items oi USING(order_id)
	WHERE b.converted_order = 1
		AND b.canvas_id IS NOT NULL 
		AND oi.product_sku IS NOT NULL
	GROUP BY 1,2,3,4,5,6,7,8
),
product_summary_grouped AS (
SELECT * FROM product_summary_grouped_canvas
UNION ALL 
SELECT * FROM product_summary_grouped_campaign
),
campaign_names AS
(
SELECT DISTINCT campaign_id, campaign_name 
FROM dm_marketing.braze_campaign_mapping bcm 
),
canvas_name_prep AS (
	SELECT DISTINCT 
		canvas_id , 
		canvas_name ,
		MAX(REPLACE(TRANSLATE(tags, '[]', ''), '"', '')::VARCHAR(100)) AS canvas_tags,
		max(updated_at) AS updated_at
	FROM stg_external_apis.braze_canvas_details
	GROUP BY 1,2
),
canvas_name AS (
	SELECT DISTINCT 
		canvas_id , 
		canvas_name ,
		canvas_tags,
		ROW_NUMBER() OVER(PARTITION BY canvas_id ORDER BY updated_at desc) AS row_no
	FROM canvas_name_prep
),
campaign_tags AS
(
	SELECT DISTINCT campaign_name, REPLACE(TRANSLATE(tags, '[]', ''), '"', '')::VARCHAR(100) AS campaign_tags
	FROM stg_external_apis.braze_campaign_details bcd 
)
SELECT DISTINCT
	COALESCE(cn.campaign_name,can.canvas_name) AS campaign_canvas_name,
	g.*,
	pn.product_name,
	LOWER(COALESCE(ct.campaign_tags,can.canvas_tags )) AS campaign_canvas_tags
FROM product_summary_grouped g
LEFT JOIN campaign_names cn 
	ON g.campaign_canvas_id = cn.campaign_id
	AND g.type_event = 'Campaign' 
LEFT JOIN canvas_name can 
	ON g.campaign_canvas_id = can.canvas_id
	AND g.type_event = 'Canvas' 
	AND can.row_no =1
LEFT JOIN ods_production.product pn
	ON pn.product_sku = g.product_sku
LEFT JOIN campaign_tags ct 
	ON ct.campaign_name = g.campaign_canvas_id
	AND g.type_event = 'Campaign' 		
WITH NO SCHEMA BINDING;
