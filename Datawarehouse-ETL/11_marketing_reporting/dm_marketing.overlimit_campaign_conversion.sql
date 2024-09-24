CREATE OR REPLACE VIEW dm_marketing.v_overlimit_conversion AS 
WITH EU_orders AS
(
	SELECT 
		d.user_id AS customer_id,
		d.order_id,
		d.creation_timestamp::TIMESTAMP AS created_at,
		d.credit_limit::FLOAT AS amount, 
		o.store_country,
		o.new_recurring,
		o.device,
		ROW_NUMBER() OVER (PARTITION BY d.order_id ORDER BY d.updated_at DESC) AS row_num
	FROM s3_spectrum_rds_dwh_fraud_credit_and_risk.order_approval_results d
	LEFT JOIN master."order" o 
      ON o.order_id = d.order_id
	WHERE o.declined_reason ILIKE '%Overlimit%'
	  AND o.store_country IN ('Spain', 'Germany', 'Netherlands', 'Austria') 
	  AND o.submitted_date IS NOT NULL
	  AND created_at >= '2023-02-01'
)
, subcategory_declined AS 
(
	SELECT 
		order_id AS order_id_declined,
		LISTAGG(DISTINCT subcategory, ', ') WITHIN GROUP (ORDER BY subcategory) subcategories_declined_order,
		LISTAGG(DISTINCT name, ', ') WITHIN GROUP (ORDER BY subcategory) product_names_declined_order
	FROM s3_spectrum_rds_dwh_order_approval.order_item o
	WHERE EXISTS (SELECT NULL FROM EU_orders eu WHERE o.order_id = eu.order_id)
	GROUP BY 1
)
, braze AS 
(
	SELECT DISTINCT customer_id, sent_date, is_opened, is_clicked, first_time_clicked
	FROM dm_marketing.braze_canvas_events_all b
	WHERE canvas_name LIKE '%declined_overlimit%'
)
, new_order_info AS 
(
	SELECT 
		o.order_id,
		o.customer_id,
		o.created_date,
		o.submitted_date,
		o.paid_date,
		o.new_recurring,
		o.marketing_campaign,
		o.device,
		o.basket_size,
		o.order_value 
	FROM master."order" o
	WHERE created_date >= '2023-02-01'
	  AND marketing_campaign LIKE '%declined_overlimit%'
	  AND EXISTS (SELECT NULL FROM braze b WHERE o.customer_id = b.customer_id)	
	  AND o.submitted_date IS NOT NULL
)
, product_info_new_submission AS 
(
	SELECT 
		oi.order_id,
		LISTAGG(DISTINCT oi.subcategory_name,', ') WITHIN GROUP (ORDER BY oi.subcategory_name) AS subcategory_order_post_campaign,
		LISTAGG(DISTINCT oi.product_name,', ') WITHIN GROUP (ORDER BY oi.subcategory_name) AS product_name_order_post_campaign
	FROM ods_production.order_item oi 
	WHERE EXISTS (SELECT NULL FROM new_order_info o WHERE o.order_id = oi.order_id)
	GROUP BY 1
)
, base_data_declined_order AS
(
	SELECT 
		d.customer_id,
		d.order_id,
		d.created_at,
		d.amount::FLOAT AS subscription_limit,
		d.store_country,
		o.submitted_date::date AS  submitted_date,
		date_trunc('week',o.submitted_date)::date AS week_of_submission,
		o.new_recurring,
		CASE
			WHEN o.basket_size > 0 THEN o.basket_size
			ELSE order_value
		END AS order_value,
		c.subcategories_declined_order,
		c.product_names_declined_order,
		COALESCE(SUM(pm.subscription_value_eur),0) AS ASV_at_order_time_euro
	FROM EU_orders d 
	LEFT JOIN master."order" o ON o.order_id = d.order_id
	LEFT JOIN ods_production.subscription_phase_mapping	pm
		ON o.customer_id = pm.customer_id 
        AND o.order_id <> pm.order_id
		AND d.created_at::date >= pm.fact_day::date  
		AND d.created_at::date < COALESCE(pm.end_date::date , CURRENT_DATE)
	LEFT JOIN subcategory_declined c ON c.order_id_declined = d.order_id
    WHERE d.row_num = 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
, final_declined AS
(	
	SELECT 
		b.customer_id,
		b.order_id,
		b.created_at,
		b.subscription_limit,
		b.store_country,
		b.submitted_date,
		b.week_of_submission,
		b.new_recurring,
		b.order_value,
		b.subcategories_declined_order,
		b.product_names_declined_order,
		b.ASV_at_order_time_euro,
		(order_value + (COALESCE(ASV_at_order_time_euro,0))) AS potential_limit,
		potential_limit - subscription_limit AS overlimit_amount_,
		subscription_limit - COALESCE(ASV_at_order_time_euro,0) AS limit_availability,
		CASE 
			WHEN limit_availability BETWEEN 0 AND 29.99 THEN 'B1 - [0,30)'
			WHEN limit_availability BETWEEN 30 AND 39.99 THEN 'B2 - [30,40)'
			WHEN limit_availability BETWEEN 40 AND 49.99 THEN 'B3 - [40,50)'
			WHEN limit_availability BETWEEN 50 AND 69.99 THEN 'B4 - [50,70)'
			WHEN limit_availability BETWEEN 70 AND 99.99 THEN 'B5 - [70,100)'
			WHEN limit_availability > 0 THEN 'B6 - [100,9999)'
			ELSE 'B7 - Overlimit before Order'
		END AS limit_availability_buckets
	FROM base_data_declined_order b
	WHERE potential_limit > subscription_limit
)
, base_data_submitted_order AS 
(
	SELECT 
		b.customer_id, 
		b.sent_date, 
		b.is_opened, 
		b.is_clicked, 
		b.first_time_clicked,
		o.order_id,
		o.created_date,
		o.submitted_date,
		o.paid_date,
		o.device,
		o.marketing_campaign,
		o.basket_size,
		o.order_value,
		n.subcategory_order_post_campaign,
		n.product_name_order_post_campaign
	FROM braze b
	LEFT JOIN new_order_info o
		ON o.customer_id = b.customer_id 
		 AND o.created_date >= b.first_time_clicked
	LEFT JOIN product_info_new_submission n
		ON n.order_id = o.order_id 
)
, pre_final AS 
(	
	SELECT 
		d.customer_id,
		d.order_id,
		d.created_at,
		d.subscription_limit,
		d.store_country,
		d.submitted_date,
		d.week_of_submission,
		d.new_recurring,
		d.order_value,
		d.subcategories_declined_order,
		d.product_names_declined_order,
		d.ASV_at_order_time_euro,
		d.potential_limit,
		d.overlimit_amount_,
		d.limit_availability,
		d.limit_availability_buckets,
		s2.sent_date,
		s2.is_opened,
		s2.is_clicked,
		s2.first_time_clicked,
		s.order_id AS order_id_post_campaign,
		s.created_date AS created_date_order_post_campaign,
		s.submitted_date AS submitted_date_order_post_campaign,
		s.paid_date AS paid_date_order_post_campaign,
		s.device AS device_post_campaign,
		s.marketing_campaign,
		s.basket_size,
		s.order_value AS order_value_post_campaign,
		s.subcategory_order_post_campaign,
		s.product_name_order_post_campaign,
		ROW_NUMBER() OVER (PARTITION BY d.order_id ORDER BY s.submitted_date) AS rowno,
		CASE 
			WHEN d.created_at < s.sent_date 
			 THEN ROW_NUMBER() OVER (PARTITION BY s.order_id, s.sent_date, CASE WHEN d.created_at < s.sent_date THEN 1 END ORDER BY d.created_at DESC) 
		END AS rowno_sent,
		CASE 
			WHEN d.created_at < s.sent_date
			 THEN ROW_NUMBER() OVER (PARTITION BY d.customer_id, CASE WHEN d.created_at < s.sent_date THEN 1 END ORDER BY d.created_at DESC)
		END AS rowno_
	FROM final_declined d 
	LEFT JOIN base_data_submitted_order s
		ON s.customer_id = d.customer_id 
		 AND s.created_date >= d.created_at
		 AND s.order_id != d.order_id
		 AND s.created_date <= dateadd('day',5,s.sent_date)
	LEFT JOIN base_data_submitted_order s2
		ON s2.customer_id = d.customer_id 
		 AND s2.sent_date >= d.created_at
)
SELECT 
	customer_id,
	order_id,
	created_at,
	subscription_limit,
	store_country,
	submitted_date,
	week_of_submission,
	new_recurring,
	order_value,
	subcategories_declined_order,
	product_names_declined_order,
	ASV_at_order_time_euro,
	potential_limit,
	overlimit_amount_,
	limit_availability,
	limit_availability_buckets,
	sent_date,
	is_opened,
	is_clicked,
	first_time_clicked,
	CASE WHEN rowno_sent = 1 THEN order_id_post_campaign END as order_id_post_campaign,
	CASE WHEN rowno_sent = 1 THEN created_date_order_post_campaign END as created_date_order_post_campaign,
	CASE WHEN rowno_sent = 1 THEN submitted_date_order_post_campaign END as submitted_date_order_post_campaign,
	CASE WHEN rowno_sent = 1 THEN paid_date_order_post_campaign END as paid_date_order_post_campaign,
	CASE WHEN rowno_sent = 1 THEN device_post_campaign END as device_post_campaign,
	CASE WHEN rowno_sent = 1 THEN marketing_campaign END as marketing_campaign,
	CASE WHEN rowno_sent = 1 THEN basket_size END as basket_size,
	CASE WHEN rowno_sent = 1 THEN order_value_post_campaign END as order_value_post_campaign,
	CASE WHEN rowno_sent = 1 THEN subcategory_order_post_campaign END as subcategory_order_post_campaign,
	CASE WHEN rowno_sent = 1 THEN product_name_order_post_campaign END as product_name_order_post_campaign
FROM pre_final f
WHERE (rowno = 1 OR rowno_sent = 1)
WITH NO SCHEMA BINDING	 
;