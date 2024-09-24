
DROP TABLE IF EXISTS tmp_braze_data;
CREATE TEMP TABLE tmp_braze_data
SORTKEY(id_campaign_hash, id_canva_hash)
DISTKEY(id_canva_hash)
AS
	SELECT 
		be.campaign_id,
		be.message_variation_id ,
		be.canvas_id,
		be.canvas_variation_id,
		be.canvas_step_id,
		be.user_id ,
		be.customer_id,
		'Email' AS source_type,
		NULL AS os,
		NULL AS os_version,
		be.first_time_clicked AS awareness_date,
		be.first_time_opened AS awareness_date_inf,
		be.sent_date,
		be.first_time_opened,
		be.first_time_clicked,
		be.is_sent,
		be.is_sent_overall,
		be.is_delivered,
		be.is_delivered_overall,
		be.is_opened_overall,
		be.is_opened,
		be.is_clicked_overall,
		be.is_clicked,
		be.is_unsubscribed,
		be.is_unsubscribed_overall,
		be.is_sent_overall - be.is_delivered_overall AS is_bounced_overall,
		be.is_sent - be.is_delivered AS is_bounced,
		be.seconds_til_first_open,
		be.seconds_til_first_click,
		SHA1(be.canvas_id || '|' || be.customer_id || '|' || be.sent_date || '|' || be.canvas_step_id  || '|' || be.canvas_variation_id
			|| '|' || source_type || '|' || COALESCE(awareness_date::varchar,'A') || '|' || COALESCE(awareness_date_inf::varchar,'A')
			|| '|' || COALESCE(be.first_time_opened::varchar,'A') || '|' || COALESCE(be.first_time_clicked::varchar,'A') || '|' || COALESCE(os,'A')
			|| '|' || COALESCE(os_version,'A')) || '|' || RAND() AS  id_canva_hash, -- IN ORDER TO CREATE UNIQUE ids FOR EACH row
		SHA1(be.campaign_id || '|' || be.customer_id || '|' || be.sent_date || '|' || be.message_variation_id
			|| '|' || source_type || '|' || COALESCE(awareness_date::varchar,'A') || '|' || COALESCE(awareness_date_inf::varchar,'A') 
			|| '|' || COALESCE(be.first_time_opened::varchar,'A') || '|' || COALESCE(be.first_time_clicked::varchar,'A') || '|' || COALESCE(os,'A')
			|| '|' || COALESCE(os_version,'A')) || '|' || RAND() AS id_campaign_hash
	FROM dm_marketing.braze_campaign_events_email be
	INNER JOIN master.customer c
		ON be.customer_id = c.customer_id
	WHERE sent_date::date >= DATEADD('day', -15, current_date)
	UNION
	SELECT 
		bp.campaign_id,
		bp.message_variation_id,
		bp.canvas_id,
		bp.canvas_variation_id,
		bp.canvas_step_id,
		bp.user_id ,
		bp.customer_id,
		'Push Notification' AS source_type,
		bp.platform AS os,
		NULL AS os_version, 
		bp.first_time_opened AS awareness_date,
		NULL AS awareness_date_inf,
		bp.sent_date,
		bp.first_time_opened,
		NULL AS first_time_clicked,
		bp.is_sent,
		bp.is_sent_overall,
		bp.is_sent - bp.is_bounced AS is_delivered,
		bp.is_sent_overall - bp.is_bounced_overall AS is_delivered_overall,
		bp.is_opened_overall,
		bp.is_opened,
		NULL AS is_clicked_overall,
		NULL AS is_clicked,
		NULL AS is_unsubscribed,
		NULL AS is_unsubscribed_overall,
		bp.is_bounced_overall,
		bp.is_bounced,
		bp.seconds_til_first_open, 
		NULL AS seconds_til_first_click,
		SHA1(bp.canvas_id || '|' || bp.customer_id || '|' || sent_date || '|' || canvas_step_id  || '|' || canvas_variation_id
			|| '|' || source_type || '|' || COALESCE(awareness_date::varchar,'A') || '|' || COALESCE(awareness_date_inf::varchar,'A')
			|| '|' || COALESCE(first_time_opened::varchar,'A') || '|' || COALESCE(first_time_clicked::varchar,'A') || '|' || COALESCE(os,'A')
			|| '|' || COALESCE(os_version,'A')) || '|' || RAND() AS  id_canva_hash, -- IN ORDER TO CREATE UNIQUE ids FOR EACH row
		SHA1(bp.campaign_id || '|' || bp.customer_id || '|' || bp.sent_date || '|' || bp.message_variation_id
			|| '|' || source_type || '|' || COALESCE(awareness_date::varchar,'A') || '|' || COALESCE(awareness_date_inf::varchar,'A') 
			|| '|' || COALESCE(bp.first_time_opened::varchar,'A') || '|' || COALESCE(first_time_clicked::varchar,'A') || '|' || COALESCE(os,'A')
			|| '|' || COALESCE(os_version,'A')) || '|' || RAND() AS id_campaign_hash
	FROM dm_marketing.braze_campaign_events_push_notification bp
	INNER JOIN master.customer c
		ON bp.customer_id = c.customer_id
	WHERE sent_date::date >= DATEADD('day', -15, current_date)
	UNION
	SELECT 
		ba.campaign_id,
		ba.message_variation_id,
		ba.canvas_id,
		ba.canvas_variation_id,
		ba.canvas_step_id,
		ba.user_id ,
		ba.customer_id,
		'In-App Message' AS source_type,
		platform AS os,
		os_version,
		first_time_clicked AS awareness_date,
		NULL AS awareness_date_inf,
		impression_date AS sent_date, 
		NULL AS first_time_opened,
		first_time_clicked,
		is_impression AS is_sent,
		is_impression_overall AS is_sent_overall,
		NULL AS is_delivered,
		NULL AS is_delivered_overall,
		NULL AS is_opened_overall,
		NULL AS is_opened,
		is_clicked_overall,
		is_clicked,
		NULL AS is_unsubscribed,
		NULL AS is_unsubscribed_overall,
		NULL AS is_bounced_overall,
		NULL AS is_bounced,
		NULL AS seconds_til_first_open,
		seconds_til_first_click,
		SHA1(ba.canvas_id || '|' || ba.customer_id || '|' || sent_date || '|' || ba.canvas_step_id  || '|' || ba.canvas_variation_id
			|| '|' || source_type || '|' || COALESCE(awareness_date::varchar,'A') || '|' || COALESCE(awareness_date_inf::varchar,'A')
			|| '|' || COALESCE(first_time_opened::varchar,'A') || '|' || COALESCE(ba.first_time_clicked::varchar,'A') || '|' || COALESCE(os,'A')
			|| '|' || COALESCE(ba.os_version,'A')) || '|' || RAND() AS  id_canva_hash, -- IN ORDER TO CREATE UNIQUE ids FOR EACH row
		SHA1(ba.campaign_id || '|' || ba.customer_id || '|' || sent_date || '|' || ba.message_variation_id
			|| '|' || source_type || '|' || COALESCE(awareness_date::varchar,'A') || '|' || COALESCE(awareness_date_inf::varchar,'A') 
			|| '|' || COALESCE(first_time_opened::varchar,'A') || '|' || COALESCE(ba.first_time_clicked::varchar,'A') || '|' || COALESCE(os,'A')
			|| '|' || COALESCE(ba.os_version,'A')) || '|' || RAND() AS id_campaign_hash
	FROM dm_marketing.braze_campaign_events_in_app_message ba
	INNER JOIN master.customer c
		ON ba.customer_id = c.customer_id
	WHERE impression_date::date >= DATEADD('day', -15, current_date)
;





DROP TABLE IF EXISTS tmp_order_data;
CREATE TEMP TABLE tmp_order_data
SORTKEY(order_id)
DISTKEY(order_id)
AS
--- Category and Product Information :: Order Level 
WITH orders_info_per_category AS (
	SELECT 
		o.order_id,
		s.category_name ,
		count(DISTINCT s.product_sku) AS total_products,
		sum(s.price) AS total_price
	FROM master."order" o 
	INNER JOIN ods_production.order_item s 
	  ON o.order_id = s.order_id
	WHERE created_date >= '2020-03-04' -- FIRST comms BY campaign/canvas ON 4 March 2020.
		AND o.submitted_date::date >= DATEADD('day', -15, current_date)
	GROUP BY 1,2
)
, order_categories_raw AS (
	SELECT 
		order_id, 
		category_name, 
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY total_price DESC, total_products DESC, category_name) AS rowno
	FROM orders_info_per_category
),
order_product_names_raw AS
(
	SELECT 
		oi.order_id,
		LEFT(oi.product_name, 30) AS product_name,
		oi.total_price,
		ROW_NUMBER() OVER (PARTITION BY oi.order_id ORDER BY oi.total_price DESC) AS product_ranking
	FROM ods_production.order_item oi
	LEFT JOIN master.ORDER o
		ON oi.order_id = o.order_id
	WHERE oi.created_at >= '2020-03-04' -- FIRST comms BY campaign/canvas ON 4 March 2020.
		AND oi.order_id IS NOT NULL
		AND o.submitted_date::date >= DATEADD('day', -15, current_date)
),
subscription_data AS
(
	SELECT 
		order_id, 
		SUM(CASE
				WHEN s.currency = 'EUR' THEN s.committed_sub_value 
				WHEN s.currency = 'USD' THEN ROUND((s.committed_sub_value + s.additional_committed_sub_value) * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur),1)
			END) AS total_committed_subs_value_eur
	FROM master.subscription s
	LEFT JOIN trans_dev.daily_exchange_rate exc
	    ON s.created_date::date = exc.date_ 
	    AND s.currency = 'USD'
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
	    ON s.currency = 'USD'
	WHERE s.start_date >= DATEADD('day', -15, current_date)
	GROUP BY 1
)
SELECT 
	o.order_id,
	o.customer_id,
	o.created_date,
	o.submitted_date,
	CASE
		WHEN o.approved_date IS NOT NULL THEN 1 
		ELSE 0
	END AS approved_order,
	o.store_type, 
	o.marketing_channel, ----- USE IN FIRST SPLIT
	o.marketing_campaign, ----- USE IN LATER SPLIT, USE THIS TO RECONCILE BETWEEN BRAZE / ORDER TABLE CAMPAIGN MAPPING
	o.device AS device_used_to_order, ----- USE IN FIRST SPLIT
	o.store_label,
	o.store_country, ----- USE IN FIRST SPLIT
	o.payment_orders,
	o.completed_orders,
	o.declined_orders,
	o.failed_first_payment_orders,
	o.paid_orders AS paid_order,
	oc.category_name,
	prod.product_name,
	s.total_committed_subs_value_eur,
	o.new_recurring
FROM master."order" o
LEFT JOIN order_categories_raw oc 
	ON oc.order_id = o.order_id
	AND oc.rowno = 1
LEFT JOIN order_product_names_raw prod 
	ON prod.order_id = o.order_id
	AND prod.product_ranking = 1 -- taking most expensive product per order
LEFT JOIN subscription_data s 
	ON s.order_id = o.order_id 
	AND o.paid_orders = 1
WHERE created_date >= '2020-03-04'   -- Earliest Braze communication 2020-03-04 (across ALL mediums AND canvas/campaign)
	AND submitted_date IS NOT NULL
	AND o.order_id IS NOT NULL
	AND customer_id::int != 1734269 -- duplicate IN master.ORDER
	AND o.submitted_date::date >= DATEADD('day', -15, current_date)
;
		



DROP TABLE IF EXISTS tmp_full_data_raw;
CREATE TEMP TABLE tmp_full_data_raw
SORTKEY(order_id, id_campaign_hash, id_canva_hash)
DISTKEY(id_canva_hash)
AS
	SELECT 
		b.*,
		o.order_id,
		o.created_date,
		o.submitted_date,
		o.approved_order, 
		o.marketing_channel,
		o.device_used_to_order,
		o.store_country, 
		o.paid_order, 
		o.category_name, 
		o.product_name,
		o.total_committed_subs_value_eur,
		o.new_recurring,
		NULL::varchar AS customer_type,
		NULL::varchar AS customer_age,
		NULL::varchar AS rfm_segment,
		NULL::varchar AS crm_label,
		NULL::varchar AS crm_label_braze,
		NULL::timestamp AS customer_acq_cohort,
		NULL::varchar AS signup_country,
		DATEDIFF(HOUR,awareness_date::timestamp, submitted_date::timestamp)/24.0 AS order_time_lag_days,
		DATEDIFF(MINUTE,awareness_date::timestamp, submitted_date::timestamp)/60.0 AS order_time_lag_hours,
		DATEDIFF(HOUR,awareness_date_inf::timestamp, submitted_date::timestamp)/24.0 AS order_time_lag_days_influenced,
		-- TO REMOVE DUPLICATES ON THE BASIS OF MULTIPLE ORDERS PER COMMUNICATION ITEM. THIS WILL ONLY TAKE THE FIRST RELEVANT ORDER PER BRAZE ITEM.
		CASE 
			WHEN b.source_type = 'Email' AND o.order_id IS NOT NULL
			THEN 
				ROW_NUMBER() OVER (
					PARTITION BY COALESCE(b.campaign_id, 'A') , 
								COALESCE(b.message_variation_id, 'A') , 
								COALESCE(b.canvas_id, 'A'), 
								COALESCE(b.canvas_variation_id, 'A'), 
								COALESCE(b.canvas_step_id, 'A'), 
								b.user_id, 
								b.customer_id, 
								b.sent_date 
					ORDER BY o.submitted_date) 
		END AS row_num_email,
		CASE 
			WHEN b.source_type = 'Push Notification' AND o.order_id IS NOT NULL
			THEN 
				ROW_NUMBER() OVER (
					PARTITION BY COALESCE(b.campaign_id, 'A'), 
								COALESCE(b.message_variation_id, 'A'), 
								COALESCE(b.canvas_id, 'A'), 
								COALESCE(b.canvas_variation_id, 'A'), 
								COALESCE(b.canvas_step_id, 'A'), 
								b.user_id, 
								b.customer_id, 
								b.os, 
								b.sent_date 
					ORDER BY o.submitted_date) 
		END AS row_num_push,
		CASE 
			WHEN b.source_type = 'In-App Message' AND o.order_id IS NOT NULL
			THEN 
				ROW_NUMBER() OVER (
					PARTITION BY COALESCE(b.campaign_id, 'A'), 
								COALESCE(b.message_variation_id, 'A'), 
								COALESCE(b.canvas_id, 'A'), 
								COALESCE(b.canvas_variation_id, 'A'), 
								COALESCE(b.canvas_step_id, 'A'), 
								b.user_id, 
								b.customer_id, 
								b.os, 
								b.os_version, 
								b.sent_date 
					ORDER BY o.submitted_date)
		END AS row_num_inapp,
		NULL::timestamp as afl_next_payment_date,
		-- NOTE this is to ensure an order is attributed towards the last item sent in conversion period. 	
		CASE 
			WHEN order_id IS NOT NULL AND awareness_date IS NOT NULL			
				THEN ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COALESCE(awareness_date, '1990-01-01') DESC) 
			END AS row_num_order_alloc,
		CASE 
			WHEN order_id IS NOT NULL AND awareness_date_inf IS NOT NULL			
				THEN ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY COALESCE(awareness_date_inf, '1990-01-01') DESC) 
			END AS row_num_order_alloc_inf,
		-- Ensure conversions are within the correct conversion periods for their mediums.
		CASE
			WHEN source_type = 'Email' AND order_time_lag_days BETWEEN 0 AND 5 THEN 1
			WHEN source_type = 'In-App Message' AND order_time_lag_days BETWEEN 0 AND 3 THEN 1
			WHEN source_type = 'Push Notification' AND order_time_lag_days BETWEEN 0 AND 7 THEN 1
			ELSE 0
			END AS order_within_timeframe_conversion,
		-- Allocate converted and influenced orders.	
		CASE
			WHEN order_within_timeframe_conversion = 1 AND row_num_order_alloc = 1 THEN 1
			ELSE 0
			END AS converted_order, -- IN later tables, make sure you FILTER WITH converted_order = 1 
		CASE
			WHEN order_time_lag_days_influenced BETWEEN 0 AND 5  -- We ARE ONLY looking AT email opens here. This field will be NULL FOR other mediums.
				AND row_num_order_alloc_inf = 1
				THEN 1
			ELSE 0
			END AS influenced_order -- IN later tables, make sure you FILTER WITH influenced_order = 1 
	FROM tmp_braze_data b
	LEFT JOIN tmp_order_data o
		ON b.customer_id = o.customer_id
		AND ((DATEDIFF(DAY,awareness_date::date, submitted_date::date) BETWEEN 0 AND 7) OR (DATEDIFF(DAY,awareness_date_inf::date, submitted_date::date) BETWEEN 0 AND 5))
		AND ((awareness_date < submitted_date) OR (awareness_date_inf < submitted_date))
;




BEGIN TRANSACTION;

DELETE FROM dm_marketing.braze_order_attribution
WHERE braze_order_attribution.sent_date::date >= DATEADD('day', -15, current_date);

INSERT INTO dm_marketing.braze_order_attribution
SELECT * 
FROM tmp_full_data_raw;

END TRANSACTION;




		
DROP TABLE IF EXISTS tmp_customer_data;
CREATE TEMP TABLE tmp_customer_data
SORTKEY(customer_id)
DISTKEY(customer_id)
AS
	SELECT 
		c.customer_id,
		c.customer_type, 
		c."age" AS customer_age, 
		c.rfm_segment, 
		c.crm_label, 
		c.crm_label_braze,
		DATE_TRUNC('year', c.customer_acquisition_cohort) AS customer_acq_cohort, 
		CASE
			WHEN c.signup_country != 'never_add_to_cart'
				THEN c.signup_country
			WHEN c.signup_country = 'never_add_to_cart'
				THEN 'N/A'
		END AS signup_country,
		MIN(CASE WHEN o.new_recurring = 'RECURRING' THEN o.created_date::date END) AS first_recurring_date,
		MIN(o.paid_date::date) AS first_paid_date,
		max(s.next_due_date) as afl_next_payment_date  
	FROM master.customer c
	LEFT JOIN master."order" o 
		ON c.customer_id = o.customer_id
	LEFT JOIN master.subscription s
		ON c.customer_id = s.customer_id	
		AND s.status ='ACTIVE' 
	INNER JOIN tmp_braze_data bd
		ON bd.customer_id = c.customer_id
	GROUP BY 1,2,3,4,5,6,7,8
	
	
;



UPDATE
	dm_marketing.braze_order_attribution
SET
	customer_type = cd.customer_type,
	customer_age = cd.customer_age,
	rfm_segment = cd.rfm_segment,
	crm_label = cd.crm_label,
	crm_label_braze = cd.crm_label_braze, 
	customer_acq_cohort = cd.customer_acq_cohort,
	signup_country = cd.signup_country,
	afl_next_payment_date	 = cd.afl_next_payment_date	,
	new_recurring = CASE WHEN order_id IS NULL THEN 
							CASE
								WHEN sent_date > cd.first_paid_date
									 THEN 'RECURRING'
								WHEN cd.first_recurring_date IS NULL
									THEN 'NEW'
								WHEN sent_date < cd.first_recurring_date
									THEN 'NEW'
								WHEN sent_date >= cd.first_recurring_date
									THEN 'RECURRING'
							END
						 WHEN order_id IS NOT NULL THEN new_recurring
						 END
FROM tmp_customer_data cd
WHERE braze_order_attribution.customer_id = cd.customer_id;
