DROP VIEW IF EXISTS dm_marketing.v_order_data_vendor_report_any_touch;
CREATE VIEW dm_marketing.v_order_data_vendor_report_any_touch AS
WITH vendor_campaigns AS (
    SELECT DISTINCT LOWER(marketing_campaign) AS marketing_campaign, 
           LOWER(marketing_content) AS marketing_content
    FROM web.page_views
    WHERE DATE(page_view_start) >= DATE_ADD('month',-6, current_date)
		AND (marketing_content LIKE '%_Partners_%'
    	OR marketing_campaign LIKE '%_Partners_%')
)
 , total_sessions_per_order AS (
 	SELECT 
 		order_id,
 		count(DISTINCT session_id) AS total_sessions
 	FROM traffic.session_order_mapping
 	WHERE DATE(submitted_date) >= DATE_ADD('month', -6, current_date)
 	GROUP BY 1	
)
, total_sessions_per_channel AS (
	SELECT 
		o.order_id,
		tt.total_sessions,
		CASE WHEN o.marketing_channel IN ( 'Paid Search Brand', 'Paid Search Non Brand') THEN 'Paid Search'
             ELSE o.marketing_channel end AS marketing_channel,
		o.marketing_medium ,
		CASE WHEN o.marketing_channel = 'CRM' THEN
             CASE WHEN o.marketing_medium = 'email' THEN 'Email'
                  WHEN o.marketing_medium = 'push_notification' THEN 'Push Notification'
                  ELSE 'Other' end
             WHEN o.marketing_source = 'google' AND o.marketing_medium = 'cpc' THEN 'Google Search' --
             WHEN o.marketing_source = 'google' AND o.marketing_medium = 'display' THEN 'Google Display' --
             WHEN o.marketing_source = 'facebook' THEN 'Facebook'
             WHEN o.marketing_source = 'google_perf_max' THEN 'Google Performance Max'
             ELSE o.marketing_source end AS marketing_source,
		CASE WHEN LOWER(m.marketing_campaign) = 'de_de_b2c_partners_b' AND LOWER(m.marketing_content) = 'de_ad_b2c_prospecting_partners_03082021' THEN 
        	'de_de_b2c_partners_b&o_musiccat_multiproduct_video_9-16_trending__nodeal___vendor_bang_&_olufsen_230522'
        	 WHEN o.marketing_source IN ('facebook','tiktok') AND  lower(o.marketing_channel) = 'paid social' THEN lower(m.marketing_content)
        	ELSE LOWER(m.marketing_campaign) END AS marketing_campaign,
        	CASE WHEN o.marketing_channel IN ('Display', 'Paid Search Brand', 'Paid Search Non Brand', 'Paid Social', 'CRM') AND m.marketing_campaign ILIKE '%vendor%' THEN TRUE 
				WHEN o.marketing_channel = 'Paid Social' AND v.marketing_campaign IS NOT NULL THEN TRUE ELSE FALSE END AS is_vendor,
		min(lower(COALESCE(v.marketing_content,m.marketing_content))) AS marketing_content,
		count(DISTINCT o.session_id) AS total_sessions_by_channel,
		COUNT(*) OVER (PARTITION BY  o.order_id) AS number_channels_per_order
 	FROM traffic.session_order_mapping o
 	LEFT JOIN traffic.sessions m 
 		ON o.session_id = m.session_id
 	LEFT JOIN vendor_campaigns v 
 		ON v.marketing_campaign = m.marketing_campaign
 	LEFT JOIN total_sessions_per_order tt 
 		ON o.order_id = tt.order_id
 	WHERE DATE(o.submitted_date) >= DATE_ADD('month', -6, current_date)
 	GROUP BY 1,2,3,4,5,6,7	
)
SELECT 
	DATE(a.submitted_date) AS reporting_date,
    a.order_id,
    a.store_country AS country,
    COALESCE(m.marketing_channel, a.marketing_channel ) AS marketing_channel,
    COALESCE(m.marketing_medium, 'n/a') AS marketing_medium,   
    COALESCE(m.marketing_source, 'n/a') AS marketing_source,
    COALESCE(m.marketing_campaign, a.marketing_campaign ) AS marketing_campaign ,
    COALESCE(m.marketing_content, 'n/a') AS marketing_content,
    m.is_vendor, 
    a.submitted_date,
    a.paid_date,
    a.approved_date ,
    CASE WHEN a.completed_orders > 0 
    			THEN (CASE WHEN COALESCE(m.total_sessions_by_channel::float, 0) = 0 THEN 1 ELSE m.total_sessions_by_channel::float END)::float /
    					(CASE WHEN COALESCE(m.total_sessions, 0) = 0 THEN 1 ELSE m.total_sessions END)::float/
    					(COUNT(*) OVER (PARTITION BY  a.order_id))::float *
    					(CASE WHEN COALESCE(m.number_channels_per_order::float, 0) = 0 THEN 1 ELSE m.number_channels_per_order::float END)::float 			
    	ELSE 0 END AS completed_orders,
    CASE WHEN a.paid_orders > 0 
    			THEN (CASE WHEN COALESCE(m.total_sessions_by_channel::float, 0) = 0 THEN 1 ELSE m.total_sessions_by_channel::float END)::float /
    					(CASE WHEN COALESCE(m.total_sessions, 0) = 0 THEN 1 ELSE m.total_sessions END)::float/
    					(COUNT(*) OVER (PARTITION BY  a.order_id))::float *
    					(CASE WHEN COALESCE(m.number_channels_per_order::float, 0) = 0 THEN 1 ELSE m.number_channels_per_order::float END)::float
    	ELSE 0 END AS paid_orders,
    CASE WHEN c.subscription_id IS NOT NULL
    			THEN (CASE WHEN COALESCE(m.total_sessions_by_channel::float, 0) = 0 THEN 1 ELSE m.total_sessions_by_channel::float END)::float /
    					(CASE WHEN COALESCE(m.total_sessions, 0) = 0 THEN 1 ELSE m.total_sessions END)::float/
    					(COUNT(*) OVER (PARTITION BY  c.subscription_id))::float *
    					(CASE WHEN COALESCE(m.number_channels_per_order::float, 0) = 0 THEN 1 ELSE m.number_channels_per_order::float END)::float
    	ELSE 0 END AS number_subscriptions,
    c.subscription_id,
    b.category_name,
    b.subcategory_name,
    b.brand,
    b.product_name,
    b.product_sku,
    a.customer_type ,
    a.new_recurring,
    c.customer_id AS customer_id_subscription,
    c.created_date::date AS created_date_subcription
FROM master.order a
    LEFT JOIN ods_production.order_item b on a.order_id = b.order_id
    LEFT JOIN ods_production.subscription c on c.order_id = b.order_id AND c.product_sku = b.product_sku AND c.variant_sku = b.variant_sku
    LEFT JOIN total_sessions_per_channel m ON m.order_id = a.order_id
WHERE DATE(a.submitted_date) >= DATE_ADD('month', -6, current_date)
WITH NO SCHEMA BINDING;