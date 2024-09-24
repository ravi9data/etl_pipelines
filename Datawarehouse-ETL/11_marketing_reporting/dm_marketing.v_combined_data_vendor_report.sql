
--for each order I looked at the items related to the order to get the category name, brand name etc

DROP VIEW IF EXISTS dm_marketing.v_order_data_vendor_report;
CREATE VIEW dm_marketing.v_order_data_vendor_report AS
WITH vendor_campaigns AS (
    SELECT DISTINCT LOWER(marketing_campaign) AS marketing_campaign, 
           LOWER(marketing_content) AS marketing_content
    FROM traffic.page_views
    WHERE DATE(page_view_start) >= DATE_ADD('month',-6, current_date)
    AND marketing_content LIKE '%_Partners_%'
    OR marketing_campaign LIKE '%_Partners_%'
)

    SELECT DISTINCT 
    	DATE(a.submitted_date) AS reporting_date,
        a.order_id,
        a.store_country AS country,
        CASE WHEN m.marketing_channel IN ( 'Paid Search Brand', 'Paid Search Non Brand') THEN 'Paid Search'
             ELSE m.marketing_channel end AS marketing_channel,
             m.marketing_medium,
        CASE WHEN m.marketing_channel = 'CRM' THEN
             CASE WHEN m.marketing_medium = 'email' THEN 'Email'
                  WHEN m.marketing_medium = 'push_notification' THEN 'Push Notification'
                  ELSE 'Other' end
             WHEN m.marketing_source = 'google' AND m.marketing_medium = 'cpc' THEN 'Google Search' --
             WHEN m.marketing_source = 'google' AND m.marketing_medium = 'display' THEN 'Google Display' --
             WHEN m.marketing_source = 'facebook' THEN 'Facebook'
             WHEN m.marketing_source = 'google_perf_max' THEN 'Google Performance Max'
             ELSE m.marketing_source end AS marketing_source,
        CASE WHEN LOWER(m.marketing_campaign) = 'de_de_b2c_partners_b' AND LOWER(m.marketing_content) = 'de_ad_b2c_prospecting_partners_03082021' THEN 
        	'de_de_b2c_partners_b&o_musiccat_multiproduct_video_9-16_trending__nodeal___vendor_bang_&_olufsen_230522'
        	 WHEN m.marketing_source IN ('facebook','tiktok') AND  lower(m.marketing_channel) = 'paid social' THEN lower(m.marketing_content)
        	ELSE LOWER(m.marketing_campaign) END AS marketing_campaign,
        lower(COALESCE(v.marketing_content,m.marketing_content,'n/a')) AS marketing_content,
        lower(COALESCE(m.marketing_content,'n/a')) AS marketing_term,
        CASE WHEN a.marketing_channel IN ('Display', 'Paid Search Brand', 'Paid Search Non Brand', 'Paid Social', 'CRM') AND a.marketing_campaign ILIKE '%vendor%' THEN TRUE 
             WHEN a.marketing_channel = 'Paid Social' AND v.marketing_campaign IS NOT NULL THEN TRUE ELSE FALSE END AS is_vendor,
        a.submitted_date,
        a.paid_date,
        a.approved_date ,
        a.paid_orders,
        a.completed_orders,
        c.subscription_id,
        b.category_name,
        b.subcategory_name,
        b.brand,
        b.product_name,
        b.product_sku,
        a.customer_type ,
        a.new_recurring,
        a.customer_id as customer_id_order,
        s.customer_id AS customer_id_subscription,
        s.created_date::date AS created_date_subcription
    FROM master.order a
        LEFT JOIN ods_production.order_item b on a.order_id = b.order_id
        LEFT JOIN ods_production.subscription c on c.order_id = b.order_id AND c.product_sku = b.product_sku AND c.variant_sku = b.variant_sku
        LEFT JOIN master.subscription s ON c.subscription_id = s.subscription_id 
        LEFT JOIN ods_production.order_marketing_channel m on m.order_id = a.order_id
        LEFT JOIN vendor_campaigns v ON v.marketing_campaign = m.marketing_campaign
    WHERE DATE(a.submitted_date) >= DATE_ADD('month', -6, current_date)  
WITH NO SCHEMA BINDING;

--In account data, like in the orders data I mapped is_vendor based on requirements from the ticket, true is used as filter in the reporting.

DROP VIEW IF EXISTS dm_marketing.v_account_data_vendor_report;
CREATE VIEW dm_marketing.v_account_data_vendor_report AS
    SELECT
       a.date::date AS reporting_date,
       'Germany' AS country,
       'Paid Social' AS marketing_channel,
       'Facebook' AS marketing_source,
       CASE WHEN LOWER(a.ad_name) = 'de_de_b2c_partners_b&o_musiccat_multiproduct_video_9-16_trending__nodeal___vendor_bang_&_olufsen_230522' THEN LOWER(ad_name)
       	ELSE LOWER(a.campaign_name) END AS marketing_campaign,
       lower(COALESCE(ad_set_name,'n/a')) AS marketing_term,
       lower(COALESCE(ad_name,'n/a')) AS marketing_content,
       TRUE::boolean AS is_vendor,
      'normal_customer' AS customer_type,
       SUM(a.impressions) AS impressions,
       SUM(a.total_spent_eur) AS spent,
       SUM(a.clicks) AS clicks
    FROM marketing.marketing_cost_daily_facebook a
    WHERE campaign_name = 'DE_Ad_B2C_Prospecting_Partners_03082021'
    AND reporting_date >= DATE_ADD('month',-6, current_date)
    GROUP BY 1,2,3,4,5,6,7,8,9
    UNION ALL
    SELECT
        reporting_date,
        COALESCE(country, 'N/A') AS country,
        channel_grouping AS marketing_channel,
        channel AS marketing_source,
        LOWER(campaign_name) AS marketing_campaign,
        lower(COALESCE(ad_set_name,'n/a')) AS marketing_term,
        lower(COALESCE(ad_name,'n/a')) AS marketing_content,
        CASE WHEN channel_grouping IN ('Display', 'Paid Search', 'Paid Social') AND campaign_name ilike '%vendor%' THEN TRUE
             WHEN channel_grouping = 'Paid Social' AND campaign_name LIKE '%_Partners_%' THEN TRUE
        ELSE FALSE END AS is_vendor,
        CASE WHEN customer_type = 'B2C' THEN 'normal_customer'
        	 WHEN customer_type = 'B2B' THEN 'business_customer'
        	 END AS customer_type,
        SUM(impressions) AS impressions,
        SUM(total_spent_eur) AS spent,
        SUM(clicks) AS clicks
    FROM marketing.marketing_cost_daily_combined
    WHERE reporting_date >= DATE_ADD('month',-6, current_date)
    AND campaign_name != 'DE_Ad_B2C_Prospecting_Partners_03082021'
    GROUP BY 1,2,3,4,5,6,7,8,9
WITH NO SCHEMA BINDING;

--I separate asv from first table, here I will take only yesterday date filtered in the report
DROP VIEW IF EXISTS dm_marketing.v_asv_vendor_report;
CREATE VIEW dm_marketing.v_asv_vendor_report AS
WITH dates AS (
    SELECT DISTINCT
           datum AS reporting_date
    FROM public.dim_dates
    WHERE datum >= DATE_ADD('day',-7, current_date)
),

vendor_campaigns AS (
    SELECT DISTINCT LOWER(marketing_campaign) AS marketing_campaign, 
           LOWER(marketing_content) AS marketing_content
    FROM traffic.page_views
    WHERE DATE(page_view_start) >= DATE_ADD('month',-6, current_date)
    AND marketing_content LIKE '%_Partners_%'
     OR marketing_campaign LIKE '%_Partners_%'
)

SELECT DISTINCT 
		dat.reporting_date,
        ss.order_id,
        ss.subscription_id,
        ss.country_name AS country,
        ss.customer_type,
        a.category_name,
        a.subcategory_name,
        a.brand,
        a.product_name,
        a.product_sku,
        CASE WHEN o.marketing_channel in ( 'Paid Search Brand', 'Paid Search Non Brand') THEN 'Paid Search'
             ELSE o.marketing_channel end AS marketing_channel,
        CASE WHEN o.marketing_channel = 'CRM' THEN
             CASE WHEN o.marketing_medium = 'email' THEN 'Email'
                  WHEN o.marketing_medium = 'push_notification' THEN 'Push Notification'
                  ELSE 'Other' end
             WHEN o.marketing_source = 'google' AND o.marketing_medium = 'cpc' THEN 'Google Search'
             WHEN o.marketing_source = 'google' AND o.marketing_medium = 'display' THEN 'Google Display'
             WHEN o.marketing_source = 'facebook' THEN 'Facebook'
             WHEN o.marketing_source = 'google_perf_max' THEN 'Google Performance Max'
             ELSE o.marketing_source end AS marketing_source,
        CASE WHEN LOWER(o.marketing_campaign) = 'de_de_b2c_partners_b' AND LOWER(o.marketing_content) = 'de_ad_b2c_prospecting_partners_03082021' THEN 
        'de_de_b2c_partners_b&o_musiccat_multiproduct_video_9-16_trending__nodeal___vendor_bang_&_olufsen_230522'
        ELSE LOWER(o.marketing_campaign) END AS marketing_campaign,
        LOWER(COALESCE(o.marketing_content, 'n/a')) AS marketing_content,
        lower(COALESCE(o.marketing_term, 'n/a')) AS marketing_term,
        CASE WHEN o.marketing_channel IN ('Display', 'Paid Search Brand', 'Paid Search Non Brand', 'Paid Social', 'CRM')
             AND o.marketing_campaign ILIKE '%vendor%' THEN TRUE 
             WHEN o.marketing_channel = 'Paid Social' AND v.marketing_campaign IS NOT NULL THEN TRUE ELSE FALSE END AS is_vendor,
        COALESCE(ss.subscription_value_eur, 0) AS subscription_value
    FROM dates dat
        LEFT JOIN ods_production.subscription_phase_mapping ss
            ON dat.reporting_date::date >= ss.fact_day::date
            AND dat.reporting_date::date <= COALESCE(ss.end_date::date, dat.reporting_date::date + 1)
        LEFT JOIN ods_production.order_item a
            ON a.order_id = ss.order_id AND a.product_sku = ss.product_sku AND
                          a.variant_sku = ss.variant_sku
        LEFT JOIN ods_production.order_marketing_channel o on o.order_id=ss.order_id
        LEFT JOIN vendor_campaigns v ON LOWER(o.marketing_campaign) = v.marketing_campaign
    WHERE TRUE
      AND ss.store_label NOT ilike '%old%'
      AND ss.country_name <> 'United Kingdom'
      AND ss.new_recurring IS NOT NULL
WITH NO SCHEMA BINDING;


DROP VIEW IF EXISTS dm_marketing.v_crm_vendor_report;
CREATE VIEW dm_marketing.v_crm_vendor_report AS
WITH crm_campaign AS (SELECT
        DATE(sent_date) AS reporting_date,
        COALESCE(signup_country, store_country) AS country,
        source_type,
        campaign_name,
        is_sent,
        is_opened,
        is_clicked,
        customer_type 
    FROM dm_marketing.braze_campaign_events_all
    WHERE sent_date >= DATE_ADD('month',-6, current_date)
    UNION ALL
    SELECT
        DATE(sent_date) AS reporting_date,
        CASE WHEN a.signup_country = 'N/A' THEN COALESCE( billing_country, a.signup_country) ELSE a.signup_country END AS country,
        source_type,
        canvas_name AS campaign_name,
        is_sent,
        is_opened,
        is_clicked,
        a.customer_type 
    FROM dm_marketing.braze_canvas_events_all a
    LEFT JOIN master.customer b on a.customer_id = b.customer_id
    WHERE sent_date >= DATE_ADD('month',-6, current_date)
    )

    SELECT
        reporting_date,
        country,
        'CRM' AS marketing_channel,
        source_type AS marketing_source,
        campaign_name AS marketing_campaign,
        customer_type,
        CASE WHEN campaign_name ILIKE '%vendor%' THEN TRUE ELSE FALSE END AS is_vendor,
        SUM(coalesce(is_sent,0)) AS email_sent,
        SUM(coalesce(is_opened,0)) AS opened,
        SUM(coalesce(is_clicked,0)) AS clicked
    FROM crm_campaign
    WHERE reporting_date >= DATE_ADD('month',-6, current_date)
    GROUP BY 1,2,3,4,5,6,7
WITH NO SCHEMA BINDING;


--I tried to combine all data based on granularity level for whole year, without using date in join
DROP VIEW IF EXISTS dm_marketing.v_combined_data_vendor_report;
CREATE VIEW dm_marketing.v_combined_data_vendor_report AS
WITH
crm AS ( -- ONLY crm
    SELECT
        reporting_date,
    	country,
        marketing_channel,
        marketing_source,
        marketing_campaign,
        'n/a' AS marketing_content,
        'n/a' AS marketing_term,
        is_vendor,
        customer_type,
        SUM(email_sent) AS email_sent,
        SUM(opened) AS opened,
        SUM(clicked) AS clicked
    FROM dm_marketing.v_crm_vendor_report
    GROUP BY 1,2,3,4,5,6,7
    ),

orders AS (
    SELECT
    	reporting_date,
        country,
        marketing_channel,
        marketing_source,
        marketing_campaign,
        marketing_content,
        marketing_term,
        is_vendor,
        customer_type,
        COUNT(DISTINCT CASE WHEN paid_orders >= 1 THEN order_id END) AS paid_orders,
        COUNT(DISTINCT CASE WHEN completed_orders >= 1 THEN order_id END) AS submitted_orders,
    	COUNT(DISTINCT CASE WHEN approved_date IS NOT NULL THEN order_id END) AS approved_orders,
    	COUNT(DISTINCT CASE WHEN new_recurring = 'NEW' and paid_orders >= 1 THEN customer_id_order END) AS new_customers_orders
    FROM dm_marketing.v_order_data_vendor_report
    GROUP BY 1,2,3,4,5,6,7,8,9
    ),
    
new_customers AS (
    SELECT
    	created_date_subcription AS reporting_date,
        country,
        marketing_channel,
        marketing_source,
        marketing_campaign,
        marketing_content,
        marketing_term,
        is_vendor,
        customer_type,
    	COUNT(DISTINCT CASE WHEN new_recurring = 'NEW' THEN customer_id_subscription END) AS new_customers_subscription
    FROM dm_marketing.v_order_data_vendor_report
    GROUP BY 1,2,3,4,5,6,7
    ),

asv AS (
    SELECT
        reporting_date,
    	country,
        marketing_channel,
        marketing_source,
        marketing_campaign,
        marketing_content,
        marketing_term,
        is_vendor,
        customer_type,
        SUM(subscription_value) AS asv
    FROM dm_marketing.v_asv_vendor_report
    WHERE reporting_date = current_date - 1
    GROUP BY 1,2,3,4,5,6,7
    ),

account AS (
    SELECT
        reporting_date,
    	country,
        marketing_channel,
        marketing_source,
        marketing_campaign,
        marketing_content,
        marketing_term,
        is_vendor,
        customer_type,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spent) AS spent
    FROM dm_marketing.v_account_data_vendor_report
    GROUP BY 1,2,3,4,5,6,7
    )

    , final_table AS (
    SELECT
        COALESCE(a.reporting_date, b.reporting_date, 
        	c.reporting_date, d.reporting_date, nc.reporting_date) AS reporting_date,
    	COALESCE(a.country, b.country, c.country, d.country, nc.country) AS country,
        COALESCE(a.marketing_channel, b.marketing_channel, c.marketing_channel,
        	d.marketing_channel, nc.marketing_channel) AS marketing_channel,
        CASE WHEN COALESCE(a.marketing_source, b.marketing_source, c.marketing_source,
        	d.marketing_source, nc.marketing_source) ILIKE 'tiktok' THEN 'Tiktok' 
        	ELSE COALESCE(a.marketing_source, b.marketing_source, c.marketing_source,
        	d.marketing_source, nc.marketing_source) END AS marketing_source,
        COALESCE(a.marketing_campaign, b.marketing_campaign, c.marketing_campaign,
        	d.marketing_campaign, nc.marketing_campaign) AS marketing_campaign,
        COALESCE(a.marketing_content, b.marketing_content, c.marketing_content,
        	d.marketing_content, nc.marketing_content) AS marketing_content,
        COALESCE(a.marketing_term, b.marketing_term, c.marketing_term,
        	d.marketing_term, nc.marketing_term) AS marketing_term,
        COALESCE(a.is_vendor, b.is_vendor, c.is_vendor, d.is_vendor, nc.is_vendor) AS is_vendor,
        COALESCE(a.customer_type, b.customer_type, c.customer_type, d.customer_type, nc.customer_type) AS customer_type,
        asv,
        impressions,
        clicks ,
        spent,
        submitted_orders,
        paid_orders,
        email_sent,
        opened,
        clicked,
        approved_orders,
        new_customers_subscription,
        new_customers_orders
    FROM orders a
    FULL OUTER JOIN asv b USING (
    	   customer_type,
    	   reporting_date,
    	   country,
           marketing_channel,
           marketing_source,
           marketing_campaign,
           marketing_content,
           marketing_term,
           is_vendor)
    FULL OUTER JOIN account c USING (
    	   customer_type,
           reporting_date,
    	   country,
           marketing_channel,
           marketing_source,
           marketing_campaign,
           marketing_content,
           marketing_term,
           is_vendor)
    FULL OUTER JOIN crm d USING (
    	   customer_type,
    	   reporting_date,
    	   country,
           marketing_channel,
           marketing_source,
           marketing_campaign,
           marketing_content,
           marketing_term,
           is_vendor)  
    FULL OUTER JOIN new_customers nc USING (
    	   customer_type,
    	   reporting_date,
    	   country,
           marketing_channel,
           marketing_source,
           marketing_campaign,
           marketing_content,
           marketing_term,
           is_vendor) 
)
           
           SELECT * FROM final_table
           WHERE 
           COALESCE(asv,0) <> 0
           OR COALESCE(impressions,0) <> 0
         OR COALESCE(clicks ,0) <> 0
         OR COALESCE(spent,0) <> 0
         OR COALESCE(submitted_orders,0) <> 0
         OR COALESCE(paid_orders,0) <> 0
         OR COALESCE(email_sent,0) <> 0
         OR COALESCE(opened,0) <> 0
         OR COALESCE(clicked,0) <> 0
         OR COALESCE(approved_orders,0) <> 0
         OR COALESCE(new_customers_subscription,0) <> 0
         OR COALESCE(new_customers_orders,0) <> 0
    WITH NO SCHEMA BINDING;
    
