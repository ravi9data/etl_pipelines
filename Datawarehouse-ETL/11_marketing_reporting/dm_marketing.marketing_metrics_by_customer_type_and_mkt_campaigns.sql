DROP TABLE IF EXISTS tmp_marketing_metrics_by_customer_type_and_mkt_campaigns;
CREATE TABLE tmp_marketing_metrics_by_customer_type_and_mkt_campaigns AS
WITH marketing_channel_attributes AS (
	SELECT DISTINCT
	  channel_grouping
	 ,cash_non_cash
	 ,brand_non_brand
	FROM marketing.marketing_cost_channel_mapping mccm
	WHERE CASE WHEN mccm.channel_grouping = 'Influencers' AND mccm.cash_non_cash IN ('Non Cash' , 'Non-Cash')  
			THEN 'NOK' ELSE 'OK' END = 'OK'
	--
	UNION ALL
	--
	SELECT
	'Paid Search Brand' AS channel_grouping,
	'Cash' AS cash_non_cash ,
	'Performance' AS brand_non_brand
	UNION ALL
	SELECT
	'Paid Search Non Brand' AS channel_grouping,
	'Cash' AS cash_non_cash ,
	'Performance' AS brand_non_brand
	UNION ALL
	SELECT
	'Paid Social Branding' AS channel_grouping,
	'Cash' AS cash_non_cash ,
	'Performance' AS brand_non_brand
	UNION ALL
	SELECT
	'Paid Social Performance' AS channel_grouping,
	'Cash' AS cash_non_cash ,
	'Performance' AS brand_non_brand
	UNION ALL
	SELECT
	'Display Branding' AS channel_grouping,
	'Cash' AS cash_non_cash ,
	'Performance' AS brand_non_brand
	UNION ALL
	SELECT
	'Display Performance' AS channel_grouping,
	'Cash' AS cash_non_cash ,
	'Performance' AS brand_non_brand
)
,retention_group AS (
	SELECT DISTINCT new_recurring AS retention_group
	FROM master.ORDER
)
,customer_type AS (
	SELECT DISTINCT customer_type
	FROM master.ORDER
	WHERE customer_type IS NOT NULL 
	UNION
	SELECT
	'No Info' AS customer_type
)
,marketing_channel_mapping AS (
	SELECT DISTINCT
	  s.marketing_channel
	 ,COALESCE(s.marketing_source,'n/a') AS marketing_source
	 ,COALESCE(s.marketing_medium,'n/a') AS marketing_medium
	 ,COALESCE(um.marketing_channel, CASE
	   WHEN s.marketing_channel IN ('Paid Search Brand', 'Paid Search Non Brand', 'Paid Social', 'Paid Social Branding', 
    'Paid Social Performance', 
    'Display Branding',
    'Display Performance')
	    THEN s.marketing_channel || ' Other'
	   END,
	   s.marketing_channel,
	   'n/a') AS marketing_channel_detailed
	FROM traffic.sessions s
	  LEFT JOIN marketing.utm_mapping um
	    ON s.marketing_channel = um.marketing_channel_grouping
	    AND s.marketing_source = um.marketing_source
	WHERE DATE(session_start) >= DATEADD('month',-6,current_date)
	--
	UNION
	--
	SELECT DISTINCT
	  omc.marketing_channel
	 ,COALESCE(omc.marketing_source,'n/a') AS marketing_source
	 ,COALESCE(omc.marketing_medium,'n/a') AS marketing_medium
	 ,COALESCE(um.marketing_channel, CASE
	   WHEN o.marketing_channel IN ('Paid Search Brand', 'Paid Search Non Brand', 'Paid Social', 'Paid Social Branding', 
    'Paid Social Performance', 
    'Display Branding',
    'Display Performance')
	    THEN o.marketing_channel || ' Other'
	   END,
	   o.marketing_channel,
	   'n/a') AS marketing_channel_detailed
	FROM master.ORDER o
	  LEFT JOIN ods_production.order_marketing_channel omc
	    ON omc.order_id = o.order_id
	  LEFT JOIN marketing.utm_mapping um
	    ON omc.marketing_channel = um.marketing_channel_grouping
	    AND omc.marketing_source = um.marketing_source
	WHERE o.created_date >= DATEADD('month',-6,current_date)	
)
, prep AS (
	SELECT 
		w.session_id,
		w.page_view_id ,
		w.page_view_date::date,
		s.marketing_campaign,
		s.marketing_channel,
		s.marketing_content,
		CASE
			WHEN w.page_urlpath ILIKE '/de-%' THEN 'Germany'
			WHEN w.page_urlpath ILIKE '/us-%' THEN 'United States'
			WHEN w.page_urlpath ILIKE '/es-%' THEN 'Spain'
			WHEN w.page_urlpath ILIKE '/nl-%' THEN 'Netherlands'
			WHEN w.page_urlpath ILIKE '/at-%' THEN 'Austria'
			WHEN w.page_urlpath ILIKE '/business_es-%' THEN 'Spain'
			WHEN w.page_urlpath ILIKE '/business-%' THEN 'Germany'
			WHEN w.page_urlpath ILIKE '/business_at-%' THEN 'Austria'
			WHEN w.page_urlpath ILIKE '/business_nl-%' THEN 'Netherlands'
			WHEN w.page_urlpath ILIKE '/business_us-%' THEN 'United States'			
			WHEN st.country_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
			WHEN st.country_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
			WHEN st.country_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
			WHEN st.country_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
			WHEN st.country_name IS NULL AND s.geo_country = 'US' THEN 'United States'
	   		ELSE COALESCE(st.country_name,'Germany')
	  		END AS country,
		w.store_id,
		w.anonymous_id,
		w.marketing_source,
		w.marketing_medium ,
		CASE WHEN w.page_url ILIKE '%business%' THEN 1 ELSE 0 END AS is_business,
		ROW_NUMBER() OVER (PARTITION BY w.session_id ORDER BY w.page_view_start DESC) AS row_num
	FROM traffic.page_views w
	LEFT JOIN traffic.sessions s
		ON w.session_id = s.session_id 
	LEFT JOIN ods_production.store st
	    ON st.id = w.store_id
	WHERE w.page_view_date::date >= DATEADD('month',-6,current_date)	
)
, last_click_in_session AS (
	SELECT 
		session_id ,
		page_view_date,
		marketing_campaign,
		is_business,
		country,
		marketing_channel,
		marketing_content,
		anonymous_id,
		store_id,
		marketing_source,
		marketing_medium
	FROM prep
	WHERE row_num = 1 -- LAST click 
)
, last_click_in_session_weekly AS (
	SELECT 
		DATE_TRUNC('week',page_view_date) AS page_view_week,
		marketing_campaign,
		is_business,
		count(session_id) AS sessions_week
	FROM last_click_in_session
	GROUP BY 1,2,3
)
, total_numbers AS (
	SELECT 
		DATE_TRUNC('week',page_view_date) AS page_view_week,
		marketing_campaign,
		count(session_id) AS total_sessions
	FROM last_click_in_session 
	GROUP BY 1,2
)
-- we agreed to separate the weight of the costs in B2C and B2B based on the % of 
-- traffic in every week. So the % of sessions that the last page_view were in business and in B2C store
, representativeness AS (
	SELECT 
		p.page_view_week,
		p.marketing_campaign,
		p.is_business,
		sum(p.sessions_week) AS number_sessions,
		sum(p.sessions_week)::float/n.total_sessions::float AS representativeness_in_cost
	FROM last_click_in_session_weekly p
	LEFT JOIN total_numbers n 
		ON p.marketing_campaign = n.marketing_campaign
		AND p.page_view_week = n.page_view_week
	GROUP BY 1,2,3,n.total_sessions		
)
, user_traffic_daily AS (
	SELECT 
	sss.page_view_date::DATE AS reporting_date,
	COALESCE(sss.country,'Germany') AS country,
	COALESCE(sss.marketing_channel, 'n/a') AS marketing_channel,
	COALESCE(o.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed,
	COALESCE(LOWER(sss.marketing_campaign),'n/a') AS marketing_campaign,
	CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group,
	'Total' AS freelancer_split,
	'n/a' AS segment_b2b_customers,
	CASE WHEN sss.is_business = 1 THEN 'business_customer' ELSE 'normal_customer' END AS customer_type,
	COUNT(sss.session_id) AS traffic_daily_sessions,
	COUNT(DISTINCT sss.session_id) AS traffic_daily_unique_sessions,
	COUNT(DISTINCT COALESCE(cu.anonymous_id_new, sss.anonymous_id)) AS traffic_daily_unique_users
	FROM last_click_in_session sss
	LEFT JOIN marketing_channel_mapping o
	    ON sss.marketing_channel = o.marketing_channel
	    AND COALESCE(sss.marketing_source,'n/a') = o.marketing_source
	    AND COALESCE(sss.marketing_medium,'n/a') = o.marketing_medium
	LEFT JOIN traffic.snowplow_user_mapping cu
	    ON sss.anonymous_id = cu.anonymous_id 
	    AND sss.session_id = cu.session_id
	GROUP BY 1,2,3,4,5,6,7,8,9
)
,cost AS (
	SELECT
	  co.reporting_date
	 ,COALESCE(co.country,'n/a') AS country
	 ,COALESCE(ct.customer_type,'n/a') AS customer_type
	 ,CASE WHEN ct.customer_type = 'business_customer' THEN 'B2B Unknown Split'
	 	   WHEN ct.customer_type = 'normal_customer' THEN 'B2C customer'
	 	   ELSE 'No Info'
	 	   END AS freelancer_split
	 ,'n/a' AS segment_b2b_customers
 ,CASE WHEN COALESCE(co.channel_grouping,'n/a') = 'Paid Search' THEN
      CASE WHEN b.brand_non_brand = 'Brand' then 'Paid Search Brand'
           WHEN b.brand_non_brand = 'Non Brand' then 'Paid Search Non Brand'
           WHEN co.campaign_name ILIKE '%brand%' THEN 'Paid Search Brand'
           WHEN co.campaign_name ILIKE '%trademark%' THEN 'Paid Search Brand'
        ELSE 'Paid Search Non Brand' END
      WHEN COALESCE(co.channel_grouping,'n/a') = 'Display' THEN 
      CASE WHEN co.campaign_name ILIKE '%awareness%' OR co.campaign_name ILIKE '%traffic%' THEN 'Display Branding' 
        ELSE 'Display Performance' END 
      WHEN COALESCE(co.channel_grouping,'n/a') = 'Paid Social' THEN 
      CASE WHEN co.campaign_name ILIKE '%brand%' THEN 'Paid Social Branding'
      ELSE 'Paid Social Performance' END
    ELSE COALESCE(co.channel_grouping,'n/a')
  END AS marketing_channel
 ,COALESCE(CASE WHEN marketing_channel = 'Paid Search Non Brand' then channel || ' ' || 'Non Brand'
                WHEN marketing_channel = 'Paid Search Brand' then channel || ' ' || 'Brand'
                WHEN marketing_channel = 'Display Branding'  then channel || ' ' || 'Performance'
                WHEN marketing_channel = 'Display Performance' then channel || ' ' || 'Performance'
                WHEN marketing_channel = 'Paid Social Branding' then channel || ' ' || 'Performance'
                WHEN marketing_channel = 'Paid Social Performance' then channel || ' ' || 'Performance'
    ELSE co.channel END, 'n/a') AS marketing_channel_detailed
	 ,LOWER(COALESCE(co.campaign_name,'n/a')) AS marketing_campaign
	 ,CASE WHEN LOWER(COALESCE(CASE WHEN co.channel_grouping = 'Affiliates' THEN co.reporting_grouping_name 
	 		ELSE co.campaign_name END,'n/a')) ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END AS target_campaign
	 ,r.retention_group
	 ,SUM(CASE
	   WHEN r.retention_group = 'NEW'
	    THEN co.total_spent_eur * 0.8 * COALESCE(rs.representativeness_in_cost, 
	    			CASE WHEN ct.customer_type = 'business_customer' AND target_campaign = 'B2C' THEN 0 
	    				 WHEN ct.customer_type = 'normal_customer' AND target_campaign = 'B2B' THEN 0 
	    				 WHEN ct.customer_type = 'No Info' THEN 0 
	    				 ELSE 1 END)
	   ELSE co.total_spent_eur * 0.2 * COALESCE(rs.representativeness_in_cost,
	   				CASE WHEN ct.customer_type = 'business_customer' AND target_campaign = 'B2C' THEN 0 
	    				 WHEN ct.customer_type = 'normal_customer' AND target_campaign = 'B2B' THEN 0  
	   					 WHEN ct.customer_type = 'No Info' THEN 0 
	   					 ELSE 1 END)
	 END) AS cost
	FROM retention_group r
	CROSS JOIN customer_type ct 
	CROSS JOIN marketing.marketing_cost_daily_combined co
	LEFT JOIN marketing.campaigns_brand_non_brand b 
		on co.campaign_name = b.campaign_name
  	LEFT JOIN representativeness rs 
  		ON LOWER(rs.marketing_campaign) = LOWER(COALESCE(co.campaign_name,'n/a'))
  		AND CASE WHEN rs.is_business = 1 THEN 'business_customer' 
  			ELSE 'normal_customer' END = ct.customer_type
  		AND DATE_TRUNC('week',co.reporting_date) = rs.page_view_week
	WHERE co.reporting_date >= DATEADD('month',-6,current_date)
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,subs AS (
	SELECT
	  s.created_date::DATE AS reporting_date
	 ,COALESCE(o.store_country,'n/a') AS country
	 ,COALESCE(o.marketing_channel,'n/a') AS marketing_channel
	 ,COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
	 ,COALESCE(LOWER(omc.marketing_campaign),'n/a') AS marketing_campaign
	 ,COALESCE(o.new_recurring, 'n/a') AS retention_group
	 ,CASE
	 	WHEN COALESCE(s.customer_type,'No Info') = 'normal_customer' THEN 'B2C customer'
	 	WHEN COALESCE(s.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
	 	WHEN COALESCE(s.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
		WHEN COALESCE(s.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
	 	ELSE 'No Info'
	 END AS freelancer_split
	 , CASE WHEN u.full_name IN (
	 		'Kimberley Truong', 'Joel Bartz', 'Gabriel Toldi',
	 		'Stefan Ohneberg', 'Robert Perschon', 'Jordy van Mil','Maria Serrano',
	 		'Mirella Recchiuti', 'Joshua Simon','Brian Chung',
	 		'Philip Nasskau', 'Roman Spinner', 'Thomas Vandermuntert', 'Andre Abranches',
	 		'Julian Öztoprak','Hamza Gilani' , 'Sandra Picallo') THEN 'Sales'
	 		WHEN o.customer_type = 'business_customer' THEN 'Self-Service'
	 		ELSE 'n/a' END
	 		AS segment_b2b_customers
	 ,COALESCE(s.customer_type,'No Info') AS customer_type
	 ,SUM(s.minimum_term_months) AS minimum_term_months
	 ,COUNT(DISTINCT s.subscription_id) AS subscriptions
	 ,COUNT(DISTINCT s.customer_id) AS customers
	 ,SUM(s.subscription_value) as subscription_value
	 ,SUM(s.committed_sub_value + s.additional_committed_sub_value) as committed_subscription_value
	FROM master.subscription s
	  LEFT JOIN master.order o
	    ON s.order_id = o.order_id
	  LEFT JOIN ods_production.order_marketing_channel omc
	    ON s.order_id = omc.order_id
	  LEFT JOIN marketing_channel_mapping orm
	    ON omc.marketing_channel = orm.marketing_channel
	    AND COALESCE(omc.marketing_source,'n/a') = orm.marketing_source
	    AND COALESCE(omc.marketing_medium,'n/a') = orm.marketing_medium
	  LEFT JOIN master.customer mc 
	  	ON s.customer_id = mc.customer_id 
	  LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
		ON mc.company_type_name = fre.company_type_name
	  LEFT JOIN ods_b2b.account a 
        ON o.customer_id = a.customer_id
        AND o.customer_type = 'business_customer'
      LEFT JOIN ods_b2b."user" u
        ON u.user_id = a.account_owner
	WHERE s.created_date::DATE >= DATEADD('month',-6,current_date)
	GROUP BY 1,2,3,4,5,6,7,8,9
)
,cart_orders AS (
	SELECT
	  o.created_date::DATE AS reporting_date
	 ,COALESCE(o.store_country,'n/a') AS country
	 ,COALESCE(o.marketing_channel,'n/a') AS marketing_channel
	 ,COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
	 ,COALESCE(LOWER(omc.marketing_campaign),'n/a') AS marketing_campaign
	 ,COALESCE(o.new_recurring, 'n/a') AS retention_group
	 ,COALESCE(o.customer_type,'No Info') AS customer_type
	 ,CASE
	 	WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C customer'
	 	WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
	 	WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
		WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
	 	ELSE 'No Info'
	 END AS freelancer_split
	 , CASE WHEN u.full_name IN (
	 		'Kimberley Truong', 'Joel Bartz', 'Gabriel Toldi',
	 		'Stefan Ohneberg', 'Robert Perschon', 'Jordy van Mil','Maria Serrano',
	 		'Mirella Recchiuti', 'Joshua Simon','Brian Chung',
	 		'Philip Nasskau', 'Roman Spinner', 'Thomas Vandermuntert', 'Andre Abranches',
	 		'Julian Öztoprak','Hamza Gilani' , 'Sandra Picallo') THEN 'Sales'
	 		WHEN o.customer_type = 'business_customer' THEN 'Self-Service'
	 		ELSE 'n/a' END
	 		AS segment_b2b_customers
	 ,COUNT(DISTINCT CASE
	  WHEN o.cart_orders >= 1
	   THEN o.order_id
	  END) AS carts
	FROM master.order o
	  LEFT JOIN ods_production.order_marketing_channel omc
	    ON o.order_id = omc.order_id
	  LEFT JOIN marketing_channel_mapping orm
	    ON omc.marketing_channel = orm.marketing_channel
	    AND COALESCE(omc.marketing_source,'n/a') = orm.marketing_source
	    AND COALESCE(omc.marketing_medium,'n/a') = orm.marketing_medium
	  LEFT JOIN master.customer mc 
	  	ON o.customer_id = mc.customer_id 
	  LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
		ON mc.company_type_name = fre.company_type_name  
	  LEFT JOIN ods_b2b.account a 
        ON o.customer_id = a.customer_id
        AND o.customer_type = 'business_customer'
      LEFT JOIN ods_b2b."user" u
        ON u.user_id = a.account_owner
	WHERE o.created_date::DATE >= DATEADD('month',-6,current_date)
	GROUP BY 1,2,3,4,5,6,7,8,9	
)
,submitted_orders AS (
	SELECT
	  o.submitted_date::DATE AS reporting_date
	 ,COALESCE(o.store_country,'n/a') AS country
	 ,COALESCE(o.marketing_channel,'n/a') AS marketing_channel
	 ,COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed
	 ,COALESCE(LOWER(omc.marketing_campaign),'n/a') AS marketing_campaign
	 ,COALESCE(o.new_recurring, 'n/a') AS retention_group
	 ,COALESCE(o.customer_type,'No Info') AS customer_type
	 ,CASE
	 	WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C customer'
	 	WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
	 	WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
		WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
	 	ELSE 'No Info'
	 END AS freelancer_split
	 , CASE WHEN u.full_name IN (
	 		'Kimberley Truong', 'Joel Bartz', 'Gabriel Toldi',
	 		'Stefan Ohneberg', 'Robert Perschon', 'Jordy van Mil','Maria Serrano',
	 		'Mirella Recchiuti', 'Joshua Simon','Brian Chung',
	 		'Philip Nasskau', 'Roman Spinner', 'Thomas Vandermuntert', 'Andre Abranches',
	 		'Julian Öztoprak','Hamza Gilani' , 'Sandra Picallo') THEN 'Sales'
	 		WHEN o.customer_type = 'business_customer' THEN 'Self-Service'
	 		ELSE 'n/a' END
	 		AS segment_b2b_customers
	 ,COUNT(DISTINCT CASE
	   WHEN o.completed_orders >= 1
	    THEN o.order_id
	  END) AS submitted_orders
	 ,COUNT(DISTINCT
	   CASE
	    WHEN o.approved_date IS NOT NULL
	     THEN o.order_id
	  END) AS approved_orders
	 ,COUNT(DISTINCT
	   CASE
	    WHEN o.failed_first_payment_orders >= 1
	     THEN o.order_id
	  END) AS failed_first_payment_orders
	 ,COUNT(DISTINCT
	   CASE
	    WHEN o.cancelled_orders >= 1
	     THEN o.order_id
	  END) AS cancelled_orders
	 ,COUNT(DISTINCT
	   CASE
	    WHEN o.failed_first_payment_orders >= 1 OR o.cancelled_orders >= 1
	     THEN o.order_id
	  END) AS drop_off_orders
	 ,COUNT(DISTINCT
	   CASE
	    WHEN o.paid_orders >= 1
	     THEN o.order_id
	  END) AS paid_orders
	,COUNT(DISTINCT
	  CASE
	   WHEN o.new_recurring = 'NEW' AND o.paid_orders >= 1
	    THEN o.customer_id
	  END) AS new_customers
	,COUNT(DISTINCT
	  CASE
	   WHEN o.new_recurring = 'RECURRING' AND o.paid_orders >= 1
	    THEN o.customer_id
	  END) AS recurring_customers
	FROM master.order o
	  LEFT JOIN ods_production.order_marketing_channel omc
	    ON o.order_id = omc.order_id
	  LEFT JOIN marketing_channel_mapping orm
	    ON omc.marketing_channel = orm.marketing_channel
	    AND COALESCE(omc.marketing_source,'n/a') = orm.marketing_source
	    AND COALESCE(omc.marketing_medium,'n/a') = orm.marketing_medium
	  LEFT JOIN master.customer mc 
	  	ON o.customer_id = mc.customer_id 
	  LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
		ON mc.company_type_name = fre.company_type_name  
	  LEFT JOIN ods_b2b.account a 
        ON o.customer_id = a.customer_id
        AND o.customer_type = 'business_customer'
      LEFT JOIN ods_b2b."user" u
        ON u.user_id = a.account_owner
	WHERE o.created_date::DATE >= DATEADD('month',-6,current_date)
	GROUP BY 1,2,3,4,5,6,7,8,9	
)
,leads AS (
    SELECT event_time::DATE AS reporting_date,
           COALESCE(b.country_name, 'n/a') AS country,
           COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
           COALESCE(o.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed,
           COALESCE(lower(s.marketing_campaign), 'n/a') AS marketing_campaign,
           CASE WHEN s.session_index = 1 THEN 'NEW' ELSE 'RECURRING' END AS retention_group,
           CASE
			 	WHEN COALESCE(mc.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
			 	WHEN COALESCE(mc.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
				ELSE 'B2B Unknown Split'
			 END AS freelancer_split,
           'business_customer' AS customer_type,
           CASE WHEN u.full_name IN (
		 		'Kimberley Truong', 'Joel Bartz', 'Gabriel Toldi',
		 		'Stefan Ohneberg', 'Robert Perschon', 'Jordy van Mil','Maria Serrano',
		 		'Mirella Recchiuti', 'Joshua Simon','Brian Chung',
		 		'Philip Nasskau', 'Roman Spinner', 'Thomas Vandermuntert', 'Andre Abranches',
		 		'Julian Öztoprak','Hamza Gilani' , 'Sandra Picallo') THEN 'Sales'
		 		WHEN mc.customer_type = 'business_customer' THEN 'Self-Service'
		 		ELSE 'n/a' END
		 		AS segment_b2b_customers,
           COUNT(DISTINCT CASE WHEN event_name IN ('Email Entered', 'Business Account Created Submitted') THEN a.anonymous_id END) AS leads_registration,
           COUNT(DISTINCT CASE WHEN event_name = 'Lead Form Submitted' THEN a.anonymous_id END) AS leads_form
    FROM segment.track_events a
    	LEFT JOIN ods_production.store b on b.id = a.store_id
    	LEFT JOIN segment.sessions_web s on s.session_id = a.session_id
    	LEFT JOIN marketing_channel_mapping o
    	    ON s.marketing_channel = o.marketing_channel
    	    AND COALESCE(s.marketing_source,'n/a') = o.marketing_source
    	    AND COALESCE(s.marketing_medium,'n/a') = o.marketing_medium
    	LEFT JOIN ods_b2b.account aa
	        ON s.customer_id = aa.customer_id
    	LEFT JOIN ods_b2b."user" u
        	ON u.user_id = aa.account_owner
        LEFT JOIN master.customer mc 
	  		ON s.customer_id = mc.customer_id 
	 	LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
			ON mc.company_type_name = fre.company_type_name
    WHERE event_name IN ('Email Entered','Lead Form Submitted', 'Business Account Created Submitted')
      AND page_path ILIKE '%business%'
      AND event_time::DATE >= '2023-03-01'
      AND event_time::DATE >= DATEADD('month',-6,current_date)
    GROUP BY 1,2,3,4,5,6,7,8,9
)
, dates_active_subs AS (
	SELECT DISTINCT 
		 datum AS fact_date
	FROM public.dim_dates
	WHERE datum <= current_date
		AND datum::DATE >= DATEADD('month',-6,current_date)
)
, s AS (
	SELECT 
		s.subscription_id,
		s.store_label,
		orm.marketing_channel,
		o.new_recurring,
		o.customer_type,
		s.customer_id,
		s.subscription_value_eur AS subscription_value_euro, 
		s.end_date,
		s.fact_day,
		s.country_name AS country,
		s.subscription_value_lc AS actual_subscription_value,
		COALESCE(orm.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed,
		COALESCE(LOWER(omc.marketing_campaign),'n/a') AS marketing_campaign,
		 CASE
			 	WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C customer'
			 	WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
			 	WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
				WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
			 	ELSE 'No Info'
			 END AS freelancer_split,
		CASE WHEN u.full_name IN (
	 		'Kimberley Truong', 'Joel Bartz', 'Gabriel Toldi',
	 		'Stefan Ohneberg', 'Robert Perschon', 'Jordy van Mil','Maria Serrano',
	 		'Mirella Recchiuti', 'Joshua Simon','Brian Chung',
	 		'Philip Nasskau', 'Roman Spinner', 'Thomas Vandermuntert', 'Andre Abranches',
	 		'Julian Öztoprak','Hamza Gilani' , 'Sandra Picallo') THEN 'Sales'
	 		WHEN o.customer_type = 'business_customer' THEN 'Self-Service'
	 		ELSE 'n/a' END
	 		AS segment_b2b_customers
 	FROM ods_production.subscription_phase_mapping s 
	LEFT JOIN master.ORDER o
		ON o.order_id = s.order_id
	LEFT JOIN (SELECT DISTINCT 
				order_id, marketing_channel, marketing_source, marketing_medium, referer_url, marketing_campaign
			FROM ods_production.order_marketing_channel) omc
		ON o.order_id = omc.order_id 
	LEFT JOIN marketing_channel_mapping orm
	    ON omc.marketing_channel = orm.marketing_channel
	    AND COALESCE(omc.marketing_source,'n/a') = orm.marketing_source
	    AND COALESCE(omc.marketing_medium,'n/a') = orm.marketing_medium 
	LEFT JOIN master.customer mc 
	  	ON o.customer_id = mc.customer_id 
	LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
		ON mc.company_type_name = fre.company_type_name
	LEFT JOIN ods_b2b.account a 
        ON o.customer_id = a.customer_id
        AND o.customer_type = 'business_customer'
    LEFT JOIN ods_b2b."user" u
        ON u.user_id = a.account_owner
)
, active_subs_value AS (
	SELECT 
		fact_date AS reporting_date,
		COALESCE(s.country, 'n/a') AS country,
		COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
		COALESCE(s.marketing_channel_detailed, 'n/a') AS marketing_channel_detailed,
		COALESCE(s.marketing_campaign, 'n/a') AS marketing_campaign,
		COALESCE(s.new_recurring, 'n/a') AS retention_group,
		COALESCE(s.customer_type, 'No Info') AS customer_type,
		COALESCE(s.freelancer_split, 'No Info') AS freelancer_split,
		COALESCE(s.segment_b2b_customers, 'n/a') AS segment_b2b_customers,
		COUNT(DISTINCT s.subscription_id) AS active_subscriptions,
		COUNT(DISTINCT s.customer_id) AS active_customers,
		SUM(s.subscription_value_euro) as active_subscription_value,
		SUM(s.actual_subscription_value) as actual_subscription_value
	FROM dates_active_subs d
	LEFT JOIN s 
		ON d.fact_date::date >= s.fact_Day::date 
		AND d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
	WHERE country IS NOT NULL
		AND s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY 1,2,3,4,5,6,7,8,9
)
,dimensions_combined AS (
SELECT DISTINCT
  reporting_date
 ,country
 ,marketing_channel
 ,marketing_channel_detailed
 ,marketing_campaign
 ,retention_group
 ,customer_type
 ,freelancer_split
 ,segment_b2b_customers
FROM user_traffic_daily
--
UNION
--
SELECT DISTINCT
  reporting_date
 ,country
 ,marketing_channel
 ,marketing_channel_detailed
 ,marketing_campaign
 ,retention_group
 ,customer_type
 ,freelancer_split
 ,segment_b2b_customers
FROM cost
--
UNION
--
SELECT DISTINCT
  reporting_date
 ,country
 ,marketing_channel
 ,marketing_channel_detailed
 ,marketing_campaign
 ,retention_group
 ,customer_type
 ,freelancer_split
 ,segment_b2b_customers
FROM subs
--
UNION
--
SELECT DISTINCT
  reporting_date
 ,country
 ,marketing_channel
 ,marketing_channel_detailed
 ,marketing_campaign
 ,retention_group
 ,customer_type
 ,freelancer_split
 ,segment_b2b_customers
FROM cart_orders
--
UNION
--
SELECT DISTINCT
  reporting_date
 ,country
 ,marketing_channel
 ,marketing_channel_detailed
 ,marketing_campaign
 ,retention_group
 ,customer_type
 ,freelancer_split
 ,segment_b2b_customers
FROM submitted_orders
--
UNION
--
SELECT DISTINCT
  reporting_date
 ,country
 ,marketing_channel
 ,marketing_channel_detailed
 ,marketing_campaign
 ,retention_group
 ,customer_type
 ,freelancer_split
 ,segment_b2b_customers
FROM leads
--
UNION
--
SELECT DISTINCT
  reporting_date
 ,country
 ,marketing_channel
 ,marketing_channel_detailed
 ,marketing_campaign
 ,retention_group
 ,customer_type
 ,freelancer_split
 ,segment_b2b_customers
FROM active_subs_value
)
, final_table AS (
SELECT
  dc.reporting_date
 ,dc.country
 ,dc.marketing_channel
 ,dc.marketing_channel_detailed
 ,dc.marketing_campaign
 ,dc.retention_group
 ,dc.customer_type
 ,dc.freelancer_split
 ,dc.segment_b2b_customers
 ,CASE WHEN dc.marketing_campaign ILIKE '%b2b%' THEN 'B2B' ELSE 'B2C' END AS target_campaign
 ,mca.brand_non_brand
 ,mca.cash_non_cash
 ,COALESCE(s.minimum_term_months ,0) AS minimum_term_months
 ,COALESCE(tr.traffic_daily_sessions ,0) AS traffic_daily_sessions
 ,COALESCE(tr.traffic_daily_unique_sessions ,0) AS traffic_daily_unique_sessions
 ,COALESCE(tr.traffic_daily_unique_users ,0) AS traffic_daily_unique_users
 ,COALESCE(co.carts ,0) AS cart_orders
 ,COALESCE(l.leads_registration ,0) AS leads_registration
 ,COALESCE(l.leads_form, 0) AS leads_form
 ,COALESCE(so.submitted_orders ,0) AS submitted_orders
 ,COALESCE(so.approved_orders ,0) AS approved_orders
 ,COALESCE(so.failed_first_payment_orders ,0) AS failed_first_payment_orders
 ,COALESCE(so.cancelled_orders ,0) AS cancelled_orders
 ,COALESCE(so.drop_off_orders ,0) AS drop_off_orders
 ,COALESCE(so.paid_orders ,0) AS paid_orders
 ,COALESCE(s.subscriptions ,0) AS subscriptions
 ,COALESCE(s.subscription_value ,0) AS subscription_value
 ,COALESCE(s.committed_subscription_value ,0) AS committed_subscription_value
 ,COALESCE(s.customers ,0) AS customers
 ,COALESCE(so.new_customers ,0) AS new_customers
 ,COALESCE(so.recurring_customers ,0) AS recurring_customers
 ,COALESCE(c.cost ,0) AS cost
 ,COALESCE(asv.active_subscriptions, 0) AS active_subscriptions
 ,COALESCE(asv.active_customers, 0) AS active_customers
 ,COALESCE(asv.active_subscription_value, 0) AS active_subscription_value
 ,COALESCE(asv.actual_subscription_value, 0) AS actual_subscription_value
FROM dimensions_combined dc
  LEFT JOIN user_traffic_daily tr
    ON  dc.reporting_date = tr.reporting_date
    AND dc.country = tr.country
    AND dc.marketing_channel = tr.marketing_channel
    AND dc.marketing_channel_detailed = tr.marketing_channel_detailed
    AND dc.marketing_campaign = tr.marketing_campaign
    AND dc.retention_group = tr.retention_group
    AND dc.customer_type = tr.customer_type
    AND dc.freelancer_split = tr.freelancer_split
    AND dc.segment_b2b_customers = tr.segment_b2b_customers
  LEFT JOIN cart_orders co
    ON  dc.reporting_date = co.reporting_date
    AND dc.country = co.country
    AND dc.marketing_channel = co.marketing_channel
    AND dc.marketing_channel_detailed = co.marketing_channel_detailed
    AND dc.marketing_campaign = co.marketing_campaign
    AND dc.retention_group = co.retention_group
    AND dc.customer_type = co.customer_type
    AND dc.freelancer_split = co.freelancer_split
    AND dc.segment_b2b_customers = co.segment_b2b_customers
  LEFT JOIN submitted_orders so
    ON  dc.reporting_date = so.reporting_date
    AND dc.country = so.country
    AND dc.marketing_channel = so.marketing_channel
    AND dc.marketing_channel_detailed = so.marketing_channel_detailed
    AND dc.marketing_campaign = so.marketing_campaign
    AND dc.retention_group = so.retention_group
    AND dc.customer_type = so.customer_type
    AND dc.freelancer_split = so.freelancer_split
    AND dc.segment_b2b_customers = so.segment_b2b_customers
  LEFT JOIN leads l
  	ON  dc.reporting_date = l.reporting_date
  	AND dc.country = l.country
  	AND dc.marketing_channel = l.marketing_channel
  	AND dc.marketing_channel_detailed = l.marketing_channel_detailed
  	AND dc.marketing_campaign = l.marketing_campaign
  	AND dc.retention_group = l.retention_group
  	AND dc.customer_type = l.customer_type
  	AND dc.freelancer_split = l.freelancer_split
  	AND dc.segment_b2b_customers = l.segment_b2b_customers
  LEFT JOIN subs s
    ON  dc.reporting_date = s.reporting_date
    AND dc.country = s.country
    AND dc.marketing_channel = s.marketing_channel
    AND dc.marketing_channel_detailed = s.marketing_channel_detailed
    AND dc.marketing_campaign = s.marketing_campaign
    AND dc.retention_group = s.retention_group
    AND dc.customer_type = s.customer_type
    AND dc.freelancer_split = s.freelancer_split
    AND dc.segment_b2b_customers = s.segment_b2b_customers
  LEFT JOIN cost c
    ON  dc.reporting_date = c.reporting_date
    AND dc.country = c.country
    AND dc.marketing_channel = c.marketing_channel
    AND dc.marketing_channel_detailed = c.marketing_channel_detailed
    AND dc.marketing_campaign = c.marketing_campaign
    AND dc.retention_group = c.retention_group
    AND dc.customer_type = c.customer_type
    AND dc.freelancer_split = c.freelancer_split
    AND dc.segment_b2b_customers = c.segment_b2b_customers
  LEFT JOIN active_subs_value asv 
    ON  dc.reporting_date = asv.reporting_date
    AND dc.country = asv.country
    AND dc.marketing_channel = asv.marketing_channel
    AND dc.marketing_channel_detailed = asv.marketing_channel_detailed
    AND dc.marketing_campaign = asv.marketing_campaign
    AND dc.retention_group = asv.retention_group
    AND dc.customer_type = asv.customer_type
    AND dc.freelancer_split = asv.freelancer_split
    AND dc.segment_b2b_customers = asv.segment_b2b_customers
  LEFT JOIN marketing_channel_attributes mca
    ON dc.marketing_channel = mca.channel_grouping   
)
SELECT *
FROM final_table
WHERE (minimum_term_months <> 0
OR  traffic_daily_sessions <> 0
 OR  traffic_daily_unique_sessions <> 0
 OR  traffic_daily_unique_users <> 0
 OR  cart_orders <> 0
 OR  submitted_orders <> 0
 OR  approved_orders <> 0
 OR  failed_first_payment_orders <> 0
 OR  cancelled_orders <> 0
 OR  drop_off_orders <> 0
 OR  paid_orders <> 0
 OR  subscriptions <> 0
 OR  subscription_value <> 0
 OR  committed_subscription_value <> 0
 OR  customers <> 0
 OR  new_customers <> 0
 OR  recurring_customers <> 0
 OR  cost <> 0   
 OR active_subscriptions <> 0
 OR active_customers <> 0
 OR active_subscription_value <> 0
 OR actual_subscription_value <> 0
 OR leads_registration <> 0
 OR leads_form <> 0)
 AND reporting_date::date < current_date::date  
;

DROP TABLE IF EXISTS dm_marketing.marketing_metrics_by_customer_type_and_mkt_campaigns;
CREATE TABLE dm_marketing.marketing_metrics_by_customer_type_and_mkt_campaigns AS
SELECT *
FROM tmp_marketing_metrics_by_customer_type_and_mkt_campaigns;

GRANT ALL ON ALL TABLES IN SCHEMA MARKETING TO GROUP BI;
GRANT select ON ALL TABLES IN SCHEMA dm_marketing TO redash_growth;

GRANT SELECT ON dm_marketing.marketing_metrics_by_customer_type_and_mkt_campaigns TO tableau;
