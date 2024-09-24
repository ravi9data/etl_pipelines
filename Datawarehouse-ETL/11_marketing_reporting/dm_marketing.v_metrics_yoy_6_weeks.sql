CREATE OR REPLACE VIEW dm_marketing.v_metrics_yoy_6_weeks AS
WITH weeks_to_include AS (
	SELECT DISTINCT 
		DATE_TRUNC('week', dd.datum)::date AS reporting_date
	FROM public.dim_dates dd 
	WHERE CASE WHEN DATE_TRUNC('week', dd.datum) >= DATEADD('week', -6, DATE_TRUNC('week', current_date))
					AND dd.datum < current_date THEN 'Yes'
			   WHEN DATE_TRUNC('week', dd.datum) >= DATEADD('week',-6, DATEADD('year', -1,DATE_TRUNC('week',current_date)))
    				AND DATE_TRUNC('week', dd.datum) <= DATEADD('week',7, DATEADD('year', -1,DATE_TRUNC('week',current_date)))
					THEN 'Yes' 
			   WHEN DATE_TRUNC('week', dd.datum) >= DATEADD('week',-6, DATEADD('year', -2,DATE_TRUNC('week',current_date)))
    				AND DATE_TRUNC('week', dd.datum) <= DATEADD('week',7, DATEADD('year', -2,DATE_TRUNC('week',current_date)))
					THEN 'Yes' 			
					END = 'Yes'						
)
, retention_group AS (
	SELECT DISTINCT new_recurring AS retention_group
	FROM master.order
)
, orders AS (
	SELECT
		o.submitted_date::DATE AS reporting_date,
		COALESCE(o.store_country,'n/a') AS country,
		COALESCE(o.marketing_channel,'n/a') AS marketing_channel,
		COALESCE(omc.marketing_source,'n/a') AS marketing_source,
 		COALESCE(omc.marketing_medium,'n/a') AS marketing_medium,
 		COALESCE(omc.marketing_term, 'n/a') AS marketing_term,
 		COALESCE(omc.marketing_content, 'n/a') AS marketing_content,
 		COALESCE(LOWER(CASE WHEN o.created_date::DATE <= '2023-03-22' 
 									AND omc.marketing_channel IN ('Paid Social Branding', 'Paid Social Performance')
								THEN omc.marketing_content
	   						ELSE  omc.marketing_campaign
	 					END),'n/a') AS marketing_campaign,
		COALESCE(o.customer_type, 'n/a') AS customer_type,
		COALESCE(o.new_recurring, 'n/a') AS retention_group,
		COUNT(DISTINCT CASE WHEN o.completed_orders >= 1 THEN o.order_id END) AS submitted_orders,
		COUNT(DISTINCT CASE WHEN o.paid_orders >= 1 THEN o.order_id END) AS paid_orders,
		COUNT(DISTINCT CASE WHEN o.new_recurring = 'NEW' AND o.paid_orders > 0 THEN o.customer_id END) AS new_customers,
		COUNT(DISTINCT CASE WHEN o.paid_orders > 0 THEN o.customer_id END) AS customers
	FROM master.order o
	LEFT JOIN ods_production.order_marketing_channel omc
    	ON omc.order_id = o.order_id
	INNER JOIN weeks_to_include w 
		ON DATE_TRUNC('week',o.submitted_date) = w.reporting_date
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,cost AS (
	SELECT
		co.reporting_date::DATE AS reporting_date,
		COALESCE(co.country,'n/a') AS country,
		CASE WHEN COALESCE(co.channel_grouping,'n/a') = 'Paid Search' THEN
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
			 END AS marketing_channel, 
		'n/a'::varchar AS marketing_source,
 		COALESCE(co.medium,'n/a') AS marketing_medium,
 		lower(COALESCE(co.ad_set_name,'n/a')) AS marketing_term,
 		lower(COALESCE(co.ad_name,'n/a')) AS marketing_content,
 		LOWER(COALESCE(co.campaign_name,'n/a')) AS marketing_campaign,	
		CASE WHEN LOWER(COALESCE(CASE WHEN co.channel_grouping = 'Affiliates' THEN co.reporting_grouping_name 
	 								  ELSE co.campaign_name END,'n/a')) 
	 		ILIKE '%b2b%' THEN 'business_customer' ELSE 'normal_customer' END AS customer_type,	 
		r.retention_group,
		SUM(CASE
				WHEN r.retention_group = 'NEW' THEN co.total_spent_eur * 0.8
				ELSE co.total_spent_eur * 0.2
			END) AS cost,
		SUM(CASE
				WHEN r.retention_group = 'NEW' AND co.cash_non_cash = 'Cash' THEN co.total_spent_eur * 0.8
				WHEN co.cash_non_cash = 'Cash' THEN co.total_spent_eur * 0.2
			END) AS cost_cash,
		SUM(CASE
				WHEN r.retention_group = 'NEW' AND co.cash_non_cash = 'Non-Cash' THEN co.total_spent_eur * 0.8
				WHEN co.cash_non_cash = 'Non-Cash' THEN co.total_spent_eur * 0.2
			END) AS cost_non_cash
	FROM retention_group r
	CROSS JOIN marketing.marketing_cost_daily_combined co
	LEFT JOIN marketing.campaigns_brand_non_brand b
		ON co.campaign_name=b.campaign_name
	INNER JOIN weeks_to_include w 
		ON DATE_TRUNC('week',co.reporting_date) = w.reporting_date
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
, traffic AS (
	SELECT
		s.session_start::DATE AS reporting_date,
		CASE
		   WHEN st.country_name IS NULL
		    	AND s.geo_country='DE'
		     THEN 'Germany'
		   WHEN st.country_name IS NULL
		    	AND s.geo_country = 'AT'
		     THEN 'Austria'
		   WHEN st.country_name IS NULL
		    	AND s.geo_country = 'NL'
		     THEN 'Netherlands'
		   WHEN st.country_name IS NULL
		    	AND s.geo_country = 'ES'
		     THEN 'Spain'
		   WHEN st.country_name IS NULL
		    	AND s.geo_country = 'US'
		     THEN 'United States'
		   ELSE COALESCE(st.country_name,'Germany')
		  END AS country,
		COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
		COALESCE(s.marketing_source,'n/a') AS marketing_source,
 		COALESCE(s.marketing_medium,'n/a') AS marketing_medium,
 		COALESCE(s.marketing_term, 'n/a') AS marketing_term,
 		COALESCE(s.marketing_content, 'n/a') AS marketing_content,
 		COALESCE(s.marketing_campaign, 'n/a') AS marketing_campaign,		
		CASE WHEN st.store_name ILIKE '%b2b%' THEN 'business_customer'
			 WHEN s.first_page_url ILIKE '%/business%' THEN 'business_customer'
			 ELSE 'normal_customer' END AS customer_type,
		CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS retention_group,
		COUNT(DISTINCT s.session_id) AS traffic_daily_sessions
	FROM traffic.sessions s
	LEFT JOIN ods_production.store st
		ON st.id = s.store_id
	LEFT JOIN traffic.snowplow_user_mapping cu
		ON s.anonymous_id = cu.anonymous_id 
		AND s.session_id = cu.session_id
	INNER JOIN weeks_to_include w 
		ON DATE_TRUNC('week',s.session_start) = w.reporting_date
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
, committed_subs_value AS (
	SELECT
		s.created_date::DATE AS reporting_date,
		COALESCE(o.store_country,'n/a') AS country,
		COALESCE(o.marketing_channel,'n/a') AS marketing_channel,
		COALESCE(omc.marketing_source,'n/a') AS marketing_source,
		COALESCE(omc.marketing_medium,'n/a') AS marketing_medium,
 		COALESCE(omc.marketing_term, 'n/a') AS marketing_term,
 		COALESCE(omc.marketing_content, 'n/a') AS marketing_content,
		COALESCE(LOWER(CASE WHEN o.created_date::DATE <= '2023-03-22' 
									AND omc.marketing_channel IN ('Paid Social Branding', 'Paid Social Performance')
								THEN omc.marketing_content
							ELSE  omc.marketing_campaign
						END),'n/a') AS marketing_campaign,
		COALESCE(o.customer_type, 'n/a') AS customer_type,
		COALESCE(o.new_recurring, 'n/a') AS retention_group,
		SUM(s.committed_sub_value) as committed_subscription_value
	FROM master.order o
	LEFT JOIN master.subscription s
		ON s.order_id = o.order_id
	LEFT JOIN ods_production.order_marketing_channel omc
	    ON s.order_id = omc.order_id
	INNER JOIN weeks_to_include w 
		ON DATE_TRUNC('week',s.created_date) = w.reporting_date
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
, dimensions_combined AS (
	SELECT DISTINCT 
		reporting_date,
		country,
		marketing_channel,
		marketing_source,
		marketing_term,
		marketing_medium,
		marketing_content,
		marketing_campaign,	
		customer_type,
		retention_group
	FROM traffic
	UNION 
	SELECT DISTINCT 
		reporting_date,
		country,
		marketing_channel,
		marketing_source,
		marketing_term,
		marketing_medium,
		marketing_content,
		marketing_campaign,	
		customer_type,
		retention_group
	FROM cost
	UNION 
	SELECT DISTINCT 
		reporting_date,
		country,
		marketing_channel,
		marketing_source,
		marketing_term,
		marketing_medium,
		marketing_content,
		marketing_campaign,	
		customer_type,
		retention_group
	FROM orders
	UNION 
	SELECT DISTINCT 
		reporting_date,
		country,
		marketing_channel,
		marketing_source,
		marketing_term,
		marketing_medium,
		marketing_content,
		marketing_campaign,	
		customer_type,
		retention_group
	FROM committed_subs_value
)
, marketing_channel_attributes AS (
	SELECT DISTINCT
		channel_grouping,
		brand_non_brand
	FROM marketing.marketing_cost_channel_mapping mccm
	UNION
	SELECT
		'Paid Search Brand' AS channel_grouping,
		'Performance' AS brand_non_brand
	UNION
	SELECT
		'Paid Search Non Brand' AS channel_grouping,
		'Performance' AS brand_non_brand
	UNION
	SELECT
		'Paid Social Branding' AS channel_grouping,
		'Performance' AS brand_non_brand
	UNION
	SELECT
		'Paid Social Performance' AS channel_grouping,
		'Performance' AS brand_non_brand
	UNION
	SELECT
		'Display Branding' AS channel_grouping,
		'Performance' AS brand_non_brand
	UNION
	SELECT
		'Display Performance' AS channel_grouping,
		'Performance' AS brand_non_brand
)
SELECT 
	dc.reporting_date,
	date_part('week', dc.reporting_date) AS week_number,
	CASE WHEN DATE_TRUNC('week', dc.reporting_date) >= DATEADD('week',-8, DATE_TRUNC('week',current_date))
   				 AND dc.reporting_date <= current_date THEN 'This Year'
		WHEN DATE_TRUNC('week', dc.reporting_date) >= DATEADD('week',-8, DATEADD('year', -1,DATE_TRUNC('week',current_date)))
   				 AND DATE_TRUNC('week', dc.reporting_date) <= DATEADD('week',8, DATEADD('year', -1,DATE_TRUNC('week',current_date)))
        	THEN 'Previous Year' 
        WHEN DATE_TRUNC('week', dc.reporting_date) >= DATEADD('week',-8, DATEADD('year', -2,DATE_TRUNC('week',current_date)))
   				 AND DATE_TRUNC('week', dc.reporting_date) <= DATEADD('week',8, DATEADD('year', -2,DATE_TRUNC('week',current_date)))
        	THEN '2 Years Ago'    	
        END AS this_year_previous_year,
	dc.country,
	dc.marketing_channel,
	dc.marketing_source,
	dc.marketing_term,
	dc.marketing_medium,
	dc.marketing_content,
	dc.marketing_campaign,	
	dc.customer_type,
	dc.retention_group,
	mca.brand_non_brand,
	COALESCE(t.traffic_daily_sessions, 0) AS traffic_daily_sessions,
	COALESCE(c.cost, 0) AS cost,
	COALESCE(c.cost_cash, 0) AS cost_cash,
	COALESCE(c.cost_non_cash, 0) AS cost_non_cash,
	COALESCE(o.submitted_orders, 0) AS submitted_orders,
	COALESCE(o.paid_orders, 0) AS paid_orders,
	COALESCE(o.new_customers, 0) AS new_customers,
	COALESCE(o.customers, 0) AS customers,
	COALESCE(cs.committed_subscription_value,0) AS committed_subscription_value
FROM dimensions_combined dc
LEFT JOIN traffic t 
	ON dc.reporting_date = t.reporting_date
	AND dc.country = t.country
	AND dc.marketing_channel = t.marketing_channel
	AND dc.customer_type = t.customer_type
	AND dc.retention_group = t.retention_group
	AND dc.marketing_source = t.marketing_source
	AND dc.marketing_term = t.marketing_term
	AND dc.marketing_medium = t.marketing_medium
	AND dc.marketing_content = t.marketing_content
	AND dc.marketing_campaign = t.marketing_campaign
LEFT JOIN cost c
	ON dc.reporting_date = c.reporting_date
	AND dc.country = c.country
	AND dc.marketing_channel = c.marketing_channel
	AND dc.customer_type = c.customer_type
	AND dc.retention_group = c.retention_group
	AND dc.marketing_source = c.marketing_source
	AND dc.marketing_term = c.marketing_term
	AND dc.marketing_medium = c.marketing_medium
	AND dc.marketing_content = c.marketing_content
	AND dc.marketing_campaign = c.marketing_campaign
LEFT JOIN orders o
	ON dc.reporting_date = o.reporting_date
	AND dc.country = o.country
	AND dc.marketing_channel = o.marketing_channel
	AND dc.customer_type = o.customer_type
	AND dc.retention_group = o.retention_group
	AND dc.marketing_source = o.marketing_source
	AND dc.marketing_term = o.marketing_term
	AND dc.marketing_medium = o.marketing_medium
	AND dc.marketing_content = o.marketing_content
	AND dc.marketing_campaign = o.marketing_campaign
LEFT JOIN committed_subs_value cs
	ON dc.reporting_date = cs.reporting_date
	AND dc.country = cs.country
	AND dc.marketing_channel = cs.marketing_channel
	AND dc.customer_type = cs.customer_type
	AND dc.retention_group = cs.retention_group
	AND dc.marketing_source = cs.marketing_source
	AND dc.marketing_term = cs.marketing_term
	AND dc.marketing_medium = cs.marketing_medium
	AND dc.marketing_content = cs.marketing_content
	AND dc.marketing_campaign = cs.marketing_campaign
LEFT JOIN marketing_channel_attributes mca
    ON dc.marketing_channel = mca.channel_grouping  
WITH NO SCHEMA BINDING;
