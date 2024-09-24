CREATE OR REPLACE VIEW dm_marketing.v_orders_anytouch_report AS	
WITH total_sessions_per_order AS (
 	SELECT 
 		o.order_id,
 		count(DISTINCT o.session_id) AS total_sessions
 	FROM traffic.session_order_mapping o
 	WHERE DATE_TRUNC('month',submitted_date) >= DATE_ADD('month', -6, current_date)
 	GROUP BY 1	
)
, total_sessions_per_channel AS (
	SELECT DISTINCT 
		o.order_id,
		tt.total_sessions,
		o.marketing_channel,
		o.marketing_medium ,
		o.marketing_source,
		t.marketing_campaign,
		count(DISTINCT o.session_id) AS total_sessions_by_channel,
		COUNT(*) OVER (PARTITION BY  o.order_id) AS number_channels_per_order
 	FROM traffic.session_order_mapping o
 	LEFT JOIN total_sessions_per_order tt 
 		ON o.order_id = tt.order_id
 	LEFT JOIN traffic.sessions t
 		ON o.session_id = t.session_id
 	WHERE DATE_TRUNC('month',o.submitted_date) >= DATE_ADD('month', -6, current_date)
 	GROUP BY 1,2,3,4,5,6
 )
, first_mkt_capaign AS (
	SELECT DISTINCT 
		o.order_id,
 		FIRST_VALUE(s.marketing_campaign) OVER (PARTITION BY o.order_id ORDER BY s.session_start
    		ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_touchpoint_mkt_campaign
	FROM traffic.session_order_mapping o
	LEFT JOIN traffic.sessions s
		ON o.session_id = s.session_id
 	WHERE DATE_TRUNC('month',o.submitted_date) >= DATE_ADD('month', -6, current_date)
 )
, products_orders AS ( 
	SELECT 
		s.order_id,
		s.category_name,
		SUM(s.quantity) AS total_products,
		SUM(s.price) AS total_price
	FROM ods_production.order_item s 
	LEFT JOIN master.ORDER o 
		ON s.order_id = o.order_id
	WHERE DATE_TRUNC('month',o.submitted_date) >= DATE_ADD('month', -6, current_date)
	GROUP BY 1,2
)
, row_order_category AS (  
	SELECT 
		o.order_id, 
		o.category_name, 
		ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.total_price DESC, o.total_products DESC, category_name) AS rowno
	FROM products_orders o
)
, orders_details AS (
	SELECT 
		oo.order_id,
		oo.submitted_date::date,
		oo.paid_orders,
		oo.customer_id,
		oo.new_recurring,
		CASE WHEN oo.customer_type = 'business_customer' THEN 'B2B'
			 WHEN oo.customer_type = 'normal_customer' THEN 'B2C'
			 ELSE 'n/a'
			 END AS customer_type,
		oo.store_country,
		oo.avg_plan_duration,
		rc.category_name,
		c.subscription_id,
		COALESCE(oc.first_touchpoint,'n/a') AS first_touchpoint_traffic,
		COALESCE(oc.first_touchpoint_mkt_source,'n/a') AS first_touchpoint_mkt_source_traffic,
		COALESCE(oc.first_touchpoint_mkt_medium,'n/a') AS first_touchpoint_mkt_medium_traffic,
		COALESCE(fm.first_touchpoint_mkt_campaign,'n/a') AS first_touchpoint_mkt_campaign_traffic,
		COALESCE(oc.last_touchpoint_excl_direct,'n/a') AS last_touchpoint_traffic,
		COALESCE(oc.last_touchpoint_excl_direct_mkt_source,'n/a') AS last_touchpoint_mkt_source_traffic,
		COALESCE(oc.last_touchpoint_excl_direct_mkt_medium,'n/a') AS last_touchpoint_mkt_medium_traffic,
		COALESCE(oc.last_touchpoint_excl_direct_marketing_campaign, 'n/a') AS last_touchpoint_mkt_campaign_traffic,
		COALESCE(m.marketing_channel, 'n/a') AS marketing_channel_any_touch,
		COALESCE(m.marketing_source, 'n/a') AS marketing_source_any_touch,
		COALESCE(m.marketing_medium, 'n/a') AS marketing_medium_any_touch,
		COALESCE(m.marketing_campaign, 'n/a') AS marketing_campaign_any_touch,
		CASE WHEN oo.completed_orders > 0  THEN 1 END AS submitted_orders_any_touch, 
				--IF 1 ORDER had 10 sessions with different channels, we will SHOW AS 10 orders. Giacomo asked FOR this
		CASE WHEN oo.completed_orders > 0 
	    			THEN (CASE WHEN COALESCE(m.total_sessions_by_channel::float, 0) = 0 THEN 1 ELSE m.total_sessions_by_channel::float END)::float /
	    					(CASE WHEN COALESCE(m.total_sessions, 0) = 0 THEN 1 ELSE m.total_sessions END)::float/
	    					(COUNT(*) OVER (PARTITION BY  oo.order_id))::float *
	    					(CASE WHEN COALESCE(m.number_channels_per_order::float, 0) = 0 THEN 1 ELSE m.number_channels_per_order::float END)::float 			
	    	ELSE 0 END AS submitted_orders_any_touch_normalized ,
	    CASE WHEN oo.completed_orders > 0  THEN 1::float / (COUNT(*) OVER (PARTITION BY  oo.order_id))::float END AS submitted_orders_first_or_last_touch, 	
	    CASE WHEN c.subscription_id IS NOT NULL  THEN 1 END AS subscriptions_any_touch,
    	CASE WHEN c.subscription_id IS NOT NULL
    			THEN (CASE WHEN COALESCE(m.total_sessions_by_channel::float, 0) = 0 THEN 1 ELSE m.total_sessions_by_channel::float END)::float /
    					(CASE WHEN COALESCE(m.total_sessions, 0) = 0 THEN 1 ELSE m.total_sessions END)::float/
    					(COUNT(*) OVER (PARTITION BY  c.subscription_id))::float *
    					(CASE WHEN COALESCE(m.number_channels_per_order::float, 0) = 0 THEN 1 ELSE m.number_channels_per_order::float END)::float
    		ELSE 0 END AS number_subscriptions_any_touch_normalized,
    	CASE WHEN c.subscription_id IS NOT NULL THEN 1::float / (COUNT(*) OVER (PARTITION BY  c.subscription_id))::float END 
    		AS subscriptions_first_or_last_touch
	FROM master.ORDER oo
	LEFT JOIN ods_production.order_marketing_channel o 
		ON o.order_id = oo.order_id
	LEFT JOIN traffic.order_conversions oc  
		ON o.order_id = oc.order_id
	LEFT JOIN row_order_category rc 
		ON oo.order_id = rc.order_id 
		AND rc.rowno = 1
	LEFT JOIN total_sessions_per_channel m 
		ON oo.order_id = m.order_id
	LEFT JOIN first_mkt_capaign fm 
		ON oo.order_id = fm.order_id
	LEFT JOIN master.subscription c
		ON oo.order_id = c.order_id
	WHERE DATE_TRUNC('month',oo.submitted_date) >= DATE_ADD('month', -6, current_date)
		AND oo.submitted_date::date < current_date
)
, new_recurring AS (
	SELECT DISTINCT new_recurring FROM master.order
)
, cost AS (
	SELECT
		o.reporting_date::date,
		COALESCE(n.new_recurring, 'n/a') AS new_recurring,
		COALESCE(o.customer_type,'n/a') AS customer_type,
		COALESCE(o.country, 'n/a') AS store_country,
		CASE WHEN COALESCE(o.channel_grouping,'n/a') = 'Paid Search' THEN
		      CASE WHEN b.brand_non_brand = 'Brand' then 'Paid Search Brand'
		           WHEN b.brand_non_brand = 'Non Brand' then 'Paid Search Non Brand'
		           WHEN o.campaign_name ILIKE '%brand%' THEN 'Paid Search Brand'
		           WHEN o.campaign_name ILIKE '%trademark%' THEN 'Paid Search Brand'
		        ELSE 'Paid Search Non Brand' END
		    ELSE COALESCE(o.channel_grouping,'n/a')
		  END AS marketing_channel,
		'n/a'::varchar AS marketing_source,
		COALESCE(o.medium, 'n/a') AS marketing_medium,
		COALESCE(o.campaign_name, 'n/a') AS marketing_campaign,
		COALESCE(o.channel, 'n/a') AS mkt_channel_detail_cost,
	    SUM(CASE WHEN n.new_recurring = 'NEW' THEN  0.8 * o.total_spent_eur
	    		 WHEN n.new_recurring = 'RECURRING' THEN  0.2 * o.total_spent_eur
	    		 END) AS cost,
	    SUM(CASE WHEN o.cash_non_cash = 'Cash' THEN 
	    				CASE WHEN n.new_recurring = 'NEW' THEN  0.8 * o.total_spent_eur
				    		 WHEN n.new_recurring = 'RECURRING' THEN  0.2 * o.total_spent_eur
				    		 END
				 END) AS cash_cost,
	    SUM(CASE WHEN o.cash_non_cash = 'Non-Cash' THEN 
	    				CASE WHEN n.new_recurring = 'NEW' THEN  0.8 * o.total_spent_eur
				    		 WHEN n.new_recurring = 'RECURRING' THEN  0.2 * o.total_spent_eur
				    		 END
				 END) AS non_cash_cost
	FROM marketing.marketing_cost_daily_combined o
	LEFT JOIN marketing.campaigns_brand_non_brand b 
	  	ON o.campaign_name = b.campaign_name
	CROSS JOIN new_recurring n 
	WHERE DATE_TRUNC('month',o.reporting_date) >= DATE_ADD('month', -6, current_date)
		AND o.reporting_date::date < current_date
	 GROUP BY 1,2,3,4,5,6,7,8,9
)
, traffic AS (
	 SELECT 
		s.session_start::date AS reporting_date,
		CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS new_recurring,
		CASE WHEN s.first_page_url ILIKE '%/business%' THEN 'B2B' ELSE 'B2C' END AS customer_type,
		CASE
	        WHEN s.first_page_url ILIKE '/de-%' THEN 'Germany'
	        WHEN s.first_page_url ILIKE '/us-%' THEN 'United States'
	        WHEN s.first_page_url ILIKE '/es-%' THEN 'Spain'
	        WHEN s.first_page_url ILIKE '/nl-%' THEN 'Netherlands'
	        WHEN s.first_page_url ILIKE '/at-%' THEN 'Austria'
	        WHEN s.first_page_url ILIKE '/business_es-%' THEN 'Spain'
	        WHEN s.first_page_url ILIKE '/business-%' THEN 'Germany'
	        WHEN s.first_page_url ILIKE '/business_at-%' THEN 'Austria'
	        WHEN s.first_page_url ILIKE '/business_nl-%' THEN 'Netherlands'
	        WHEN s.first_page_url ILIKE '/business_us-%' THEN 'United States'
	        WHEN s.store_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
	        WHEN s.store_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
	        WHEN s.store_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
	        WHEN s.store_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
	        WHEN s.store_name IS NULL AND s.geo_country = 'US' THEN 'United States'
	        WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
	        ELSE 'Germany' END
	        AS store_country,
		COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
		COALESCE(s.marketing_source, 'n/a') AS marketing_source,
		COALESCE(s.marketing_medium, 'n/a') AS marketing_medium,
		COALESCE(s.marketing_campaign, 'n/a')  AS marketing_campaign,
		'n/a'::varchar AS mkt_channel_detail_cost,
	    count(DISTINCT s.session_id) AS unique_sessions_traffic,
	    count(DISTINCT s.anonymous_id) AS unique_users_traffic
	FROM traffic.sessions s
	LEFT JOIN traffic.snowplow_user_mapping cu
		ON s.anonymous_id = cu.anonymous_id
		AND s.session_id = cu.session_id
	WHERE DATE_TRUNC('month',s.session_start) >= DATE_ADD('month', -6, current_date)
		AND s.session_start::date < current_date
	 GROUP BY 1,2,3,4,5,6,7,8,9
)
	SELECT 
		oo.submitted_date::date AS reporting_date,
		oo.new_recurring,
		oo.customer_type,
		oo.store_country,
		'n/a'::varchar AS mkt_channel_detail_cost,
		oo.category_name,
		oo.avg_plan_duration,
		oo.customer_id,
	FROM orders_details oo
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
, first_touch AS (
	SELECT 
		oo.submitted_date::date AS reporting_date,
		oo.new_recurring,
		oo.customer_type,
		oo.store_country,
		oo.first_touchpoint_traffic AS marketing_channel,
		oo.first_touchpoint_mkt_source_traffic AS marketing_source,
		oo.first_touchpoint_mkt_medium_traffic AS marketing_medium,
		oo.first_touchpoint_mkt_campaign_traffic AS marketing_campaign,
		'n/a'::varchar AS mkt_channel_detail_cost,
		oo.category_name,
		oo.avg_plan_duration,
		oo.customer_id,
		SUM(submitted_orders_first_or_last_touch) AS submitted_orders_first_touch,
		SUM(CASE WHEN oo.paid_orders > 0 THEN submitted_orders_first_or_last_touch END) AS paid_orders_first_touch,
		SUM(subscriptions_first_or_last_touch) AS subscriptions_first_touch,
		COUNT(DISTINCT CASE WHEN oo.new_recurring = 'NEW' and oo.paid_orders > 0 THEN oo.customer_id END) AS new_customers_first_touch
	FROM orders_details oo
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
, last_touch AS (
	SELECT 
		oo.submitted_date::date AS reporting_date,
		oo.new_recurring,
		oo.customer_type,
		oo.store_country,
		oo.last_touchpoint_traffic AS marketing_channel,
		oo.last_touchpoint_mkt_source_traffic AS marketing_source,
		oo.last_touchpoint_mkt_medium_traffic AS marketing_medium,
		oo.last_touchpoint_mkt_campaign_traffic AS marketing_campaign,
		'n/a'::varchar AS mkt_channel_detail_cost,
		oo.category_name,
		oo.avg_plan_duration,
		oo.customer_id,
		SUM(submitted_orders_first_or_last_touch) AS submitted_orders_last_touch,
		SUM(CASE WHEN oo.paid_orders > 0 THEN submitted_orders_first_or_last_touch END) AS paid_orders_last_touch,
		SUM(subscriptions_first_or_last_touch) AS subscriptions_last_touch,
		COUNT(DISTINCT CASE WHEN oo.new_recurring = 'NEW' and oo.paid_orders > 0  THEN oo.customer_id END) AS new_customers_last_touch
	FROM orders_details oo
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
, anytouch AS (
	SELECT 
		oo.submitted_date::date AS reporting_date,
		oo.new_recurring,
		oo.customer_type,
		oo.store_country,
		oo.marketing_channel_any_touch AS marketing_channel,
		oo.marketing_source_any_touch AS marketing_source,
		oo.marketing_medium_any_touch AS marketing_medium,
		oo.marketing_campaign_any_touch AS marketing_campaign,
		'n/a'::varchar AS mkt_channel_detail_cost,
		oo.category_name,
		oo.avg_plan_duration,
		oo.customer_id,
		SUM(oo.submitted_orders_any_touch) AS submitted_orders_any_touch, 
		SUM(oo.submitted_orders_any_touch_normalized) AS submitted_orders_any_touch_normalized ,
		SUM(CASE WHEN oo.paid_orders > 0 THEN oo.submitted_orders_any_touch END) AS paid_orders_any_touch,
		SUM(CASE WHEN oo.paid_orders > 0 THEN oo.submitted_orders_any_touch_normalized END) AS paid_orders_any_touch_normalized,
		SUM(oo.subscriptions_any_touch) AS subscriptions_any_touch,
	    SUM(oo.number_subscriptions_any_touch_normalized) AS subscriptions_any_touch_normalized,
	    COUNT(DISTINCT CASE WHEN oo.new_recurring = 'NEW' and oo.paid_orders > 0  THEN oo.customer_id END) AS new_customers_last_touch
	FROM orders_details oo
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)
, dimensions_combined AS (
	SELECT DISTINCT 
		reporting_date,
		new_recurring,
		customer_type,
		store_country,
		marketing_channel,
		marketing_source,
		marketing_medium,
		marketing_campaign,
		mkt_channel_detail_cost,
		customer_id,
		category_name::varchar,
		avg_plan_duration::varchar
	FROM anytouch
	UNION 
	SELECT DISTINCT 
		reporting_date,
		new_recurring,
		customer_type,
		store_country,
		marketing_channel,
		marketing_source,
		marketing_medium,
		marketing_campaign,
		mkt_channel_detail_cost,
		customer_id,
		category_name::varchar,
		avg_plan_duration::varchar
	FROM last_touch
	UNION 
	SELECT DISTINCT 
		reporting_date,
		new_recurring,
		customer_type,
		store_country,
		marketing_channel,
		marketing_source,
		marketing_medium,
		marketing_campaign,
		mkt_channel_detail_cost,
		customer_id,
		category_name::varchar,
		avg_plan_duration::varchar
	FROM first_touch
	UNION 
	SELECT DISTINCT 
		reporting_date,
		new_recurring,
		customer_type,
		store_country,
		marketing_channel,
		marketing_source,
		marketing_medium,
		marketing_campaign,
		mkt_channel_detail_cost,
		customer_id,
		category_name::varchar,
		avg_plan_duration::varchar
	UNION 
	SELECT DISTINCT 
		reporting_date,
		new_recurring,
		customer_type,
		store_country,
		marketing_channel,
		marketing_source,
		marketing_medium,
		marketing_campaign,
		mkt_channel_detail_cost,
		NULL::int AS customer_id,
		NULL::varchar AS category_name,
		NULL::varchar AS avg_plan_duration
	FROM cost
	UNION 
	SELECT DISTINCT 
		reporting_date,
		new_recurring,
		customer_type,
		store_country,
		marketing_channel,
		marketing_source,
		marketing_medium,
		marketing_campaign,
		mkt_channel_detail_cost,
		NULL::int AS customer_id,
		NULL::varchar AS category_name,
		NULL::varchar AS avg_plan_duration
	FROM traffic
)
SELECT 
	d.reporting_date,
	d.new_recurring,
	d.customer_type,
	d.store_country,
	d.marketing_channel,
	d.marketing_source,
	d.marketing_medium,
	d.marketing_campaign,
	d.mkt_channel_detail_cost,
	d.customer_id,
	d.category_name,
	d.avg_plan_duration,
	COALESCE(t.unique_sessions_traffic, 0) AS unique_sessions_traffic,
	COALESCE(t.unique_users_traffic, 0) AS unique_users_traffic,
	COALESCE(c.cost, 0) AS cost,
	COALESCE(c.cash_cost, 0) AS cash_cost,
	COALESCE(c.non_cash_cost, 0) AS non_cash_cost,
	COALESCE(f.submitted_orders_first_touch, 0) AS submitted_orders_first_touch,
	COALESCE(f.paid_orders_first_touch, 0) AS paid_orders_first_touch,
	COALESCE(f.subscriptions_first_touch, 0) AS subscriptions_first_touch,
	COALESCE(f.new_customers_first_touch, 0) AS new_customers_first_touch,
	COALESCE(l.submitted_orders_last_touch, 0) AS submitted_orders_last_touch,
	COALESCE(l.paid_orders_last_touch, 0) AS paid_orders_last_touch,
	COALESCE(l.subscriptions_last_touch, 0) AS subscriptions_last_touch,
	COALESCE(l.new_customers_last_touch, 0) AS new_customers_last_touch,
	COALESCE(a.submitted_orders_any_touch,  0) AS submitted_orders_any_touch,
	COALESCE(a.submitted_orders_any_touch_normalized , 0) AS submitted_orders_any_touch_normalized,
	COALESCE(a.paid_orders_any_touch, 0) AS paid_orders_any_touch,
	COALESCE(a.paid_orders_any_touch_normalized, 0) AS paid_orders_any_touch_normalized,
	COALESCE(a.subscriptions_any_touch, 0) AS subscriptions_any_touch,
	COALESCE(a.subscriptions_any_touch_normalized, 0) AS subscriptions_any_touch_normalized,
	COALESCE(a.new_customers_last_touch, 0) AS new_customers_any_touch
FROM dimensions_combined d
LEFT JOIN traffic t 
	ON d.reporting_date = t.reporting_date
	AND d.new_recurring = t.new_recurring
	AND d.customer_type = t.customer_type
	AND d.store_country = t.store_country
	AND d.marketing_channel = t.marketing_channel
	AND d.marketing_source = t.marketing_source
	AND d.marketing_medium = t.marketing_medium
	AND d.marketing_campaign = t.marketing_campaign
	AND d.mkt_channel_detail_cost = t.mkt_channel_detail_cost
	AND COALESCE(d.customer_id::varchar , 'n/a') = 'n/a'
	AND COALESCE(d.category_name::varchar, 'n/a') = 'n/a'
	AND COALESCE(d.avg_plan_duration::varchar, 'n/a') = 'n/a'
LEFT JOIN cost c
	ON d.reporting_date = c.reporting_date
	AND d.new_recurring = c.new_recurring
	AND d.customer_type = c.customer_type
	AND d.store_country = c.store_country
	AND d.marketing_channel = c.marketing_channel
	AND d.marketing_source = c.marketing_source
	AND d.marketing_medium = c.marketing_medium
	AND d.marketing_campaign = c.marketing_campaign
	AND d.mkt_channel_detail_cost = c.mkt_channel_detail_cost
	AND COALESCE(d.customer_id::varchar , 'n/a') = 'n/a'
	AND COALESCE(d.category_name::varchar, 'n/a') = 'n/a'
	AND COALESCE(d.avg_plan_duration::varchar, 'n/a') = 'n/a'
	ON d.reporting_date = g.reporting_date
	AND d.new_recurring = g.new_recurring
	AND d.customer_type = g.customer_type
	AND d.store_country = g.store_country
	AND d.marketing_channel = g.marketing_channel
	AND d.marketing_source = g.marketing_source
	AND d.marketing_medium = g.marketing_medium
	AND d.marketing_campaign = g.marketing_campaign
	AND d.mkt_channel_detail_cost = g.mkt_channel_detail_cost
	AND COALESCE(d.customer_id::varchar , 'n/a') = COALESCE(g.customer_id::varchar, 'n/a')
	AND COALESCE(d.category_name::varchar, 'n/a') = COALESCE(g.category_name::varchar, 'n/a')
	AND COALESCE(d.avg_plan_duration::varchar, 'n/a') = COALESCE(g.avg_plan_duration::varchar, 'n/a')
LEFT JOIN first_touch f
	ON d.reporting_date = f.reporting_date
	AND d.new_recurring = f.new_recurring
	AND d.customer_type = f.customer_type
	AND d.store_country = f.store_country
	AND d.marketing_channel = f.marketing_channel
	AND d.marketing_source = f.marketing_source
	AND d.marketing_medium = f.marketing_medium
	AND d.marketing_campaign = f.marketing_campaign
	AND d.mkt_channel_detail_cost = f.mkt_channel_detail_cost
	AND COALESCE(d.customer_id::varchar , 'n/a') = COALESCE(f.customer_id::varchar, 'n/a')
	AND COALESCE(d.category_name::varchar, 'n/a') = COALESCE(f.category_name::varchar, 'n/a')
	AND COALESCE(d.avg_plan_duration::varchar, 'n/a') = COALESCE(f.avg_plan_duration::varchar, 'n/a')
LEFT JOIN last_touch l
	ON d.reporting_date = l.reporting_date
	AND d.new_recurring = l.new_recurring
	AND d.customer_type = l.customer_type
	AND d.store_country = l.store_country
	AND d.marketing_channel = l.marketing_channel
	AND d.marketing_source = l.marketing_source
	AND d.marketing_medium = l.marketing_medium
	AND d.marketing_campaign = l.marketing_campaign
	AND d.mkt_channel_detail_cost = l.mkt_channel_detail_cost
	AND COALESCE(d.customer_id::varchar , 'n/a') = COALESCE(l.customer_id::varchar, 'n/a')
	AND COALESCE(d.category_name::varchar, 'n/a') = COALESCE(l.category_name::varchar, 'n/a')
	AND COALESCE(d.avg_plan_duration::varchar, 'n/a') = COALESCE(l.avg_plan_duration::varchar, 'n/a')
LEFT JOIN anytouch a
	ON d.reporting_date = a.reporting_date
	AND d.new_recurring = a.new_recurring
	AND d.customer_type = a.customer_type
	AND d.store_country = a.store_country
	AND d.marketing_channel = a.marketing_channel
	AND d.marketing_source = a.marketing_source
	AND d.marketing_medium = a.marketing_medium
	AND d.marketing_campaign = a.marketing_campaign
	AND d.mkt_channel_detail_cost = a.mkt_channel_detail_cost
	AND COALESCE(d.customer_id::varchar , 'n/a') = COALESCE(a.customer_id::varchar, 'n/a')
	AND COALESCE(d.category_name::varchar, 'n/a') = COALESCE(a.category_name::varchar, 'n/a')
	AND COALESCE(d.avg_plan_duration::varchar, 'n/a') = COALESCE(a.avg_plan_duration::varchar, 'n/a')
WITH NO SCHEMA BINDING;
	


