CREATE OR REPLACE VIEW dm_marketing.v_metrics_by_first_page_type AS
WITH traffic AS (
	SELECT 
		s.session_start::date AS reporting_date,
		CASE
	        WHEN s.first_page_url ILIKE '%/de-%' THEN 'Germany'
	        WHEN s.first_page_url ILIKE '%/us-%' THEN 'United States'
	        WHEN s.first_page_url ILIKE '%/es-%' THEN 'Spain'
	        WHEN s.first_page_url ILIKE '%/nl-%' THEN 'Netherlands'
	        WHEN s.first_page_url ILIKE '%/at-%' THEN 'Austria'
	        WHEN s.first_page_url ILIKE '%/business_es-%' THEN 'Spain'
	        WHEN s.first_page_url ILIKE '%/business-%' THEN 'Germany'
	        WHEN s.first_page_url ILIKE '%/business_at-%' THEN 'Austria'
	        WHEN s.first_page_url ILIKE '%/business_nl-%' THEN 'Netherlands'
	        WHEN s.first_page_url ILIKE '%/business_us-%' THEN 'United States'
	        WHEN s.store_name IS NULL AND s.geo_country = 'DE' THEN 'Germany'
	        WHEN s.store_name IS NULL AND s.geo_country = 'AT' THEN 'Austria'
	        WHEN s.store_name IS NULL AND s.geo_country = 'NL' THEN 'Netherlands'
	        WHEN s.store_name IS NULL AND s.geo_country = 'ES' THEN 'Spain'
	        WHEN s.store_name IS NULL AND s.geo_country = 'US' THEN 'United States'
	        WHEN s.store_name IN ('Germany', 'Spain', 'Austria', 'Netherlands', 'United States') THEN s.store_name
	        ELSE 'Germany' END AS country,
		COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
		COALESCE(s.marketing_medium, 'n/a') AS marketing_medium,
		COALESCE(s.marketing_source, 'n/a') AS marketing_source,
		COALESCE(s.marketing_term, 'n/a') AS marketing_term,
		COALESCE(s.marketing_content, 'n/a') AS marketing_content,
		COALESCE(s.marketing_campaign, 'n/a') AS marketing_campaign,
		CASE WHEN s.first_page_type IN ('pdp', 'brand', 'category', 'sub-category') THEN s.first_page_type
			 END AS first_page_type,
		NULLIF(upper(CASE WHEN s.first_page_type = 'pdp' THEN b.page_type_detail END), '') AS product_sku,
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(NULLIF(upper(CASE WHEN s.first_page_type IN ('sub-category', 'subcategory') THEN b.page_type_detail END), '')
			,'POINT AND SHOOT', 'POINT-AND-SHOOT' ),' AND ', ' & '), 'HI FI', 'HI-FI'), 'EMOBILITY', 'E-MOBILITY')
			, 'VACUUM', 'VACUUMS'), 'E READERS','E-READERS'), 'COMPUTERCOMPONENTS', 'COMPUTER COMPONENTS') AS subcategory_name,
		REPLACE(REPLACE(REPLACE(NULLIF(upper(CASE WHEN s.first_page_type = 'category' THEN b.page_type_detail END), '')
			, ' AND ', ' & '), 'TV & PROJECTORS' , 'TV AND PROJECTORS'), 'E MOBILITY','EMOBILITY') AS category_name,
		NULLIF(upper(CASE WHEN s.first_page_type = 'brand' THEN split_part(split_part(split_part(b.page_url, '/brands/', 2), '/',1), '?',1) END), '') AS brand,
				split_part(split_part(split_part(split_part(s.first_page_url, '/blog/', 2), '-',1), '/', 1), '_', 1) END, '') AS blog,
		count(DISTINCT s.session_id) AS sessions
	FROM traffic.sessions s
	LEFT JOIN segment.page_views_web b 
		ON s.session_id = b.session_id
		AND b.page_view_in_session_index = 1
	WHERE s.session_start::date >= DATEADD('month',-6,DATE_TRUNC('month', current_date))
		AND (s.first_page_type IN ('pdp', 'brand', 'category', 'sub-category')
		AND s.first_page_url NOT ILIKE '%locale%'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
, orders_prep AS (
	SELECT 
		o.submitted_date::date AS reporting_date,
		COALESCE(o.store_country, 'n/a') AS country,
		COALESCE(m.marketing_channel, 'n/a') AS marketing_channel,
		COALESCE(m.marketing_medium, 'n/a') AS marketing_medium,
		COALESCE(m.marketing_source, 'n/a') AS marketing_source,
		COALESCE(m.marketing_term, 'n/a') AS marketing_term,
		COALESCE(m.marketing_content, 'n/a') AS marketing_content,
		COALESCE(m.marketing_campaign, 'n/a') AS marketing_campaign,
		NULL::varchar AS first_page_type,
		o.order_id,
		o.paid_orders,
		o.new_recurring,
		upper(oi.product_sku) AS product_sku,
		SUM(oi.quantity) AS total_products,
        SUM(oi.total_price) AS total_price
	FROM master.ORDER o 
	LEFT JOIN ods_production.order_marketing_channel m 
	    ON o.order_id = m.order_id
	LEFT JOIN ods_production.order_item oi 
		ON o.order_id = oi.order_id
	WHERE o.submitted_date::date >= DATEADD('month',-6,DATE_TRUNC('month', current_date))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
)
, orders_rank AS (
	SELECT 
		o.reporting_date,
		o.country,
		o.marketing_channel,
		o.marketing_medium,
		o.marketing_source,
		o.marketing_term,
		o.marketing_content,
		o.marketing_campaign,
		o.first_page_type,
		o.order_id,
		o.paid_orders,
		o.new_recurring,
		o.product_sku,
		upper(p.subcategory_name) AS subcategory_name,
		upper(p.category_name) AS category_name,
		upper(p.brand) AS brand,
		ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o.total_price DESC, o.total_products DESC, o.product_sku) AS product_rank
	FROM orders_prep o
	LEFT JOIN ods_production.product p 
		ON o.product_sku = p.product_sku
)
, orders AS (
	SELECT 
		reporting_date,
		country,
		marketing_channel,
		marketing_medium,
		marketing_source,
		marketing_term,
		marketing_content,
		marketing_campaign,
		first_page_type,
		product_sku,
		subcategory_name,
		category_name,
		brand,
		'n/a'::varchar AS blog,
		count(order_id) AS submitted_orders,
		count(CASE WHEN paid_orders > 0 THEN order_id END ) AS paid_orders,
		count(CASE WHEN paid_orders > 0 AND new_recurring = 'NEW' THEN order_id  END) AS new_customers,
		count(CASE WHEN paid_orders > 0 AND new_recurring = 'RECURRING' THEN order_id  END) AS recurring_customers
	FROM orders_rank 
	WHERE product_rank = 1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
, subscriptions AS (
	SELECT 
		s.created_date::date AS reporting_date,
		COALESCE(s.country_name,'n/a') AS country,
	   	COALESCE(m.marketing_channel, 'n/a') AS marketing_channel,
		COALESCE(m.marketing_medium, 'n/a') AS marketing_medium,
		COALESCE(m.marketing_source, 'n/a') AS marketing_source,
		COALESCE(m.marketing_term, 'n/a') AS marketing_term,
		COALESCE(m.marketing_content, 'n/a') AS marketing_content,
		COALESCE(m.marketing_campaign, 'n/a') AS marketing_campaign,
		NULL::varchar AS first_page_type,
		upper(s.product_sku) AS product_sku,
		upper(s.subcategory_name) AS subcategory_name,
		upper(s.category_name) AS category_name,
		upper(s.brand) AS brand,
		'n/a'::varchar AS blog,
		count(DISTINCT s.subscription_id) AS subscriptions,
		SUM(s.subscription_value) as subscription_value,
	    SUM(s.committed_sub_value + s.additional_committed_sub_value) as committed_subscription_value	
	FROM master.subscription s 
	LEFT JOIN ods_production.order_marketing_channel m 
		    ON s.order_id = m.order_id
	WHERE s.created_date::date >= DATEADD('month',-6,DATE_TRUNC('month', current_date))
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
, dimensions AS (
	SELECT DISTINCT 
		reporting_date,
		country,
		marketing_channel,
		marketing_medium,
		marketing_source,
		marketing_term,
		marketing_content,
		marketing_campaign,
		first_page_type,
		product_sku,
		subcategory_name,
		category_name,
		brand,
		blog
	FROM traffic
	UNION 
	SELECT DISTINCT 
		reporting_date,
		country,
		marketing_channel,
		marketing_medium,
		marketing_source,
		marketing_term,
		marketing_content,
		marketing_campaign,
		first_page_type,
		product_sku,
		subcategory_name,
		category_name,
		brand,
		blog
	FROM orders
	UNION 
	SELECT DISTINCT 
		reporting_date,
		country,
		marketing_channel,
		marketing_medium,
		marketing_source,
		marketing_term,
		marketing_content,
		marketing_campaign,
		first_page_type,
		product_sku,
		subcategory_name,
		category_name,
		brand,
		blog
	FROM subscriptions
)
SELECT 
		d.reporting_date,
		d.country,
		d.marketing_channel,
		d.marketing_medium,
		d.marketing_source,
		d.marketing_term,
		d.marketing_content,
		d.marketing_campaign,
		d.first_page_type,
		d.product_sku,
		p.product_name,
		d.subcategory_name,
		d.category_name,
		d.brand,
		d.blog,
		t.sessions,
		o.submitted_orders,
		o.paid_orders,
		o.new_customers,
		o.recurring_customers,
		s.subscriptions,
		s.subscription_value,
	    s.committed_subscription_value
FROM dimensions d
LEFT JOIN traffic t 
	ON d.reporting_date = t.reporting_date
	AND d.country = t.country
	AND d.marketing_channel = t.marketing_channel
	AND d.marketing_medium = t.marketing_medium
	AND d.marketing_source = t.marketing_source
	AND d.marketing_term = t.marketing_term
	AND d.marketing_content = t.marketing_content
	AND d.marketing_campaign = t.marketing_campaign
	AND COALESCE(d.first_page_type, 'n/a') = COALESCE(t.first_page_type, 'n/a')
	AND COALESCE(d.product_sku, 'n/a') = COALESCE(t.product_sku, 'n/a')
	AND COALESCE(d.subcategory_name, 'n/a') = COALESCE(t.subcategory_name, 'n/a')
	AND COALESCE(d.category_name, 'n/a') = COALESCE(t.category_name, 'n/a')
	AND COALESCE(d.brand, 'n/a') = COALESCE(t.brand, 'n/a')
	AND COALESCE(d.blog, 'n/a') = COALESCE(t.blog, 'n/a')
LEFT JOIN orders o
	ON d.reporting_date = o.reporting_date
	AND d.country = o.country
	AND d.marketing_channel = o.marketing_channel
	AND d.marketing_medium = o.marketing_medium
	AND d.marketing_source = o.marketing_source
	AND d.marketing_term = o.marketing_term
	AND d.marketing_content = o.marketing_content
	AND d.marketing_campaign = o.marketing_campaign
	AND COALESCE(d.first_page_type, 'n/a') = COALESCE(o.first_page_type, 'n/a')
	AND COALESCE(d.product_sku, 'n/a') = COALESCE(o.product_sku, 'n/a')
	AND COALESCE(d.subcategory_name, 'n/a') = COALESCE(o.subcategory_name, 'n/a')
	AND COALESCE(d.category_name, 'n/a') = COALESCE(o.category_name, 'n/a')
	AND COALESCE(d.brand, 'n/a') = COALESCE(o.brand, 'n/a')
	AND COALESCE(d.blog, 'n/a') = COALESCE(o.blog, 'n/a')
LEFT JOIN subscriptions s
	ON d.reporting_date = s.reporting_date
	AND d.country = s.country
	AND d.marketing_channel = s.marketing_channel
	AND d.marketing_medium = s.marketing_medium
	AND d.marketing_source = s.marketing_source
	AND d.marketing_term = s.marketing_term
	AND d.marketing_content = s.marketing_content
	AND d.marketing_campaign = s.marketing_campaign
	AND COALESCE(d.first_page_type, 'n/a') = COALESCE(s.first_page_type, 'n/a')
	AND COALESCE(d.product_sku, 'n/a') = COALESCE(s.product_sku, 'n/a')
	AND COALESCE(d.subcategory_name, 'n/a') = COALESCE(s.subcategory_name, 'n/a')
	AND COALESCE(d.category_name, 'n/a') = COALESCE(s.category_name, 'n/a')
	AND COALESCE(d.brand, 'n/a') = COALESCE(s.brand, 'n/a')
	AND COALESCE(d.blog, 'n/a') = COALESCE(s.blog, 'n/a')
LEFT JOIN ods_production.product p  
	ON d.product_sku = p.product_sku	
WITH NO SCHEMA BINDING;
/* On this report, for example, if we have a subcategory page, teorically, we know the category, cause a subcategory can only have 1 category, 
but for traffic data, we want to leave it as null, due to how the Tableau report is built.
Cause in Tableau, if we select to filter for Category page, we are going to show only sessions where a Category Page was the first page of a session.
If we fill with the Category column (for Subcategory Pages), we would show also the Subcategory sessions and we do not want that.
*/
