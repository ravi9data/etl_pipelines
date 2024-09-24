SET enable_case_sensitive_identifier TO TRUE;

CREATE TEMP TABLE vector AS 
WITH fact_days AS (
	SELECT DISTINCT 
		datum AS fact_day
	FROM public.dim_dates d
	WHERE datum >= dateadd('month',-3,date_trunc('month',current_date)) 
		AND datum <= current_date
)
, product_countries AS (
	SELECT DISTINCT
		p.product_sku, 
		s.country_name AS country_name
	FROM ods_production.product p
	CROSS JOIN (SELECT DISTINCT country_name FROM ods_production.store WHERE country_name NOT IN ('Andorra', 'United Kingdom')) s
)
, marketing_channels AS (
	SELECT DISTINCT 
		o.marketing_channel AS marketing_channel_original,
		CASE 
			WHEN o.marketing_channel IN ('Display Branding','Display Performance')
				THEN 'Display'
			WHEN o.marketing_channel IN ('Offline Partnerships','Partnerships')
				THEN 'Partnerships'
			WHEN o.marketing_channel IN ('Paid Search Brand','Paid Search Non Brand')
				THEN 'Paid Search'
			WHEN o.marketing_channel IN ('Paid Social Branding','Paid Social Performance')
				THEN 'Paid Social'
			WHEN o.marketing_channel IN ('Organic Search','Organic Social')
				THEN 'Organic'
			WHEN o.marketing_channel IN ('Podcasts','Influencers','Refer Friend','Shopping','Referral','Other')
				THEN 'Other'
			ELSE o.marketing_channel
		END AS marketing_channel
	FROM master.ORDER o 
)
SELECT DISTINCT 
	d.fact_day,
	p.product_sku,
	p.country_name,
	mk.marketing_channel,
	s.customer_type 
FROM fact_days d
CROSS JOIN product_countries p
CROSS JOIN marketing_channels mk
CROSS JOIN (SELECT DISTINCT customer_type FROM master.subscription) s 
;


CREATE TEMP TABLE contentful_campaigns_timeframe_and_skus 
SORTKEY(
	product_sku,
	fact_date
)
DISTKEY(
	product_sku
)
AS
WITH cte AS (
	SELECT  
		*,
		json_parse(fields) AS f
	FROM pricing.contentful_snapshots cs
)
, landing_pages AS (
	SELECT 
		f."entryTitle".en::text,
		first_published_at::timestamp,
		f."metaKeyWords".en::date AS campaign_end_date,
		replace(f."slug".en::varchar,'"','') AS campaign_slug,
		published_at::timestamp,
		published_counter::int,
		published_version::int,
		f."storeCode".en::text AS store_code,
		items.sys.id::text as link_entry_id,
		ROW_NUMBER() OVER (PARTITION BY link_entry_id ORDER BY published_at DESC) AS row_no
	FROM cte t
	LEFT JOIN t.f."pageContent".en AS items ON TRUE	
	WHERE t.content_type = 'landingPage'
		AND store_code IN ('de','at','es','nl','us')
)
, product_widgets AS (
	SELECT 
		t.entry_id,
		f."title".en::text,
		first_published_at::timestamp,
		published_at::timestamp,
		published_counter::int,
		published_version::int,
		json_serialize(f."productIDs".en)::text AS product_ids
	FROM cte t
	LEFT JOIN t.f."pageContent".en AS items ON TRUE
	WHERE t.content_type = 'productListWidget'
)
, contentful_landing_pages AS (
	SELECT DISTINCT
		lp.en AS campaign_title,
		lp.campaign_end_date,
		MAX(lp.campaign_end_date) OVER (PARTITION BY lp.en, lp.store_code) AS final_campaign_end_date,
		lp.campaign_slug,
		lp.store_code AS country_name,
		lp.link_entry_id
	FROM landing_pages lp
	WHERE row_no = 1
)
, contentful_product_widgets AS (
	SELECT
		pw.*, 
		MAX(published_at) OVER (PARTITION BY entry_id)::date AS max_publish_date,
		ROW_NUMBER() OVER (PARTITION BY entry_id ORDER BY published_at DESC) AS rowno  
	FROM product_widgets pw
)
, contentful_raw AS (
	SELECT 
		d.datum AS fact_date,
		REPLACE(lp.campaign_slug,'-',' ') AS campaign_title,
		CASE 
			WHEN lp.country_name = 'de' THEN 'Germany'
			WHEN lp.country_name = 'at' THEN 'Austria'
			WHEN lp.country_name = 'es' THEN 'Spain'
			WHEN lp.country_name = 'nl' THEN 'Netherlands'
			WHEN lp.country_name = 'us' THEN 'United States'
		END AS store_country,
		p.product_sku
	FROM contentful_landing_pages lp 
	INNER JOIN contentful_product_widgets c
		ON c.entry_id = lp.link_entry_id
	LEFT JOIN ods_production.product p
		ON POSITION(p.product_id IN c.product_ids) > 0 
	LEFT JOIN public.dim_dates d
		ON d.datum BETWEEN c.first_published_at::date AND COALESCE(lp.final_campaign_end_date::date, CASE WHEN DATEADD('day', 3, c.max_publish_date) > current_date THEN current_date ELSE DATEADD('day', 3, c.max_publish_date) END)
	WHERE c.rowno = 1
	  AND c.first_published_at::date >= DATEADD('month', -12, date_trunc('month',current_date))
	  AND LEFT(SUBSTRING(c.product_ids FROM POSITION(p.product_id IN c.product_ids) - 1), 1) = '"' 
	  AND LEFT(SUBSTRING(c.product_ids FROM POSITION(p.product_id IN c.product_ids) + length(p.product_id)),1) = '"'
	  AND fact_date >= dateadd('month',-3,date_trunc('month',current_date)) 
)
SELECT 
	c.*,
	mc.marketing_channel,
	ct.customer_type
FROM contentful_raw c
CROSS JOIN (SELECT DISTINCT marketing_channel FROM vector) mc
CROSS JOIN (SELECT DISTINCT customer_type FROM vector) ct
;


CREATE TEMP TABLE tmp_commercial_metrics_overview AS 
WITH fact_days AS (
	SELECT DISTINCT fact_day
	FROM vector
)
, marketing_channels AS ( 
	SELECT DISTINCT 
		o.marketing_channel AS marketing_channel_original,
		CASE 
			WHEN o.marketing_channel IN ('Display Branding','Display Performance')
				THEN 'Display'
			WHEN o.marketing_channel IN ('Offline Partnerships','Partnerships')
				THEN 'Partnerships'
			WHEN o.marketing_channel IN ('Paid Search Brand','Paid Search Non Brand')
				THEN 'Paid Search'
			WHEN o.marketing_channel IN ('Paid Social Branding','Paid Social Performance')
				THEN 'Paid Social'
			WHEN o.marketing_channel IN ('Organic Search','Organic Social')
				THEN 'Organic'
			WHEN o.marketing_channel IN ('Podcasts','Influencers','Refer Friend','Shopping','Referral','Other')
				THEN 'Other'
			ELSE o.marketing_channel
		END AS marketing_channel
	FROM master.ORDER o 
)
, traffic_metrics AS ( 
	SELECT
		d.fact_day,
		st.country_name,
		pv.page_type_detail AS product_sku,
		mc.marketing_channel,	
		CASE 
			WHEN page_urlpath LIKE '%business%' THEN 'business_customer'
			ELSE 'normal_customer'
		END AS customer_type,
		count(DISTINCT pv.page_view_id) AS pageviews,
		count(DISTINCT pv.session_id) AS pageview_unique_sessions,
		count(DISTINCT CASE WHEN s.is_paid THEN pv.session_id END) paid_traffic_sessions,
		count(DISTINCT pv.anonymous_id) AS unique_users
	FROM fact_days d 
	LEFT JOIN traffic.page_views pv
		ON pv.page_view_date::date = d.fact_day
	LEFT JOIN traffic.sessions s
		ON pv.session_id = s.session_id
	LEFT JOIN marketing_channels mc
	    ON mc.marketing_channel_original = s.marketing_channel
	LEFT JOIN ods_production.product p
		ON p.product_sku = pv.page_type_detail
	LEFT JOIN ods_production.store st
	    ON  st.id = pv.store_id
	WHERE pv.page_view_start::date >= (SELECT min(fact_day) FROM vector)
		AND pv.page_type = 'pdp'
		AND pv.store_id IS NOT NULL
	GROUP BY 1,2,3,4,5
)
, orders_raw_created AS (
    SELECT
        o.order_id,
        o.store_country AS country_name,
        i.product_sku AS product_sku,
        o.created_date::date AS created_date,
        o.submitted_date::date AS submitted_date,
        mc.marketing_channel,
        COALESCE(o.customer_type, CASE WHEN o.store_commercial NOT LIKE '%B2B%' THEN 'normal_customer' ELSE 'business_customer' END) AS customer_type,
        count(*) OVER (PARTITION BY o.order_id) AS orders_number,
        count(DISTINCT o.order_id)/orders_number::float AS carts,
        count(DISTINCT CASE WHEN o.completed_orders >= 1 THEN o.order_id END)/orders_number::float AS completed_orders,
        count(DISTINCT CASE WHEN o.paid_orders >= 1 THEN o.order_id END)/orders_number::float AS paid_orders
    FROM ods_production.order_item i
             INNER JOIN master."order" o
                        ON i.order_id = o.order_id
             LEFT JOIN marketing_channels mc
                       ON mc.marketing_channel_original = o.marketing_channel
    WHERE o.created_date::date >= (SELECT min(fact_day) FROM vector)
    GROUP BY 1,2,3,4,5,6,7
)
, orders_raw_submitted_paid AS (
    SELECT
        o.order_id,
        o.store_country AS country_name,
        i.product_sku AS product_sku,
        o.created_date::date AS created_date,
        o.submitted_date::date AS submitted_date,
        mc.marketing_channel,
        COALESCE(o.customer_type, CASE WHEN o.store_commercial NOT LIKE '%B2B%' THEN 'normal_customer' ELSE 'business_customer' END) AS customer_type,
        count(*) OVER (PARTITION BY o.order_id) AS orders_number,
        count(DISTINCT o.order_id)/orders_number::float AS carts,
        count(DISTINCT CASE WHEN o.completed_orders >= 1 THEN o.order_id END)/orders_number::float AS completed_orders,
        count(DISTINCT CASE WHEN o.paid_orders >= 1 THEN o.order_id END)/orders_number::float AS paid_orders
    FROM ods_production.order_item i
             INNER JOIN master."order" o
                        ON i.order_id = o.order_id
             LEFT JOIN marketing_channels mc
                       ON mc.marketing_channel_original = o.marketing_channel
    WHERE o.submitted_date::date >= (SELECT min(fact_day) FROM vector)
    GROUP BY 1,2,3,4,5,6,7
)
   , cart_orders AS (
    SELECT
        created_date,
        country_name,
        product_sku,
        marketing_channel,
        customer_type,
        sum(carts) AS carts
    FROM orders_raw_created
    GROUP BY 1,2,3,4,5
)
   , completed_orders AS (
    SELECT
        submitted_date,
        country_name,
        product_sku,
        marketing_channel,
        customer_type,
        sum(completed_orders) AS completed_orders,
        sum(paid_orders) AS paid_orders
    FROM orders_raw_submitted_paid
    GROUP BY 1,2,3,4,5
)
, acquired_subs AS (
	SELECT
		start_date::date AS start_date,
		s.country_name AS country_name,
		s.product_sku AS product_sku,
		mc.marketing_channel,
		o.customer_type, 
		sum(original_subscription_value) AS acquired_subscription_value,
		sum(original_subscription_value_euro) AS acquired_subscription_value_euro,
		sum(committed_sub_value) AS acquired_committed_subscription_value,
		avg(subscription_value) AS avg_price,
		avg(rental_period) AS avg_duration,
		count(DISTINCT subscription_Id) AS acquired_subscriptions
	FROM master.subscription s
	LEFT JOIN master.order o 
		ON o.order_id = s.order_id 
	LEFT JOIN marketing_channels mc
	    ON mc.marketing_channel_original = o.marketing_channel
	WHERE s.start_date::date >= (SELECT min(fact_day) FROM vector)
	GROUP BY 1,2,3,4,5
) 
, acquired_subs_order_date AS (
	SELECT
		order_created_date::date AS order_created_date,
		s.country_name AS country_name,
		s.product_sku AS product_sku,
		mc.marketing_channel,
		o.customer_type, 
		sum(original_subscription_value) AS acquired_subscription_value_order_date,
		sum(original_subscription_value_euro) AS acquired_subscription_value_euro_order_date,
		sum(committed_sub_value) AS acquired_committed_subscription_value_order_date,
		avg(subscription_value) AS avg_price_order_date,
		avg(rental_period) AS avg_duration_order_date,
		count(DISTINCT subscription_id) AS acquired_subscriptions_order_date
	FROM master.subscription s
	LEFT JOIN master.order o 
		ON o.order_id = s.order_id 
	LEFT JOIN marketing_channels mc
	    ON mc.marketing_channel_original = o.marketing_channel
	WHERE s.order_created_date::date >= (SELECT min(fact_day) FROM vector)
	GROUP BY 1,2,3,4,5
)	
, cancelled_subs AS (
	SELECT
		cancellation_date::date AS cancellation_date,
		COALESCE(s.country_name, 'N/A') AS country_name,
		COALESCE(product_sku, 'N/A') AS product_sku,
		mc.marketing_channel,
		o.customer_type, 
		sum(subscription_value) AS cancelled_subscription_value,
		sum(subscription_value_euro) AS cancelled_subscription_value_euro,
		count(DISTINCT subscription_id) AS cancelled_subscriptions
	FROM ods_production.subscription s
	LEFT JOIN master.order o 
		ON o.order_id = s.order_id
	LEFT JOIN marketing_channels mc
	    ON mc.marketing_channel_original = o.marketing_channel
	WHERE s.cancellation_date::date >= (SELECT min(fact_day) FROM vector)
	GROUP BY 1,2,3,4,5
)
, last_phase_subscriptions AS (
	SELECT 
		DISTINCT 
			 subscription_id,
			 fact_day::date AS fact_day,
			 s.country_name,
			 s.customer_type,
			 s.product_sku,
			 mc.marketing_channel,
			 subscription_value_eur
	FROM ods_production.subscription_phase_mapping s
	LEFT JOIN master.order o 
		ON o.order_id = s.order_id 
	LEFT JOIN marketing_channels mc
	    ON mc.marketing_channel_original = o.marketing_channel
	LEFT JOIN ods_production.companies c2 
		ON c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping f 
		ON f.company_type_name = c2.company_type_name
	WHERE latest_phase_idx = 1
		AND s.status = 'ACTIVE'
		AND s.free_year_offer_taken IS TRUE 
)
, last_phase_before_change_of_price AS (
	SELECT 
		s.subscription_id,
		s.subscription_value_eur AS subscription_value_eur_before_free_year_offer
	FROM ods_production.subscription_phase_mapping s
	WHERE s.status = 'ACTIVE'
		AND latest_phase_idx = 2 --last phase before freeyear acceptance
		AND EXISTS (SELECT NULL FROM last_phase_subscriptions lp WHERE lp.subscription_id = s.subscription_id)
	GROUP BY 1,2
)
, subscription_price_changes AS (
	SELECT 
		s.fact_day AS last_change_date,
		s.country_name,
		s.customer_type,
		s.product_sku,
		s.marketing_channel,
		sum(subscription_value_eur) - sum(lp.subscription_value_eur_before_free_year_offer) AS decrease_from_free_year_offer
	FROM last_phase_subscriptions s
	LEFT JOIN last_phase_before_change_of_price lp
		ON lp.subscription_id = s.subscription_id
	GROUP BY 1,2,3,4,5
)
, pre_final AS (
	SELECT  
       v.fact_day AS fact_day, 
       v.product_sku AS product_sku, 
       v.country_name AS country_name,
       v.marketing_channel,
       v.customer_type,
       COALESCE(pageviews, 0) AS pageviews,
       COALESCE(pageview_unique_sessions, 0) AS pageview_unique_sessions,
       COALESCE(paid_traffic_sessions, 0) AS paid_traffic_sessions,
       COALESCE(unique_users,0) AS unique_users,
       COALESCE(carts, 0) AS carts,
       COALESCE(completed_orders, 0) AS completed_orders,
       COALESCE(paid_orders, 0) AS paid_orders,
       COALESCE(acquired_subscription_value, 0) AS acquired_subscription_value,
       COALESCE(acquired_subscription_value_euro, 0) AS acquired_subscription_value_euro,
       COALESCE(acquired_subscription_value_order_date, 0) AS acquired_subscription_value_order_date,
       COALESCE(acquired_subscription_value_euro_order_date, 0) AS acquired_subscription_value_euro_order_date,
       COALESCE(acquired_committed_subscription_value, 0) AS acquired_committed_subscription_value,
       COALESCE(acquired_committed_subscription_value_order_date, 0) AS acquired_committed_subscription_value_order_date,
       COALESCE(avg_price, 0) AS avg_price,
       COALESCE(avg_price_order_date, 0) AS avg_price_order_date,
       COALESCE(avg_duration, 0) AS avg_duration,
       COALESCE(avg_duration_order_date, 0) AS avg_duration_order_date,
       COALESCE(acquired_subscriptions, 0) AS acquired_subscriptions,
       COALESCE(acquired_subscriptions_order_date, 0) AS acquired_subscriptions_order_date,
       COALESCE(cancelled_subscription_value, 0) AS cancelled_subscription_value,
       COALESCE(cancelled_subscription_value_euro, 0) AS cancelled_subscription_value_euro,
       COALESCE(cancelled_subscriptions, 0) AS cancelled_subscriptions,
       COALESCE(decrease_from_free_year_offer,0) AS decrease_from_free_year_offer
	FROM vector v
	LEFT JOIN traffic_metrics t
		ON v.product_sku = t.product_sku
			AND v.fact_day = t.fact_day
			AND v.country_name = t.country_name
			AND v.marketing_channel = t.marketing_channel
			AND v.customer_type = t.customer_type 
	LEFT JOIN cart_orders o 
		ON v.product_sku = o.product_sku
			AND v.fact_day = o.created_date
			AND v.country_name = o.country_name
			AND v.marketing_channel = o.marketing_channel
			AND v.customer_type = o.customer_type 
	LEFT JOIN completed_orders co
		ON v.product_sku = co.product_sku
			AND v.fact_day = co.submitted_date
			AND v.country_name = co.country_name
			AND v.marketing_channel = co.marketing_channel
			AND v.customer_type = co.customer_type 
	LEFT JOIN acquired_subs a
		ON a.product_sku = v.product_sku
			AND a.start_date = v.fact_day
			AND a.country_name = v.country_name
			AND a.marketing_channel = v.marketing_channel
			AND a.customer_type = v.customer_type
	LEFT JOIN acquired_subs_order_date ao
		ON ao.product_sku = v.product_sku
			AND ao.order_created_date = v.fact_day
			AND ao.country_name = v.country_name
			AND ao.marketing_channel = v.marketing_channel
			AND ao.customer_type = v.customer_type
	LEFT JOIN cancelled_subs c 
		ON c.product_sku = v.product_sku
			AND c.cancellation_date = v.fact_day
			AND c.country_name = v.country_name
			AND c.marketing_channel = v.marketing_channel
			AND c.customer_type = v.customer_type
	LEFT JOIN subscription_price_changes pc
		ON pc.product_sku = v.product_sku
			AND pc.last_change_date = v.fact_day
			AND pc.country_name = v.country_name
			AND pc.marketing_channel = v.marketing_channel
			AND pc.customer_type = v.customer_type
)
, final_ AS (
	SELECT 
		ppf.fact_day,
		ppf.product_sku,
		CASE WHEN cc.product_sku IS NOT NULL AND cc.fact_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_sku_in_campaign_list,
	    cc.campaign_title,
		product_name,
	    brand,
	    category_name,
	    subcategory_name,
		ppf.country_name,
	   	ppf.marketing_channel,
	   	ppf.customer_type,
	   	ppf.pageviews,
	   	ppf.pageview_unique_sessions,
	   	ppf.paid_traffic_sessions,
	   	ppf.unique_users,
	   	ppf.carts,
	   	ppf.completed_orders,
	   	ppf.paid_orders,
	   	ppf.acquired_subscription_value,
	   	ppf.acquired_subscription_value_euro,
	   	ppf.acquired_committed_subscription_value,
	   	ppf.avg_price,
	   	ppf.avg_duration,
	   	ppf.acquired_subscriptions,
	   	ppf.acquired_subscription_value_order_date,
	   	ppf.acquired_subscription_value_euro_order_date,
	   	ppf.acquired_committed_subscription_value_order_date,
	   	ppf.avg_price_order_date,
	   	ppf.avg_duration_order_date,
	   	ppf.acquired_subscriptions_order_date,
	   	ppf.cancelled_subscription_value,
	   	ppf.cancelled_subscription_value_euro,
	   	ppf.cancelled_subscriptions,
	   	ppf.decrease_from_free_year_offer,
	   	count(*) OVER (PARTITION BY fact_day,ppf.product_sku,country_name,ppf.marketing_channel,ppf.customer_type) AS campaigns_number
	FROM pre_final ppf
	LEFT JOIN ods_production.product p 
		ON p.product_sku = ppf.product_sku
	LEFT JOIN contentful_campaigns_timeframe_and_skus cc
			ON cc.product_sku = ppf.product_sku
				AND cc.fact_date = ppf.fact_day
				AND cc.store_country = ppf.country_name
				AND cc.marketing_channel = ppf.marketing_channel
				AND cc.customer_type = ppf.customer_type
	WHERE 
		COALESCE(pageviews, 0) +
		COALESCE(pageview_unique_sessions, 0) +
		COALESCE(paid_traffic_sessions, 0) +
		COALESCE(unique_users, 0) +
		COALESCE(carts, 0) +
		COALESCE(completed_orders, 0) +
		COALESCE(paid_orders, 0) +
		COALESCE(acquired_subscription_value, 0) +
		COALESCE(acquired_subscription_value_euro, 0) +
		COALESCE(acquired_committed_subscription_value, 0) +
		COALESCE(avg_price, 0) +
		COALESCE(avg_duration, 0) +
		COALESCE(acquired_subscriptions, 0) +
		COALESCE(cancelled_subscription_value, 0) +
		COALESCE(cancelled_subscription_value_euro, 0) +
		COALESCE(cancelled_subscriptions, 0) +
		COALESCE(decrease_from_free_year_offer,0) +
		COALESCE(acquired_subscription_value_order_date,0) + 
		COALESCE(acquired_subscription_value_euro_order_date,0) +
		COALESCE(acquired_committed_subscription_value_order_date,0) +
		COALESCE(avg_price_order_date,0) +
		COALESCE(avg_duration_order_date,0) +
		COALESCE(acquired_subscriptions_order_date,0) <> 0 
)
, targets AS (
	SELECT 
		to_date AS fact_day,
		CASE
		    WHEN store_label = 'AT' THEN 'Austria'
		    WHEN store_label = 'ES' THEN 'Spain'
		    WHEN store_label = 'NL' THEN 'Netherlands'
		    WHEN store_label = 'US' THEN 'United States'
		    WHEN store_label = 'DE' THEN 'Germany'
	    END AS country_name,
		CASE 
			WHEN channel_type = 'B2B' THEN 'business_customer'
			WHEN channel_type IN ('B2C','Retail') THEN 'normal_customer'
		END AS customer_type,
		categories AS category_name,
		subcategories AS subcategory_name,
		sum(CASE 
				WHEN measures = 'Acquired ASV' THEN amount
			END) AS acquired_subscription_value_targets,
		sum(CASE 
				WHEN measures = 'Acquired Subscriptions' THEN amount
			END) AS acquired_subscription_targets,
		sum(CASE 
				WHEN measures = 'Cancelled ASV' THEN amount
			END) AS cancelled_subscription_value_targets,	
		sum(CASE 
				WHEN measures = 'Cancelled Subscriptions' THEN amount
			END) AS cancelled_subscription_targets,
		sum(CASE 
				WHEN measures = 'ASV' THEN amount
			END) AS active_subscription_value_targets
	FROM dm_commercial.r_commercial_daily_targets_since_2022
	WHERE fact_day >= (SELECT min(fact_day) FROM vector)
	GROUP BY 1,2,3,4,5
)
SELECT 	
	--dimensions
	COALESCE(f.fact_day,t.fact_day) AS fact_day,
	f.product_sku,
	f.is_sku_in_campaign_list,
	f.campaign_title,
	f.product_name,
	f.brand,
	COALESCE(f.category_name,t.category_name) AS category_name,
	COALESCE(f.subcategory_name,t.subcategory_name) AS subcategory_name,
	COALESCE(f.country_name,t.country_name) AS country_name,
	f.marketing_channel,
	COALESCE(f.customer_type,t.customer_type) AS customer_type,
   	--metrics
    pageviews/campaigns_number::float AS pageviews,
	pageview_unique_sessions/campaigns_number::float AS pageview_unique_sessions,
	paid_traffic_sessions/campaigns_number::float AS paid_traffic_sessions,
	unique_users/campaigns_number::float AS unique_users,
	carts/campaigns_number::float AS carts,
	completed_orders/campaigns_number::float AS completed_orders,
	paid_orders/campaigns_number::float AS paid_orders,
	acquired_subscription_value/campaigns_number::float AS acquired_subscription_value,
	acquired_subscription_value_euro/campaigns_number::float AS acquired_subscription_value_euro,
	acquired_committed_subscription_value/campaigns_number::float AS acquired_committed_subscription_value,
	avg_price/campaigns_number::float AS avg_price,
	avg_duration/campaigns_number::float AS avg_duration,
	acquired_subscriptions/campaigns_number::float AS acquired_subscriptions,
	cancelled_subscription_value/campaigns_number::float AS cancelled_subscription_value,
	cancelled_subscription_value_euro/campaigns_number::float AS cancelled_subscription_value_euro,
	cancelled_subscriptions/campaigns_number::float AS cancelled_subscriptions,
	acquired_subscription_value_order_date/campaigns_number::float AS acquired_subscription_value_order_date, --ADD NEW COLUMNS from here AND UPDATE
	acquired_subscription_value_euro_order_date/campaigns_number::float AS acquired_subscription_value_euro_order_date,
	acquired_committed_subscription_value_order_date/campaigns_number::float AS acquired_committed_subscription_value_order_date,
	avg_price_order_date/campaigns_number::float AS avg_price_order_date,
	avg_duration_order_date/campaigns_number::float AS avg_duration_order_date,
	acquired_subscriptions_order_date/campaigns_number::float AS acquired_subscriptions_order_date,
	decrease_from_free_year_offer/campaigns_number::float AS decrease_from_free_year_offer,
	--targets
	count(*) OVER (PARTITION BY 
						COALESCE(f.fact_day,t.fact_day),
						COALESCE(f.category_name,t.category_name),
						COALESCE(f.subcategory_name,t.subcategory_name),
						COALESCE(f.customer_type,t.customer_type),
						COALESCE(f.country_name,t.country_name)
					) AS unique_key_count,	
	t.acquired_subscription_value_targets/unique_key_count::float AS acquired_subscription_value_targets,
	t.acquired_subscription_targets/unique_key_count::float AS acquired_subscription_targets,
	t.cancelled_subscription_value_targets/unique_key_count::float AS cancelled_subscription_value_targets,
	t.cancelled_subscription_targets/unique_key_count::float AS cancelled_subscription_targets,
	t.active_subscription_value_targets/unique_key_count::float AS active_subscription_value_targets
FROM final_ f
FULL JOIN targets t 
	ON t.fact_day = f.fact_day 
		AND t.country_name = f.country_name 
		AND t.customer_type = f.customer_type 
		AND t.category_name = f.category_name 
		AND t.subcategory_name = f.subcategory_name
;

BEGIN TRANSACTION;

DELETE FROM dm_commercial.commercial_metrics_overview 
WHERE fact_day::date >= dateadd('month',-3,date_trunc('month',current_date));

INSERT INTO dm_commercial.commercial_metrics_overview
SELECT *
FROM tmp_commercial_metrics_overview;

END TRANSACTION;

DROP TABLE IF EXISTS tmp_commercial_metrics_overview;

DROP TABLE IF EXISTS contentful_campaigns_timeframe_and_skus;
