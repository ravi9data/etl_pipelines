DROP TABLE IF EXISTS tmp_mkt_sources;
CREATE TEMP TABLE tmp_mkt_sources AS 
SELECT
	marketing_source,
	count(*) AS sessions
FROM traffic.sessions 
WHERE marketing_channel = 'Affiliates'
	AND marketing_source IS NOT NULL
GROUP BY 1
HAVING sessions > 10000 --ONLY revelants mkt SOURCE
;


DROP TABLE IF EXISTS tmp_order_category;
CREATE TEMP TABLE tmp_order_category AS 
WITH affiliate_orders AS ( -- FOR costs
    SELECT order_id
    FROM marketing.marketing_cost_daily_affiliate_order m
    WHERE m.date::date BETWEEN '2021-10-18' AND '2023-01-14'
    	AND DATE_TRUNC('year',m.date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
--    
    UNION
--    
    SELECT order_id
    FROM marketing.affiliate_validated_orders m
    WHERE submitted_date::date >= '2023-01-15'
      AND commission_approval = 'APPROVED'
      AND DATE_TRUNC('year',submitted_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
--    
    UNION
    SELECT order_id
    FROM marketing.marketing_yakk_b2b_cost
    WHERE DATE_TRUNC('year',date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
)
, products_orders AS (
    SELECT
        o.order_id,
        o.marketing_channel,
                CASE WHEN o.marketing_channel = 'Affiliates' THEN COALESCE(NULLIF(o.marketing_campaign,'n/a'),LOWER(ts.website_name))
             ELSE av.publisher END AS marketing_campaign,
        lower(COALESCE(ms.marketing_source, 'Others')) AS marketing_source,
        CASE WHEN av.publisher IS NOT NULL THEN 1 ELSE 0 END AS is_affiliate_voucher,
        s.category_name ,
        s.product_sku ,
        s.product_name ,
        s.brand,
        o.created_date,
        o.submitted_date,
        o.store_country,
        o.paid_orders,
        o.voucher_code,
        o.new_recurring,
        o.customer_type,
        CASE WHEN o.marketing_channel = 'Affiliates' OR av.publisher IS NOT NULL THEN 1 ELSE 0 END is_order_from_orders_info,
        CASE WHEN ao.order_id IS NOT NULL THEN 1 ELSE 0 END is_order_from_costs_info,
		MAX(s.plan_duration) OVER ( PARTITION BY o.order_id, s.category_name) AS plan_duration, 
        SUM(s.quantity) OVER (partition by o.order_id, category_name) AS total_products_cat,
        SUM(s.total_price) OVER (partition by o.order_id, category_name) AS total_price_cat,
        SUM(s.quantity) AS total_products,
        SUM(s.total_price) AS total_price
    FROM master."order" o
    LEFT JOIN ods_production.order_marketing_channel omc 
	  	ON o.order_id = omc.order_id
	LEFT JOIN tmp_mkt_sources ms 
		ON omc.marketing_source = ms.marketing_source
    INNER JOIN ods_production.order_item s
		ON o.order_id = s.order_id
	LEFT JOIN marketing.affiliate_tradedoubler_submitted_orders ts 
		ON o.order_id = ts.order_id
	LEFT JOIN staging.affiliates_vouchers av
		ON o.voucher_code ILIKE av.voucher_prefix_code + '%'
	LEFT JOIN affiliate_orders ao 
		ON ao.order_id = o.order_id
    WHERE(o.marketing_channel = 'Affiliates'
            OR av.publisher IS NOT NULL
            OR ao.order_id IS NOT NULL)
      AND (DATE_TRUNC('year',o.created_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
      		OR 
      		DATE_TRUNC('year',o.submitted_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date)) )
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,s.plan_duration, s.quantity, s.total_price      
)
, products_category_rank AS (
    SELECT
        order_id,
        category_name,
        product_name,
        product_sku,
        brand,
        marketing_channel,
        marketing_campaign,
        marketing_source,
        is_affiliate_voucher,
        is_order_from_orders_info,
        is_order_from_costs_info,
        created_date,
        submitted_date,
        store_country,
        paid_orders,
        voucher_code,
        new_recurring,
        customer_type,
        plan_duration,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY total_price_cat DESC, total_products_cat DESC, category_name) AS category_rank,
        ROW_NUMBER() OVER (PARTITION BY order_id, category_name ORDER BY total_price DESC, total_products DESC, category_name) AS product_rank
    FROM products_orders    
)
SELECT
    cat.order_id,
    COALESCE(cat.category_name, 'n/a') AS category_name,
    COALESCE(cat.marketing_channel,'n/a') AS marketing_channel,
    COALESCE(cat.marketing_campaign,'n/a') AS marketing_campaign,
    cat.marketing_source,
    cat.is_affiliate_voucher,
    cat.is_order_from_orders_info,
    cat.is_order_from_costs_info,
    COALESCE(cat.product_sku,'n/a') AS product_sku,
    COALESCE(cat.product_name,'n/a') AS product_name,
    COALESCE(cat.brand,'n/a') AS brand,
    cat.created_date,
    cat.submitted_date,
    COALESCE(cat.store_country,'n/a') AS country,
    cat.paid_orders AS is_paid_order,
    COALESCE(cat.voucher_code , 'n/a') AS voucher_code,
    COALESCE(cat.new_recurring ,'n/a') AS new_recurring,
    COALESCE(cat.customer_type ,'n/a') AS customer_type,
    CASE WHEN cat.submitted_date IS NOT NULL THEN 1 ELSE 0 END AS is_submitted_order,
    cat.plan_duration
FROM products_category_rank cat
WHERE cat.category_rank = 1 
	AND cat.product_rank = 1  
;


DROP TABLE IF EXISTS tmp_mkt_campaigns_top_10_per_month;
CREATE TEMP TABLE tmp_mkt_campaigns_top_10_per_month AS 
    SELECT
        DATE_TRUNC('month',created_date)::DATE AS reporting_date,
        country,
        marketing_campaign,
        COUNT(DISTINCT CASE WHEN is_paid_order >= 1 THEN order_id END) AS total_paid_orders,
        ROW_NUMBER() OVER (PARTITION BY reporting_date, country ORDER BY total_paid_orders DESC) AS row_num
    FROM tmp_order_category
    WHERE is_order_from_orders_info = 1
    GROUP BY 1,2,3
;




DROP TABLE IF EXISTS tmp_orders_metrics_created_orders;
CREATE TEMP TABLE tmp_orders_metrics_created_orders AS 
    SELECT
        oc.category_name,
        oc.created_date::DATE AS reporting_date,
        oc.country,
        oc.marketing_campaign,
        oc.marketing_source,
        oc.marketing_channel,
        oc.is_affiliate_voucher,
        oc.voucher_code,
        oc.plan_duration,
        oc.product_sku,
        oc.product_name,
        oc.new_recurring,
        oc.customer_type,
        oc.brand,
        CASE WHEN mc.marketing_campaign IS NOT NULL THEN 1 ELSE 0 END AS is_top_10_mkt_campaign,
        oc.is_paid_order,
        oc.is_submitted_order,
        COUNT(DISTINCT oc.order_id) AS created_orders
    FROM tmp_order_category oc
	LEFT JOIN tmp_mkt_campaigns_top_10_per_month mc
		ON DATE_TRUNC('month',oc.created_date) = mc.reporting_date
		AND oc.country = mc.country
		AND oc.marketing_campaign = mc.marketing_campaign
		AND mc.row_num <= 10
	WHERE oc.is_order_from_orders_info = 1
		AND DATE_TRUNC('year',oc.created_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;


DROP TABLE IF EXISTS tmp_orders_metrics_submitted_orders;
CREATE TEMP TABLE tmp_orders_metrics_submitted_orders AS 
    SELECT
        oc.category_name,
        oc.submitted_date::DATE AS reporting_date,
        oc.country,
        oc.marketing_campaign,
        oc.marketing_source,
        oc.marketing_channel,
        oc.is_affiliate_voucher,
        oc.voucher_code,
        oc.plan_duration,
        oc.product_sku,
        oc.product_name,
        oc.new_recurring,
        oc.customer_type,
        oc.brand,
        CASE WHEN mc.marketing_campaign IS NOT NULL THEN 1 ELSE 0 END AS is_top_10_mkt_campaign,
        oc.is_paid_order,
        oc.is_submitted_order,
        COUNT(DISTINCT CASE WHEN oc.is_paid_order >= 1 THEN oc.order_id END) AS paid_orders,
        COUNT(DISTINCT CASE WHEN oc.is_submitted_order >= 1 THEN oc.order_id END) AS submitted_order
    FROM tmp_order_category oc
	LEFT JOIN tmp_mkt_campaigns_top_10_per_month mc
		ON DATE_TRUNC('month',oc.submitted_date) = mc.reporting_date
		AND oc.country = mc.country
		AND oc.marketing_campaign = mc.marketing_campaign
		AND mc.row_num <= 10
	WHERE oc.is_order_from_orders_info = 1
		AND DATE_TRUNC('year',oc.submitted_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;


DROP TABLE IF EXISTS tmp_orders_metrics_subscriptions;
CREATE TEMP TABLE tmp_orders_metrics_subscriptions AS 
    SELECT
        oc.category_name,
        oc.submitted_date::DATE AS reporting_date,
        oc.country,
        oc.marketing_campaign,
        oc.marketing_source,
        oc.marketing_channel,
        oc.is_affiliate_voucher,
        oc.voucher_code,
        oc.plan_duration,
        oc.product_sku,
        oc.product_name,
        oc.new_recurring,
        oc.customer_type,
        oc.brand,
        CASE WHEN mc.marketing_campaign IS NOT NULL THEN 1 ELSE 0 END AS is_top_10_mkt_campaign,
        oc.is_paid_order,
        oc.is_submitted_order,
        SUM(s2.subscription_value_euro) AS acquired_subscription_value,
        SUM(CASE WHEN s2.status = 'ACTIVE' THEN s2.subscription_value_euro END) AS active_subscription_value,
        SUM(s2.committed_sub_value + s2.additional_committed_sub_value) as committed_sub_value,
        SUM(CASE WHEN s2.status = 'ACTIVE' THEN s2.committed_sub_value + s2.additional_committed_sub_value END) as active_committed_sub_value,
        COUNT(DISTINCT s2.subscription_id) AS acquired_subscriptions,
        COUNT(DISTINCT CASE WHEN s2.status = 'ACTIVE' THEN s2.subscription_id END) AS active_subscriptions,
        SUM(s2.rental_period) AS rental_period -- summing AND THEN, ON tableau, divide BY acquired_subscriptions TO take the avg
    FROM tmp_order_category oc
    LEFT JOIN master.subscription s2
		ON oc.order_id = s2.order_id
	LEFT JOIN tmp_mkt_campaigns_top_10_per_month mc
		ON DATE_TRUNC('month',s2.start_date) = mc.reporting_date
		AND oc.country = mc.country
		AND oc.marketing_campaign = mc.marketing_campaign
		AND mc.row_num <= 10
	WHERE oc.is_order_from_orders_info = 1
		AND DATE_TRUNC('year',oc.submitted_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;


DROP TABLE IF EXISTS tmp_traffic;
CREATE TEMP TABLE tmp_traffic AS 
	SELECT 
		s.session_start::date AS reporting_date,
		COALESCE(s.marketing_channel, 'n/a') AS marketing_channel,
		COALESCE(s.marketing_campaign, 'n/a') AS marketing_campaign,
		lower(COALESCE(ms.marketing_source, 'Others')) AS marketing_source,
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
	        ELSE 'Germany' END AS country,
		CASE WHEN s.first_page_url ILIKE '/business%' THEN 'business_customer' ELSE 'normal_customer' END AS customer_type,
		'n/a'::varchar AS category_name,
		0::int AS is_affiliate_voucher,
		'no voucher'::varchar AS voucher_code,
		0::int AS plan_duration,
		'n/a'::varchar AS product_sku,
		'n/a'::varchar AS product_name,
		CASE WHEN cu.is_new_visitor IS TRUE THEN 'NEW' ELSE 'RECURRING' END AS new_recurring,
		CASE WHEN mc.marketing_campaign IS NOT NULL THEN 1 ELSE 0 END AS is_top_10_mkt_campaign,
		0::int AS is_paid_order,
		0::int AS is_submitted_order,
		'n/a'::varchar AS brand,
		count(DISTINCT s.session_id) AS traffic_sessions,
		count(DISTINCT s.anonymous_id) AS traffic_users
	FROM traffic.sessions s
	LEFT JOIN traffic.snowplow_user_mapping cu
		ON s.anonymous_id = cu.anonymous_id
		AND s.session_id = cu.session_id
	LEFT JOIN tmp_mkt_sources ms 
		ON s.marketing_source = ms.marketing_source
	LEFT JOIN tmp_mkt_campaigns_top_10_per_month mc
		ON DATE_TRUNC('month',s.session_start) = mc.reporting_date
        AND s.marketing_campaign = mc.marketing_campaign
        AND mc.row_num <= 10
        AND mc.country = CASE
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
	WHERE DATE_TRUNC('year',s.session_start::date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
		AND COALESCE(s.marketing_channel, 'n/a') = 'Affiliates'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;


DROP TABLE IF EXISTS tmp_orders_metrics;
CREATE TEMP TABLE tmp_orders_metrics AS 
WITH dimensions_combined AS (
	SELECT DISTINCT 
		category_name,
        reporting_date,
        country,
        marketing_campaign,
        marketing_source,
        marketing_channel,
        is_affiliate_voucher,
        voucher_code,
        plan_duration,
        product_sku,
        product_name,
        new_recurring,
        customer_type,
        brand,
        is_top_10_mkt_campaign,
        is_paid_order,
        is_submitted_order
	FROM tmp_traffic 
	UNION 
	SELECT DISTINCT 
		category_name,
        reporting_date,
        country,
        marketing_campaign,
        marketing_source,
        marketing_channel,
        is_affiliate_voucher,
        voucher_code,
        plan_duration,
        product_sku,
        product_name,
        new_recurring,
        customer_type,
        brand,
        is_top_10_mkt_campaign,
        is_paid_order,
        is_submitted_order
	FROM tmp_orders_metrics_subscriptions 
	UNION 
	SELECT DISTINCT 
		category_name,
        reporting_date,
        country,
        marketing_campaign,
        marketing_source,
        marketing_channel,
        is_affiliate_voucher,
        voucher_code,
        plan_duration,
        product_sku,
        product_name,
        new_recurring,
        customer_type,
        brand,
        is_top_10_mkt_campaign,
        is_paid_order,
        is_submitted_order
	FROM tmp_orders_metrics_submitted_orders 
	UNION 
	SELECT DISTINCT 
		category_name,
        reporting_date,
        country,
        marketing_campaign,
        marketing_source,
        marketing_channel,
        is_affiliate_voucher,
        voucher_code,
        plan_duration,
        product_sku,
        product_name,
        new_recurring,
        customer_type,
        brand,
        is_top_10_mkt_campaign,
        is_paid_order,
        is_submitted_order
	FROM tmp_orders_metrics_created_orders	
)
	SELECT 
		dc.category_name,
        dc.reporting_date,
        dc.country,
        dc.marketing_campaign,
        dc.marketing_source,
        dc.marketing_channel,
        dc.is_affiliate_voucher,
        dc.voucher_code,
        dc.plan_duration,
        dc.product_sku,
        dc.product_name,
        dc.new_recurring,
        dc.customer_type,
        dc.brand,
        dc.is_top_10_mkt_campaign,
        dc.is_paid_order,
        dc.is_submitted_order,
        COALESCE(t.traffic_sessions, 0) AS traffic_sessions,
		COALESCE(t.traffic_users, 0) AS traffic_users,
		COALESCE(oss.acquired_subscription_value, 0) AS acquired_subscription_value,
        COALESCE(oss.active_subscription_value, 0) AS active_subscription_value,
        COALESCE(oss.committed_sub_value, 0) AS committed_sub_value,
        COALESCE(oss.active_committed_sub_value, 0) AS active_committed_sub_value,
        COALESCE(oss.acquired_subscriptions, 0) AS acquired_subscriptions,
        COALESCE(oss.active_subscriptions, 0) AS active_subscriptions,
        COALESCE(oss.rental_period, 0) AS rental_period,
        COALESCE(oso.paid_orders, 0) AS paid_orders,
        COALESCE(oso.submitted_order, 0) AS submitted_order,
        COALESCE(oco.created_orders, 0) AS created_orders
	FROM dimensions_combined dc
	LEFT JOIN tmp_traffic t 
		ON dc.category_name = t.category_name
        AND dc.reporting_date = t.reporting_date
        AND dc.country = t.country
        AND dc.marketing_campaign = t.marketing_campaign
        AND dc.marketing_channel = t.marketing_channel
        AND dc.is_affiliate_voucher = t.is_affiliate_voucher
        AND dc.voucher_code = t.voucher_code
        AND dc.plan_duration = t.plan_duration
        AND dc.product_sku = t.product_sku
        AND dc.product_name = t.product_name
        AND dc.new_recurring = t.new_recurring
        AND dc.customer_type = t.customer_type
        AND dc.brand = t.brand
        AND dc.is_top_10_mkt_campaign = t.is_top_10_mkt_campaign
        AND dc.is_paid_order = t.is_paid_order
        AND dc.is_submitted_order = t.is_submitted_order
        AND dc.marketing_source = t.marketing_source
	LEFT JOIN tmp_orders_metrics_created_orders oco
		ON dc.category_name = oco.category_name
        AND dc.reporting_date = oco.reporting_date
        AND dc.country = oco.country
        AND dc.marketing_campaign = oco.marketing_campaign
        AND dc.marketing_channel = oco.marketing_channel
        AND dc.is_affiliate_voucher = oco.is_affiliate_voucher
        AND dc.voucher_code = oco.voucher_code
        AND dc.plan_duration = oco.plan_duration
        AND dc.product_sku = oco.product_sku
        AND dc.product_name = oco.product_name
        AND dc.new_recurring = oco.new_recurring
        AND dc.customer_type = oco.customer_type
        AND dc.brand = oco.brand
        AND dc.is_top_10_mkt_campaign = oco.is_top_10_mkt_campaign
        AND dc.is_paid_order = oco.is_paid_order
        AND dc.is_submitted_order = oco.is_submitted_order
        AND dc.marketing_source = oco.marketing_source
	LEFT JOIN tmp_orders_metrics_submitted_orders oso
		ON dc.category_name = oso.category_name
        AND dc.reporting_date = oso.reporting_date
        AND dc.country = oso.country
        AND dc.marketing_campaign = oso.marketing_campaign
        AND dc.marketing_channel = oso.marketing_channel
        AND dc.is_affiliate_voucher = oso.is_affiliate_voucher
        AND dc.voucher_code = oso.voucher_code
        AND dc.plan_duration = oso.plan_duration
        AND dc.product_sku = oso.product_sku
        AND dc.product_name = oso.product_name
        AND dc.new_recurring = oso.new_recurring
        AND dc.customer_type = oso.customer_type
        AND dc.brand = oso.brand
        AND dc.is_top_10_mkt_campaign = oso.is_top_10_mkt_campaign
        AND dc.is_paid_order = oso.is_paid_order
        AND dc.is_submitted_order = oso.is_submitted_order
        AND dc.marketing_source = oso.marketing_source
	LEFT JOIN tmp_orders_metrics_subscriptions oss
		ON dc.category_name = oss.category_name
        AND dc.reporting_date = oss.reporting_date
        AND dc.country = oss.country
        AND dc.marketing_campaign = oss.marketing_campaign
        AND dc.marketing_channel = oss.marketing_channel
        AND dc.is_affiliate_voucher = oss.is_affiliate_voucher
        AND dc.voucher_code = oss.voucher_code
        AND dc.plan_duration = oss.plan_duration
        AND dc.product_sku = oss.product_sku
        AND dc.product_name = oss.product_name
        AND dc.new_recurring = oss.new_recurring
        AND dc.customer_type = oss.customer_type
        AND dc.brand = oss.brand
        AND dc.is_top_10_mkt_campaign = oss.is_top_10_mkt_campaign
        AND dc.is_paid_order = oss.is_paid_order
        AND dc.is_submitted_order = oss.is_submitted_order
        AND dc.marketing_source = oss.marketing_source
;



DROP TABLE IF EXISTS tmp_costs_with_order_id_agg;
CREATE TEMP TABLE tmp_costs_with_order_id_agg AS 
WITH costs_with_order_id AS (
    SELECT
        'Affiliates' as marketing_channel, 
        LOWER(af.affiliate) AS marketing_campaign,
        lower(COALESCE(CASE WHEN af.affiliate_network = 'Rakuten' THEN 'rakutenmarketing' ELSE af.affiliate_network END, 'Others')) AS marketing_source,
        ccc.category_name, 
        ccc.is_affiliate_voucher, 
        ccc.product_sku, 
        ccc.product_name,
        ccc.brand,
        af.date::date AS reporting_date, 
        ccc.country, 
        ccc.is_paid_order,
        ccc.voucher_code , 
        ccc.new_recurring , 
        ccc.customer_type ,
        ccc.is_submitted_order,
        ccc.plan_duration,
        SUM(af.total_spent_local_currency * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
            AS total_spent_eur
    FROM marketing.marketing_cost_daily_affiliate_order af
	LEFT JOIN  trans_dev.daily_exchange_rate exc
		ON af.date::date = exc.date_
		AND af.currency = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
		ON af.currency = exc_last.currency
	LEFT JOIN tmp_order_category ccc
		ON af.order_id = ccc.order_id
    WHERE af.date::date BETWEEN '2021-10-18' AND '2023-01-14'
		AND af.order_id IS NOT NULL
		AND DATE_TRUNC('year',af.date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--​
    UNION
--​
    SELECT
        'Affiliates' AS marketing_channel, 
        LOWER(af.affiliate) AS marketing_campaign, 
        lower(COALESCE(CASE WHEN af.affiliate_network = 'Rakuten' THEN 'rakutenmarketing' ELSE af.affiliate_network END, 'Others')) AS marketing_source,
        ccc.category_name, 
        ccc.is_affiliate_voucher, 
        ccc.product_sku, 
        ccc.product_name,
        ccc.brand,
        af.submitted_date::date AS reporting_date, 
        ccc.country, 
        ccc.is_paid_order,
        ccc.voucher_code , 
        ccc.new_recurring , 
        ccc.customer_type ,
        ccc.is_submitted_order,
        ccc.plan_duration,
    FROM marketing.affiliate_validated_orders af
	LEFT JOIN tmp_order_category ccc
		ON af.order_id = ccc.order_id
    WHERE af.submitted_date::date >= '2023-01-15'
      AND af.commission_approval = 'APPROVED'
      AND af.order_id IS NOT NULL
      AND DATE_TRUNC('year',af.submitted_date)::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--​
    UNION
--​
    SELECT
        'Affiliates' as marketing_channel, 
        'yakk' AS marketing_campaign, 
        'others' AS marketing_source,
        ccc.category_name, 
        ccc.is_affiliate_voucher, 
        ccc.product_sku, 
        ccc.product_name,
        ccc.brand,
        af."date"::date AS reporting_date, 
        ccc.country, 
        ccc.is_paid_order,
        ccc.voucher_code , 
        ccc.new_recurring , 
        ccc.customer_type ,
        ccc.is_submitted_order,
        ccc.plan_duration, 
        SUM(paid_commission) AS total_spent_eur
    FROM marketing.marketing_yakk_b2b_cost af
	LEFT JOIN tmp_order_category ccc
		ON af.order_id = ccc.order_id
    WHERE af.order_id IS NOT NULL
        AND DATE_TRUNC('year',af."date")::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
)
    SELECT
        ccc.marketing_channel, 
        ccc.marketing_campaign, 
        ccc.marketing_source,
        ccc.category_name, 
        ccc.is_affiliate_voucher, 
        ccc.product_sku, 
        ccc.product_name,
        ccc.brand,
        ccc.reporting_date::date, 
        ccc.country, 
        ccc.is_paid_order,
        ccc.voucher_code , 
        ccc.new_recurring , 
        ccc.customer_type ,
        ccc.is_submitted_order,
        ccc.plan_duration, 
        CASE WHEN mc.marketing_campaign IS NOT NULL THEN 1 ELSE 0 END AS is_top_10_mkt_campaign,
        sum(total_spent_eur) AS total_spent_eur
    FROM costs_with_order_id ccc
	LEFT JOIN tmp_mkt_campaigns_top_10_per_month mc
		ON DATE_TRUNC('month',ccc.reporting_date) = mc.reporting_date
		AND ccc.country = mc.country
		AND ccc.marketing_campaign = mc.marketing_campaign
		AND mc.row_num <= 10
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;



DROP TABLE IF EXISTS tmp_costs_affiliates_yakk_without_order_id;
CREATE TEMP TABLE tmp_costs_affiliates_yakk_without_order_id AS 
    SELECT
        af.date::date AS reporting_date,
        'Affiliates' as marketing_channel,
        'yakk' AS marketing_campaign,
        'others' AS marketing_source,
        'Spain' AS country,
        'business_customer' AS customer_type,
        SUM(paid_commission) AS total_spent_eur
    FROM marketing.marketing_yakk_b2b_cost af
    WHERE af.order_id IS NULL
    	AND DATE_TRUNC('year',af."date")::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
    GROUP BY 1,2,3,4,5,6
;


DROP TABLE IF EXISTS tmp_costs_affiliates_without_order_id;
CREATE TEMP TABLE tmp_costs_affiliates_without_order_id AS 
    SELECT
        af.date::date AS reporting_date,
        'Affiliates' as marketing_channel,
        LOWER(af.affiliate) AS marketing_campaign,
        lower(COALESCE(CASE WHEN af.affiliate_network = 'Rakuten' THEN 'rakutenmarketing' ELSE af.affiliate_network END, 'Others')) AS marketing_source,
        af.country,
        SUM(af.total_spent_local_currency * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1))
            AS total_spent_eur
    FROM marketing.marketing_cost_daily_affiliate_fixed af
	LEFT JOIN  trans_dev.daily_exchange_rate exc
		ON af.date::date = exc.date_
		AND af.currency = exc.currency
	LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
		ON af.currency = exc_last.currency
    WHERE af.date::date >= '2021-10-18'
    	AND DATE_TRUNC('year',af."date")::DATE >= DATEADD('year',-2,DATE_TRUNC('year',current_date))
    GROUP BY 1,2,3,4,5
;


DROP TABLE IF EXISTS tmp_affiliates_metrics_paid_orders;
CREATE TEMP TABLE tmp_affiliates_metrics_paid_orders AS 
WITH joining_costs_with_metrics AS (
	SELECT
	    COALESCE(oc.reporting_date, caw.reporting_date, cy.reporting_date, ccc.reporting_date) AS _date,
	    COALESCE(oc.marketing_channel, caw.marketing_channel, cy.marketing_channel, ccc.marketing_channel) AS _channel,
	    COALESCE(oc.marketing_campaign, caw.marketing_campaign, cy.marketing_campaign, ccc.marketing_campaign) AS _campaign,
	    COALESCE(oc.marketing_source, caw.marketing_source, cy.marketing_source, ccc.marketing_source) AS _source,
	    COALESCE(oc.country, caw.country, cy.country, ccc.country) AS _country,
	    COALESCE(oc.customer_type, cy.customer_type, ccc.customer_type) AS _cust_type,
	    COALESCE(oc.category_name, ccc.category_name) AS category_name,
	    COALESCE(oc.is_affiliate_voucher, ccc.is_affiliate_voucher) AS is_affiliate_voucher,
	    COALESCE(oc.voucher_code, ccc.voucher_code) AS voucher_code,
	    COALESCE(oc.plan_duration, ccc.plan_duration) AS plan_duration,
	    COALESCE(oc.product_sku, ccc.product_sku) AS product_sku,
	    COALESCE(oc.product_name, ccc.product_name) AS product_name,
	    COALESCE(oc.new_recurring, ccc.new_recurring) AS new_recurring,
	    COALESCE(oc.is_top_10_mkt_campaign, ccc.is_top_10_mkt_campaign) AS is_top_10_mkt_campaign,
	    COALESCE(oc.is_paid_order, ccc.is_paid_order) AS is_paid_order,
	    COALESCE(oc.is_submitted_order, ccc.is_submitted_order) AS is_submitted_order,
	    COALESCE(oc.brand,ccc.brand) AS brand,
	    oc.created_orders,
	    oc.paid_orders,
	    oc.submitted_order,
	    oc.acquired_subscription_value,
	    oc.active_subscription_value,
	    oc.committed_sub_value,
	    oc.active_committed_sub_value,
	    oc.acquired_subscriptions,
	    oc.active_subscriptions,
	    oc.rental_period,
	    oc.traffic_sessions,
		oc.traffic_users,
	    caw.total_spent_eur::float / COUNT(*) OVER (PARTITION BY _date, _channel, _campaign, _country) AS cost_without_order_id,
	    cy.total_spent_eur::float / COUNT(*) OVER (PARTITION BY _date, _channel, _campaign, _country, _cust_type) AS cost_without_order_id_yakk,
	    ccc.total_spent_eur
	FROM tmp_orders_metrics oc
	FULL JOIN tmp_costs_with_order_id_agg ccc
		ON ccc.marketing_channel = oc.marketing_channel
		AND ccc.marketing_campaign = oc.marketing_campaign
		AND ccc.category_name = oc.category_name
		AND ccc.is_affiliate_voucher = oc.is_affiliate_voucher
    	AND ccc.product_sku = oc.product_sku
     	AND ccc.product_name = oc.product_name
    	AND ccc.brand = oc.brand
     	AND ccc.reporting_date::date = oc.reporting_date::date
    	AND ccc.country = oc.country
    	AND ccc.is_paid_order = oc.is_paid_order
    	AND ccc.voucher_code = oc.voucher_code
      	AND ccc.new_recurring = oc.new_recurring
    	AND ccc.customer_type = oc.customer_type
    	AND ccc.is_submitted_order = oc.is_submitted_order
    	AND ccc.plan_duration = oc.plan_duration
    	AND ccc.is_top_10_mkt_campaign = oc.is_top_10_mkt_campaign
    	AND ccc.marketing_source = oc.marketing_source
     FULL JOIN tmp_costs_affiliates_yakk_without_order_id cy
		ON COALESCE(oc.reporting_date, ccc.reporting_date) = cy.reporting_date
		AND COALESCE(oc.marketing_channel, ccc.marketing_channel) = cy.marketing_channel
		AND COALESCE(oc.marketing_campaign, ccc.marketing_campaign) = cy.marketing_campaign
		AND COALESCE(oc.country, ccc.country) = cy.country
		AND COALESCE(oc.customer_type,ccc.customer_type) = cy.customer_type
		AND COALESCE(oc.marketing_source, ccc.marketing_source) = cy.marketing_source
     FULL JOIN tmp_costs_affiliates_without_order_id caw
		ON COALESCE(oc.marketing_channel,ccc.marketing_channel,cy.marketing_channel) = caw.marketing_channel
		AND COALESCE(oc.reporting_date, ccc.reporting_date, cy.reporting_date) = caw.reporting_date
		AND COALESCE(oc.marketing_campaign, ccc.marketing_campaign, cy.marketing_campaign) = caw.marketing_campaign
		AND COALESCE(oc.country, ccc.country, cy.country) = caw.country	
		AND COALESCE(oc.marketing_source, ccc.marketing_source, cy.marketing_source) = caw.marketing_source
)
SELECT 
	_date AS reporting_date,
    _channel AS marketing_channel,
    _campaign AS marketing_campaign,
    _source AS marketing_source,
    _country AS country,
    _cust_type AS customer_type,
    category_name AS category_name,
    is_affiliate_voucher,
    voucher_code,
    plan_duration,
    product_sku,
    product_name,
    brand,
    new_recurring,
    is_top_10_mkt_campaign,
    is_paid_order,
    is_submitted_order,
    created_orders,
    paid_orders,
    submitted_order,
    acquired_subscription_value,
    active_subscription_value,
    committed_sub_value,
    active_committed_sub_value,
    acquired_subscriptions,
    active_subscriptions,
    rental_period,
    traffic_sessions,
	traffic_users,
    COALESCE(cost_without_order_id,0)::float + COALESCE(total_spent_eur,0)::float + COALESCE(cost_without_order_id_yakk,0) AS costs
FROM joining_costs_with_metrics
;


DROP TABLE IF EXISTS dm_marketing.affiliates_metrics_paid_orders;
CREATE TABLE dm_marketing.affiliates_metrics_paid_orders AS
SELECT * 
FROM tmp_affiliates_metrics_paid_orders;

GRANT SELECT ON dm_marketing.affiliates_metrics_paid_orders TO tableau;
GRANT SELECT ON dm_marketing.affiliates_metrics_paid_orders TO GROUP BI;
