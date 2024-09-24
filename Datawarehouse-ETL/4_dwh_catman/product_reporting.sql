DROP TABLE IF EXISTS dwh.product_reporting;
CREATE TABLE dwh.product_reporting AS 
WITH a AS (--From order item
	SELECT
		product_sku,
		store_id,
		min(created_at) AS min_order_item_date
	FROM
		ods_production.order_item i
	INNER JOIN ods_production."order" o ON
		i.order_id = o.order_id
	WHERE
		product_sku IS NOT NULL
	GROUP BY
		1,2
)
,
s AS (
	SELECT
		product_sku,
		store_id,
		min(start_date) AS min_start_date
	FROM
		ods_production.subscription s
	GROUP BY
		1,2
)
,
traffic AS (--- From Traffic
	SELECT
		store_id::CHARACTER VARYING,
		CASE
			WHEN page_type = 'pdp' 
				THEN product_sku::CHARACTER VARYING
			ELSE 'Non-pdp-pages'
		END AS product_sku,
		(page_view_start::date) AS pageview_Date
	FROM
		traffic.page_views pv
	LEFT JOIN ods_production.product p
		ON p.product_sku = pv.page_type_detail
	WHERE
		page_view_start::date >= '2019-07-01'
	UNION ALL
	SELECT
		v.store_id::CHARACTER VARYING AS store_id,
		v.product_sku::CHARACTER VARYING AS product_sku,
		v.creation_time::Date AS pageview_Date
	FROM
		stg_events.product_views v
	LEFT JOIN ods_production.product p ON
		v.product_id = p.product_id
	WHERE
		v.creation_time::Date<'2019-07-01'
	ORDER BY 3 DESC
)
,
t AS (
	SELECT
		store_id,
		product_sku,
		min(pageview_date) AS pageview_date
	FROM
		Traffic
	WHERE
		producT_sku IS NOT NULL
	GROUP BY
		1,2
)
,
ast AS (---From Asset investment
	SELECT
		product_sku,
		CASE
			WHEN warehouse IN ('office_us','ups_softeon_us_kylse')
		        	THEN 621::character VARYING
			WHEN warehouse IN ('synerlogis_de', 'office_de','ups_softeon_eu_nlrng','ingram_micro_eu_flensburg')
	        		THEN 1::character VARYING
		END AS store_id,
		min(purchased_date) AS min_purchase_date
	FROM
		ods_production.asset
	WHERE
		product_sku IS NOT NULL
	GROUP BY
		1,2
)
,
pre_final AS (
	SELECT
		COALESCE(a.product_sku, t.product_sku) AS product_sku,
		COALESCE(a.store_id, t.store_id) AS store_id,
		min(min_order_item_date) AS min_order_date,
		min(pageview_date::date) AS min_page_view_date
	FROM
		a
	FULL OUTER JOIN t ON
		t.product_sku = a.product_sku
		AND a.store_id = t.store_id
	GROUP BY
		1,2
)
,
final_ AS (
	SELECT
		COALESCE(pf.product_sku, ast.product_sku) AS product_sku,
		COALESCE(pf.store_id, ast.store_id) AS store_id,
		LEAST(min_order_date::date, min_page_view_date, min_purchase_date) AS min_date
	FROM
		pre_final pf
	FULL OUTER JOIN ast ON
		ast.product_sku = pf.product_sku
		AND ast.store_id = pf.store_id
)
,
lasT_check AS (
	SELECT
		*,
		CURRENT_DATE- min_date AS diff_check
	FROM
		final_
	ORDER BY
		diff_check ASC
) 
,
product_vector AS (
	SELECT
		datum AS fact_day,
		product_sku,
		store_id
	FROM
		public.dim_dates d
	JOIN last_check lc 
		 	 ON
		d.datum >= lc.min_date
		AND d.datum < current_date
)
,
FACT_DAYS AS (
	SELECT
		DISTINCT DATUM AS FACT_DAY
	FROM
		public.dim_dates
	WHERE
		DATUM <= CURRENT_DATE
)	
,
asv AS (
	SELECT
		f.fact_day,
		COALESCE(PRODUCT_SKU, 'n/a') AS product_sku,
		COALESCE(s.store_id::text, 'n/a') AS store_id,
		COALESCE(sum(s.subscription_value_eur), 0) AS active_subscription_value,
		COALESCE(count(DISTINCT s.subscription_id), 0) AS active_subscriptionS
	FROM
		fact_days f
	LEFT JOIN ods_production.subscription_phase_mapping AS s
	   ON
		f.fact_day::date >= s.fact_day::date
		AND
	  F.fact_day::date <= COALESCE(s.end_date::date, f.fact_day::date + 1)
	GROUP BY
		1,2,3
)
,
traffic_new AS (
	SELECT
		pv.store_id::CHARACTER VARYING,
		CASE
			WHEN pv.page_type = 'pdp' 
				THEN p.product_id::CHARACTER VARYING
			ELSE 'Non-pdp-pages'
		END AS product_id,
		pv.page_view_start::date AS pageview_Date,
		count(DISTINCT pv.page_view_id) AS pageviews,
		count(DISTINCT pv.page_view_id) AS pageviews_w_url,
		count(DISTINCT pv.session_id) AS pageview_unique_sessions,
		-- count(distinct coalesce(customer_id_mapped,anonymous_id)) as pageview_unique_customers,
		count(DISTINCT 
			CASE 
				WHEN s.is_paid 
					THEN pv.session_id 
			END) paid_traffic_sessions
		-- count(distinct case when paid_nonpaid='Paid' then coalesce(customer_id_mapped,anonymous_id) end) paid_traffic_customers
	FROM
		traffic.page_views pv
	LEFT JOIN traffic.sessions s
	 ON
		pv.session_id = s.session_id
	LEFT JOIN ods_production.product p
	 ON
		p.product_sku = pv.page_type_detail
	WHERE
		pv.page_view_start::date >= '2019-07-01'
		AND pv.page_type = 'pdp'
		AND pv.store_id IS NOT NULL
	GROUP BY
		1,2,3
	UNION ALL
	SELECT
		v.store_id::CHARACTER VARYING AS store_id,
		v.product_id::CHARACTER VARYING AS product_id,
		v.creation_time::Date AS pageview_Date,
		count(*) AS pageviews,
		count(CASE 
				WHEN page_id IS NOT NULL 
					THEN session_id 
			  END) AS pageviews_w_url,
		-- count(distinct session_id)
		count(CASE 
				WHEN page_id IS NOT NULL 
					THEN session_id 
			  END) AS pageview_unique_sessions,
		--count(distinct user_id) as pageview_unique_customers,
	 NULL AS paid_traffic_sessions
		-- null as paid_traffic_customers
	FROM
		stg_events.product_views v
	WHERE
		v.creation_time::Date<'2019-07-01'
	GROUP BY
		1,2,3
	ORDER BY
		3 DESC
)
,
traffic_sku AS (
	SELECT
		product_sku,
		t.*
	FROM
		traffic_new t
	LEFT JOIN (
		SELECT
			product_id,
			product_sku
		FROM
			ods_production.product) p 
	 		ON
		t.product_id = p.product_id
)
,
orders AS (
	SELECT
		o.store_id AS store_id,
		i.product_sku AS product_sku,
		o.created_date::date AS fact_day,
		count(DISTINCT i.order_id) AS carts,
		-- count(distinct case when plan_duration <= 1 then o.order_id end) as short_term_carts,
		-- count(distinct case when plan_duration >= 12 then o.order_id end) as long_term_carts,
		count(DISTINCT CASE WHEN submitted_date IS NOT NULL THEN o.order_id END) AS completed_orders,
		count(DISTINCT CASE WHEN o.status = 'PAID' THEN o.order_id END) AS paid_orders,
		count(DISTINCT CASE WHEN o.status = 'PAID' AND plan_duration <= 1 THEN o.order_id END) AS paid_short_term_orders,
		count(DISTINCT CASE WHEN o.status = 'PAID' AND plan_duration >= 12 THEN o.order_id END) AS paid_long_term_orders,
		count(DISTINCT CASE WHEN o.is_in_salesforce AND rg.new_recurring = 'NEW' THEN o.order_id END) AS completed_new_orders,
		count(DISTINCT CASE WHEN o.status = 'PAID' AND rg.new_recurring = 'NEW' THEN o.order_id END) AS paid_new_orders,
		count(DISTINCT CASE WHEN returned>0 THEN o.order_id END) AS returned_orders
	FROM
		ods_production.order_item i
	INNER JOIN ods_production."order" o ON
		i.order_id = o.order_id
	LEFT JOIN ods_production.order_retention_group rg ON
		rg.order_id = o.order_id
	LEFT JOIN (
		SELECT
			order_id,
			sum(CASE WHEN in_transit_at::date-delivered_at::date <= 7 THEN 1 ELSE 0 END) AS returned
		FROM
			ods_production.allocation
		WHERE
			in_transit_at IS NOT NULL
		GROUP BY
			1 ) r ON
		r.order_id = o.order_id
	GROUP BY
		1,2,3
	ORDER BY
		3 DESC
)
,
subs AS (
	SELECT
		start_date::date AS report_date,
		COALESCE(store_id, 'N/A') AS store_id,
		COALESCE(product_sku, 'N/A') AS product_sku,
		sum(subscription_value) AS acquired_subscription_value,
		sum(subscription_value_euro) AS acquired_subscription_value_euro,
		sum(committed_sub_value) AS acquired_committed_subscription_value,
		avg(subscription_value) AS avg_price,
		avg(rental_period) AS avg_duration,
		count(DISTINCT subscription_Id) AS acquired_subscriptions
	FROM
		ods_production.subscription s
	WHERE
		s.start_date IS NOT NULL
	GROUP BY
		1,2,3
) 
,
subs_c AS (
	SELECT
		cancellation_date::date AS report_date,
		COALESCE(store_id, 'N/A') AS store_id,
		COALESCE(product_sku, 'N/A') AS product_sku,
		sum(subscription_value) AS cancelled_subscription_value,
		sum(subscription_value_euro) AS cancelled_subscription_value_euro,
		count(DISTINCT subscription_Id) AS cancelled_subscriptions
	FROM
		ods_production.subscription s
	WHERE
		s.cancellation_date IS NOT NULL
	GROUP BY
		1,2,3
)
,
assets_purchased AS (
	SELECT
		purchased_date::date AS purchased_date,
		CASE
			WHEN warehouse IN ('office_us','ups_softeon_us_kylse')
		        	THEN 621
			WHEN warehouse IN ('synerlogis_de', 'office_de','ups_softeon_eu_nlrng','ingram_micro_eu_flensburg')
	        		THEN 1
		END AS store_id,
		product_sku,
		count(DISTINCT asset_id) AS assets_purchased_in_period,
		sum(initial_price) AS asset_invest_in_period
	FROM
		ods_production.asset
	GROUP BY
		1,2,3
)
,
utilization AS (
	SELECT
		aa.reporting_date::date AS fact_day,
	    COALESCE(aa.product_sku, 'N/A') AS product_sku,
	    CASE
			WHEN warehouse IN ('office_us','ups_softeon_us_kylse')
		        	THEN 621
			WHEN warehouse IN ('synerlogis_de','office_de','ups_softeon_eu_nlrng','ingram_micro_eu_flensburg')
	        		THEN 1
		END AS store_id,
	    sum(purchase_price_performing_on_rent) AS inventory_on_rent,
	    sum(purchase_price_at_risk_on_rent) AS inventory_debt_collection,
	    sum(purchase_price_in_stock) AS inventory_in_stock,
	    sum(purchase_price_refurbishment) AS inventory_refurbishment,
	    sum(purchase_price_transition) AS inventory_transition,
	    sum(purchase_price_sold_to_customers) AS inventory_sold_to_customers,
	    sum(purchase_price_sold_to_3rdparty) AS inventory_sold_to_3rdparty,
	    sum(purchase_price_sold_others) AS inventory_sold_others,
	    inventory_sold_to_customers + inventory_sold_to_3rdparty + inventory_sold_others AS inventory_sold,
	    sum(purchase_price_selling) AS inventory_selling,
	    sum(purchase_price_others) AS inventory_others,
	    sum(purchase_price_lost) AS inventory_lost,
	    sum(purchase_price_writtenoff_ops) AS inventory_writtenoff_ops,
	    sum(purchase_price_writtenoff_dc) AS inventory_writtenoff_dc,
	    inventory_writtenoff_ops + inventory_writtenoff_dc AS inventory_writtenoff, 
	    sum(purchase_price_irreparable) AS inventory_irreparable,
	    sum(purchase_price_in_repair) AS inventory_repair,
	    sum(purchase_price_inbound) AS inventory_inbound,
	    sum(purchase_price_inbound_unallocable) AS inventory_inbound_unallocable,
	    sum(number_of_assets_in_stock) AS stock_at_hand,
	    sum(purchase_price) AS asset_investment
	FROM
		dwh.portfolio_overview aa
	WHERE
		product_sku IS NOT NULL
	GROUP BY
		1,2,3
)
,
asset_details AS (
	SELECT
		fact_day::date AS fact_day,
		CASE
			WHEN warehouse IN ('office_us','ups_softeon_us_kylse')
		        	THEN 621
			WHEN warehouse IN ('synerlogis_de', 'office_de','ups_softeon_eu_nlrng','ingram_micro_eu_flensburg')
	        		THEN 1
		END AS store_id,
		COALESCE(s.product_sku, 'N/A') AS product_sku,
		count(CASE WHEN asset_status_original NOT IN ('SOLD', 'LOST', 'LOST SOLVED') THEN asset_id END) AS nb_assets_inportfolio_todate,
		SUM(residual_value_market_price) AS asset_market_value_todate
	FROM
		fact_days f
	LEFT JOIN master.asset AS s
	         ON
		f.fact_day::date >= s.purchased_date::date
	WHERE
		s.product_sku IS NOT NULL
	GROUP BY
		1,2,3
	ORDER BY
		1 DESC
)
,
prod_pre_final AS (
	SELECT 
	       pv.fact_day AS fact_day, 
		   o.fact_day AS order_created_date,
	       pv.product_sku AS product_sku, 
	       pv.store_id AS store_id, 
	       COALESCE(pageviews, 0) AS pageviews,
	       COALESCE(pageview_unique_sessions, 0) AS pageview_unique_sessions,
	       COALESCE(pageviews_w_url, 0) AS pageviews_w_url,
	       COALESCE(paid_traffic_sessions, 0) AS paid_traffic_sessions,
	       COALESCE(carts, 0) AS carts,
	       COALESCE(completed_orders, 0) AS completed_orders,
	       COALESCE(paid_orders, 0) AS paid_orders,
	       COALESCE(paid_short_term_orders, 0) AS paid_short_term_orders,
	       COALESCE(paid_long_term_orders, 0) AS paid_long_term_orders,
	       COALESCE(completed_new_orders, 0) AS completed_new_orders,
	       COALESCE(paid_new_orders, 0) AS paid_new_orders,
	       COALESCE(returned_orders, 0) AS returned_within_7_Days,
	       COALESCE(active_subscription_value, 0) AS active_subscription_value,
	       COALESCE(active_subscriptions, 0) AS active_subscriptions,
	       COALESCE(acquired_subscription_value, 0) AS acquired_subscription_value,
	       COALESCE(acquired_subscription_value_euro, 0) AS acquired_subscription_value_euro,
	       COALESCE(acquired_committed_subscription_value, 0) AS acquired_committed_subscription_value,
	       COALESCE(avg_price, 0) AS avg_price,
	       COALESCE(avg_duration, 0) AS avg_duration,
	       COALESCE(acquired_subscriptions, 0) AS acquired_subscriptions,
	       COALESCE(cancelled_subscription_value, 0) AS cancelled_subscription_value,
	       COALESCE(cancelled_subscription_value_euro, 0) AS cancelled_subscription_value_euro,
	       COALESCE(cancelled_subscriptions, 0) AS cancelled_subscriptions,
	        COALESCE(inventory_on_rent, 0) AS inventory_on_rent,
			COALESCE(inventory_refurbishment, 0) AS inventory_refurbishment,
			COALESCE(inventory_debt_collection, 0) AS inventory_debt_collection,
			COALESCE(inventory_in_stock, 0) AS inventory_in_stock,
			COALESCE(inventory_transition, 0) AS inventory_transition,
			COALESCE(inventory_sold, 0) AS inventory_sold,
			COALESCE(inventory_sold_to_customers,0) AS inventory_sold_to_customers,
			COALESCE(inventory_sold_to_3rdparty,0) AS inventory_sold_to_3rdparty,
			COALESCE(inventory_sold_others,0) AS inventory_sold_others,
			COALESCE(inventory_selling, 0) AS inventory_selling,
			COALESCE(inventory_others, 0) AS inventory_others,
			COALESCE(inventory_lost, 0) AS inventory_lost,
			COALESCE(inventory_writtenoff_ops,0) AS inventory_writtenoff_ops,
	    	COALESCE(inventory_writtenoff_dc,0) AS inventory_writtenoff_dc,
			COALESCE(inventory_writtenoff, 0) AS inventory_writtenoff,
			COALESCE(inventory_repair, 0) AS inventory_repair,
			COALESCE(inventory_irreparable, 0) AS inventory_irreparable, 
			COALESCE(inventory_inbound, 0) AS inventory_inbound,
			COALESCE(inventory_inbound_unallocable, 0) AS inventory_inbound_unallocable,
			COALESCE(stock_at_hand, 0) AS stock_at_hand,
			COALESCE(asset_investment, 0) AS asset_investment,
			COALESCE(assets_purchased_in_period, 0) AS assets_purchased_in_period,
			COALESCE(asset_invest_in_period, 0) AS asset_invest_in_period,
			COALESCE(nb_assets_inportfolio_todate, 0) AS nb_assets_inportfolio_todate,
			COALESCE(asset_market_value_todate, 0) AS asset_market_value_todate
	FROM
		product_vector pv
	LEFT JOIN orders o 
		     				ON
		pv.product_sku = o.product_sku
		AND pv.fact_day = o.fact_day
		AND pv.store_id = o.store_id
	LEFT JOIN traffic_sku tn 
		     				ON
		tn.product_sku = pv.product_sku
		AND tn.pageview_date = pv.fact_day
		AND tn.store_id = pv.store_id
	LEFT JOIN asv asv 
		     				ON
		asv.product_sku = pv.product_sku
		AND asv.fact_day = pv.fact_day
		AND asv.store_id = pv.store_id
	LEFT JOIN subs subs 
		     				ON
		subs.product_sku = pv.product_sku
		AND subs.report_date = pv.fact_day
		AND subs.store_id = pv.store_id
	LEFT JOIN subs_c sc 
		     				ON
		sc.product_sku = pv.product_sku
		AND sc.report_date = pv.fact_day
		AND sc.store_id = pv.store_id
	LEFT JOIN assets_purchased ap 
		     				ON
		ap.product_sku = pv.product_sku
		AND ap.purchased_date = pv.fact_day
		AND ap.store_id = pv.store_id
	LEFT JOIN utilization u   			
		        			ON
		u.product_sku = pv.product_sku
		AND u.fact_day = pv.fact_day
		AND u.store_id = pv.store_id
	LEFT JOIN asset_details ad  	
		      			  ON
		ad.product_sku = pv.product_sku
		AND ad.fact_day = pv.fact_day
		AND ad.store_id = pv.store_id   			
)
SELECT
		product_name,
	    brand,
	    category_name,
	    subcategory_name,
	    account_name,
	    store_name,
	    store_label,
	    store_short,
		s.country_name AS store_country,
	    ppf.*
FROM
	prod_pre_final ppf
LEFT JOIN ods_production.product p 
	    							ON
	p.product_sku = ppf.product_sku
LEFT JOIN ods_production.store s 
	     							ON
	s.id = ppf.store_id
;

GRANT SELECT ON dwh.product_reporting TO group pricing;	
GRANT SELECT ON dwh.product_reporting TO group commercial;	
GRANT SELECT ON dwh.product_reporting TO redash_commercial;
GRANT SELECT ON dwh.product_reporting TO redash_pricing;
GRANT SELECT ON dwh.product_reporting TO tableau;
GRANT SELECT ON dwh.product_reporting TO GROUP mckinsey;
