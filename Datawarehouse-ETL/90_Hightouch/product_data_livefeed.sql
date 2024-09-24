DROP TABLE IF EXISTS hightouch_sources.product_data_livefeed;
CREATE TABLE hightouch_sources.product_data_livefeed AS
WITH raw_rental_plans AS (
	SELECT 
		*,
		ROW_NUMBER () OVER (PARTITION BY product_id,minimum_term_months ORDER BY updated_at DESC) AS rowno
	FROM ods_production.rental_plans
	WHERE store_id IN (1,4,5,618)
)
, rental_plans AS (
	SELECT 
		product_id,
		listagg(minimum_term_months,'  |  ' ) WITHIN GROUP (ORDER BY minimum_term_months ASC) AS rental_plans,
		max(CASE WHEN minimum_term_months = 1 THEN rental_plan_price_higher_price END) AS rental_plan_price_1_month,
		max(CASE WHEN minimum_term_months = 3 THEN rental_plan_price_higher_price END) AS rental_plan_price_3_months,
		max(CASE WHEN minimum_term_months = 6 THEN rental_plan_price_higher_price END) AS rental_plan_price_6_months,
		max(CASE WHEN minimum_term_months = 12 THEN rental_plan_price_higher_price END) AS rental_plan_price_12_months,
		max(CASE WHEN minimum_term_months = 18 THEN rental_plan_price_higher_price END) AS rental_plan_price_18_months,
		max(CASE WHEN minimum_term_months = 24 THEN rental_plan_price_higher_price END) AS rental_plan_price_24_months
	FROM raw_rental_plans
	WHERE rowno = 1
	GROUP BY 1
), fact_days AS (
	SELECT
		DISTINCT DATUM AS FACT_DAY
	FROM public.dim_dates
	WHERE DATUM <= CURRENT_DATE
), last_week_active_sub AS (
	SELECT
		fact_day,
		variant_sku,
		COALESCE(sum(s.subscription_value), 0) AS active_sub_value_last_week,
		COALESCE(count(DISTINCT s.subscription_id), 0) AS active_subs_last_week
	FROM fact_days f
	LEFT JOIN ods_production.subscription AS s
	   ON f.fact_day::date >= s.start_date::date
		AND F.fact_day::date < COALESCE(s.cancellation_date::date, f.fact_day::date + 1)
	WHERE fact_day = DATEADD('day',-1,DATE_TRUNC('week', CURRENT_DATE))
	GROUP BY 1,2
), subs AS (
	SELECT
		variant_sku,
		avg(subscription_value) AS avg_sub_value,
		count(CASE WHEN datediff('day', start_date, current_date) <= 7 THEN subscription_id END ) AS created_subs_last_week,
		count(CASE WHEN datediff('day', start_date, current_date) <= 14 THEN subscription_id END ) AS created_subs_last_2weeks,
		count(CASE WHEN datediff('day', start_date, current_date) <= 28 THEN subscription_id END ) AS created_subs_last_4weeks,
		sum(CASE WHEN datediff('day', start_date, current_date) <= 7 THEN subscription_value END ) AS created_asv_last_week,
		sum(CASE WHEN datediff('day', start_date, current_date) <= 14 THEN subscription_value END ) AS created_asv_last_2weeks,
		sum(CASE WHEN datediff('day', start_date, current_date) <= 28 THEN subscription_value END ) AS created_asv_last_4weeks,
		count(CASE WHEN datediff('day', cancellation_date, current_date) <= 7 THEN subscription_id END ) AS cancelled_subs_last_week,
		count(CASE WHEN datediff('day', cancellation_date, current_date) <= 14 THEN subscription_id END ) AS cancelled_subs_last_2weeks,
		count(CASE WHEN datediff('day', cancellation_date, current_date) <= 28 THEN subscription_id END ) AS cancelled_subs_last_4weeks,
		count(CASE WHEN datediff('day', start_date, current_date) <= 90 THEN subscription_id END ) AS created_subs_last_3months,
		count(CASE WHEN datediff('day', cancellation_date, current_date) <= 90 THEN subscription_id END) AS cancelled_subs_last_3months
	FROM ods_production.subscription
	GROUP BY 1
), orders AS(
	SELECT
		oi.variant_sku,
		count(CASE WHEN datediff('day', paid_date::timestamp, current_date) <= 90 AND o.status = 'PAID' THEN oi.order_id END) AS paid_orders_last_3months,
		count(CASE WHEN datediff('day', submitted_date::timestamp, current_date) <= 90 THEN oi.order_id END) AS submitted_orders_last_3months,
		count(CASE WHEN datediff('day', paid_date::timestamp, current_date) <= 7 AND o.status = 'PAID' THEN o.order_id END) AS paid_orders_last_week,
		count(CASE WHEN datediff('day', paid_date::timestamp, current_date) <= 14 AND o.status = 'PAID' THEN o.order_id END) AS paid_orders_last_2weeks,
		count(CASE WHEN datediff('day', paid_date::timestamp, current_date) <= 28 AND o.status = 'PAID' THEN o.order_id END) AS paid_orders_last_4weeks
	FROM ods_production.order_item oi
	LEFT JOIN ods_production."order" o 
	ON o.order_id = oi.order_id
	LEFT JOIN ods_production.order_marketing_channel omc 
	ON omc.order_id = oi.order_id
	GROUP BY 1
), product_orders AS (
	SELECT
		oi.product_sku,
		count(CASE WHEN datediff('day', paid_date::timestamp, current_date) <= 28 AND o.status = 'PAID' THEN oi.order_id END) AS paid_orders_last_4weeks_per_product,
		count(CASE WHEN datediff('day', submitted_date::timestamp, current_date) <= 7 AND omc.is_paid IS TRUE THEN oi.order_id END) AS submitted_orders_last_week_per_product_paid_traffic,
		count(CASE WHEN datediff('day', submitted_date::timestamp, current_date) <= 7 AND omc.is_paid IS FALSE THEN oi.order_id END) AS submitted_orders_last_week_per_product_organic_traffic,
		count(CASE WHEN datediff('day', submitted_date::timestamp, current_date) <= 14 AND omc.is_paid IS TRUE THEN oi.order_id END) AS submitted_orders_last_2weeks_per_product_paid_traffic,
		count(CASE WHEN datediff('day', submitted_date::timestamp, current_date) <= 14 AND omc.is_paid IS FALSE THEN oi.order_id END) AS submitted_orders_last_2weeks_per_product_organic_traffic,
		count(CASE WHEN datediff('day', submitted_date::timestamp, current_date) <= 28 AND omc.is_paid IS TRUE THEN oi.order_id END) AS submitted_orders_last_4weeks_per_product_paid_traffic,
		count(CASE WHEN datediff('day', submitted_date::timestamp, current_date) <= 28 AND omc.is_paid IS FALSE THEN oi.order_id END) AS submitted_orders_last_4weeks_per_product_organic_traffic
	FROM ods_production.order_item oi
	LEFT JOIN ods_production."order" o 
	 ON o.order_id = oi.order_id
	LEFT JOIN ods_production.order_marketing_channel omc 
	 ON omc.order_id = oi.order_id
	GROUP BY 1
), assets AS (
	SELECT
		o.variant_sku,
		count(CASE WHEN o.asset_status_original IN ('IN STOCK','INBOUND UNALLOCABLE') THEN o.asset_id END) AS stock_on_hand,
		count(CASE WHEN o.asset_status_original = 'IN STOCK' THEN o.asset_id END) AS stock_only_in_stock,
		count(CASE WHEN o.asset_status_original = 'INBOUND UNALLOCABLE' THEN o.asset_id END) AS stock_inbound_unallocable,
		count(CASE WHEN o.asset_status_grouped IN ('IN STOCK', 'ON RENT', 'REFURBISHMENT') OR o.asset_status_original IN ('INBOUND DAMAGED','INBOUND QUARANTINE','INBOUND UNALLOCABLE') THEN o.asset_id END) AS stock_on_book,
		count(CASE WHEN o.asset_status_original = 'IN STOCK' AND a.warehouse = 'ingram_micro_eu_flensburg' THEN o.asset_id END) AS stock_waiting_wh_transfer,
		count(CASE WHEN o.asset_status_original = 'TRANSFERRED TO WH' AND a.warehouse = 'ingram_micro_eu_flensburg' THEN o.asset_id END) AS stock_transfer_from_ingram,
		sum(CASE WHEN o.asset_status_original = 'IN STOCK' OR o.asset_status_original = 'INBOUND UNALLOCABLE' THEN a.initial_price ELSE 0 END) AS price_stock_on_hand,
		sum(CASE WHEN o.asset_status_original = 'INBOUND UNALLOCABLE' THEN a.initial_price ELSE 0 END) AS price_stock_inbound_unallocable,
		sum(CASE WHEN o.asset_status_original = 'IN STOCK' THEN a.initial_price ELSE 0 END) AS price_stock_only_in_stock,
		sum(CASE WHEN o.asset_status_grouped = 'ON RENT' THEN a.initial_price ELSE 0 END) AS price_on_rent,
		sum(a.initial_price) AS asset_investment_amount,
		sum(CASE WHEN a.asset_status_new = 'SOLD' THEN a.initial_price ELSE 0 END) AS price_sold,
		sum(CASE WHEN o.asset_status_original = 'TRANSFERRED TO WH' AND a.warehouse = 'ingram_micro_eu_flensburg' THEN 0
				 WHEN a.asset_status_new NOT IN ('SELLING','SOLD','TRANSITION','REFURBISHMENT','PERFORMING','AT RISK','NOT AVAILABLE','IN STOCK', 'IN REPAIR') AND o.asset_status_original NOT IN ('INBOUND UNALLOCABLE')
					THEN a.initial_price
					ELSE 0 END) AS price_others,
		sum(CASE WHEN o.asset_status_grouped IN ('IN STOCK','REFURBISHMENT') OR o.asset_status_original IN ('INBOUND DAMAGED','INBOUND QUARANTINE','INBOUND UNALLOCABLE') THEN a.initial_price END) AS purchase_price_stock,
		sum(CASE WHEN o.asset_status_original = 'IN STOCK' AND a.warehouse = 'ingram_micro_eu_flensburg' THEN a.initial_price END) AS purchase_price_stock_waiting_wh_transfer, 
		sum(CASE WHEN o.asset_status_original = 'TRANSFERRED TO WH' AND a.warehouse = 'ingram_micro_eu_flensburg' THEN a.initial_price END) AS purchase_price_transfer_from_ingram,
		(asset_investment_amount) - (price_sold) - (price_others) AS purchase_price_on_book,	
		CASE
			WHEN (price_on_rent + price_stock_on_hand) != 0 THEN (price_on_rent /(price_on_rent + price_stock_on_hand))::decimal(10,
			2)
			ELSE 0
		END AS marketable_utilisation,
		AVG(a.purchase_price_commercial) AS avg_purchase_price_commercial,
		AVG(CASE WHEN o.months_since_purchase <= 3 THEN a.purchase_price_commercial END) AS avg_purchase_price_commercial_last_3months,
		AVG(CASE WHEN o.asset_status_original = 'IN STOCK' THEN COALESCE(o.days_in_stock, 0) END ) AS avg_days_in_stock
	FROM ods_production.asset o
	LEFT JOIN master.asset a
		ON o.asset_id = a.asset_id 
	GROUP BY 1
), inbound AS (
	SELECT
		variant_sku,
		sum(net_quantity) AS incoming_qty
	FROM ods_production.purchase_request_item
	WHERE request_status NOT IN ('CANCELLED', 'NEW')
	GROUP BY 1
), last_values AS (
	SELECT
		o.variant_sku,
		LAST_VALUE(CASE WHEN o.asset_status_grouped != 'NEVER PURCHASED' THEN a.purchase_price_commercial END IGNORE NULLS) OVER(PARTITION BY o.variant_sku
	ORDER BY
		o.purchased_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_purchase_price_commercial,
		LAST_VALUE(CASE WHEN o.asset_status_grouped != 'NEVER PURCHASED' THEN o.purchased_date END IGNORE NULLS) OVER(PARTITION BY o.variant_sku
	ORDER BY
		o.purchased_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_purchased_date
	FROM ods_production.asset o
	LEFT JOIN master.asset a
		ON a.asset_id = o.asset_id
), last_values_grouped AS (
	SELECT *
	FROM last_values
	GROUP BY 1,2,3
), traffic AS (
	SELECT
		p.product_id AS product_id,
		count(DISTINCT CASE WHEN s.is_paid AND datediff('d', pv.page_view_start::date, CURRENT_DATE) <= 7 THEN COALESCE(pv.customer_id_mapped, pv.anonymous_id) END) AS paid_traffic_last_week,
		count(DISTINCT CASE WHEN s.is_paid IS FALSE AND datediff('d', pv.page_view_start::date, CURRENT_DATE) <= 7 THEN COALESCE(pv.customer_id_mapped, pv.anonymous_id) END) AS organic_traffic_last_week,
		count(DISTINCT CASE WHEN s.is_paid AND datediff('d', pv.page_view_start::date, CURRENT_DATE) <= 14 THEN COALESCE(pv.customer_id_mapped, pv.anonymous_id) END) AS paid_traffic_last_2weeks,
		count(DISTINCT CASE WHEN s.is_paid IS FALSE AND datediff('d', pv.page_view_start::date, CURRENT_DATE) <= 14 THEN COALESCE(pv.customer_id_mapped, pv.anonymous_id) END) AS organic_traffic_last_2weeks,
		count(DISTINCT CASE WHEN s.is_paid AND datediff('d', pv.page_view_start::date, CURRENT_DATE) <= 28 THEN COALESCE(pv.customer_id_mapped, pv.anonymous_id) END) AS paid_traffic_last_4weeks,
		count(DISTINCT CASE WHEN s.is_paid IS FALSE AND datediff('d', pv.page_view_start::date, CURRENT_DATE) <= 28 THEN COALESCE(pv.customer_id_mapped, pv.anonymous_id) END) AS organic_traffic_last_4weeks,
		count(DISTINCT CASE WHEN datediff('day', pv.page_view_start::date, current_date) <= 7 THEN pv.page_view_id END) AS pageviews_per_product_last_week,
		count(DISTINCT CASE WHEN datediff('day', pv.page_view_start::date, current_date) <= 14 THEN pv.page_view_id END) AS pageviews_per_product_last_2weeks,
		count(DISTINCT CASE WHEN datediff('day', pv.page_view_start::date, current_date) <= 28 THEN pv.page_view_id END) AS pageviews_per_product_last_4weeks
	FROM traffic.page_views pv
	LEFT JOIN traffic.sessions s
	  ON pv.session_id = s.session_id
	LEFT JOIN ods_production.product p
	 ON p.product_sku = pv.page_type_detail
	WHERE page_view_start::date >= dateadd('day',-28,current_date)
	 AND page_type = 'pdp'
	GROUP BY 1
),
conversion AS (
	SELECT
		p.product_id,
		CASE
			WHEN COALESCE(organic_traffic_last_4weeks, 0) != 0 THEN (submitted_orders_last_4weeks_per_product_organic_traffic::DOUBLE PRECISION / organic_traffic_last_4weeks::DOUBLE PRECISION)
			ELSE 0
		END AS organic_conversion_per_product_last4weeks,
		CASE
			WHEN COALESCE(paid_traffic_last_4weeks, 0) != 0 THEN (submitted_orders_last_4weeks_per_product_paid_traffic::DOUBLE PRECISION / paid_traffic_last_4weeks::DOUBLE PRECISION )
			ELSE 0
		END AS paid_conversion_per_product_last4weeks,
		CASE
			WHEN COALESCE(organic_traffic_last_2weeks, 0) != 0 THEN (submitted_orders_last_2weeks_per_product_organic_traffic::DOUBLE PRECISION / organic_traffic_last_2weeks::DOUBLE PRECISION)
			ELSE 0
		END AS organic_conversion_per_product_last2weeks,
		CASE
			WHEN COALESCE(paid_traffic_last_2weeks, 0) != 0 THEN (submitted_orders_last_2weeks_per_product_paid_traffic::DOUBLE PRECISION / paid_traffic_last_2weeks::DOUBLE PRECISION )
			ELSE 0
		END AS paid_conversion_per_product_last2weeks,
		CASE
			WHEN COALESCE(organic_traffic_last_week, 0) != 0 THEN (submitted_orders_last_week_per_product_organic_traffic::DOUBLE PRECISION / organic_traffic_last_week::DOUBLE PRECISION)
			ELSE 0
		END AS organic_conversion_per_product_lastweek,
		CASE
			WHEN COALESCE(paid_traffic_last_week, 0) != 0 THEN (submitted_orders_last_week_per_product_paid_traffic::DOUBLE PRECISION / paid_traffic_last_week::DOUBLE PRECISION )
			ELSE 0
		END AS paid_conversion_per_product_lastweek
	FROM ods_production.product p
	LEFT JOIN traffic t 
	 ON t.product_id = p.product_id
	LEFT JOIN product_orders po 
	 ON po.product_sku = p.product_sku
)
,collected_revenue AS (
	SELECT 
		s.variant_sku,
		sum(pa.amount_paid) AS collected_revenue
	FROM ods_production.payment_all pa 
	LEFT JOIN master.subscription s 
	 ON pa.subscription_id = s.subscription_id 
	GROUP BY 1
)
, current_market_value AS (
	SELECT 
		variant_sku,
		sum(residual_value_market_price) AS current_asset_market_value
	FROM master.asset
	GROUP BY 1
)
, product_families AS (
	SELECT 
		product_sku,
		product_type,
		model_name,
		model_family
	FROM pricing.v_product_family_check  
)
SELECT
	v.variant_sku,
	v.ean,
	v.upcs,
	p.product_id,
	p.product_sku,
	p.category_name,
	p.subcategory_name,
	p.brand,
	p.product_name,
	p."rank",
	v.variant_color,
	p.slug AS pdp_url,
	v.availability_state AS Availability,
	v.article_number,
	rp.rental_plans,
	rp.rental_plan_price_1_month,
	rp.rental_plan_price_3_months,
	rp.rental_plan_price_6_months,
	rp.rental_plan_price_12_months,
	rp.rental_plan_price_18_months,
	rp.rental_plan_price_24_months,
	p.market_price,
	a.stock_on_book,
	a.avg_purchase_price_commercial,
	a.avg_purchase_price_commercial_last_3months,
	a.price_stock_on_hand - COALESCE(a.purchase_price_stock_waiting_wh_transfer,0) AS price_stock_on_hand,
	a.price_stock_only_in_stock - COALESCE(a.purchase_price_stock_waiting_wh_transfer,0) AS price_stock_only_in_stock,
	a.price_stock_inbound_unallocable,
	a.purchase_price_stock_waiting_wh_transfer,
	a.price_on_rent,
	a.marketable_utilisation,
	a.stock_on_hand - COALESCE(a.stock_waiting_wh_transfer,0) AS stock_on_hand,
	a.stock_only_in_stock - COALESCE(a.stock_waiting_wh_transfer,0) AS stock_only_in_stock,
	a.stock_inbound_unallocable,
	a.stock_waiting_wh_transfer,
	COALESCE(avg_days_in_stock, 0) AS avg_days_in_stock,
	COALESCE(o.submitted_orders_last_3months, 0) AS submitted_orders_last_3months,
	o.paid_orders_last_3months,
	COALESCE(s.created_asv_last_week, 0) AS created_asv_last_week,
	COALESCE(s.created_asv_last_2weeks, 0) AS created_asv_last_2weeks,
	COALESCE(s.created_asv_last_4weeks, 0) AS created_asv_last_4weeks,
	s.created_subs_last_3months,
	s.cancelled_subs_last_3months,
	s.avg_sub_value,
	lg.last_purchase_price_commercial,
	datediff('month',
	lg.last_purchased_date,
	CURRENT_DATE) AS months_since_last_purchase,
	CASE
		WHEN COALESCE(i.incoming_qty, 0) > 0 
			THEN i.incoming_qty
		ELSE 0
	END AS incoming_qty,
	created_subs_last_week,
	created_subs_last_2weeks,
	created_subs_last_4weeks,
	cancelled_subs_last_week,
	cancelled_subs_last_2weeks,
	cancelled_subs_last_4weeks,
	COALESCE(paid_traffic_last_week, 0) AS paid_traffic_last_week,
	COALESCE(organic_traffic_last_week, 0) AS organic_traffic_last_week,
	COALESCE(paid_traffic_last_2weeks, 0) paid_traffic_last_2weeks,
	COALESCE(organic_traffic_last_2weeks, 0) AS organic_traffic_last_2weeks,
	COALESCE(paid_traffic_last_4weeks, 0) AS paid_traffic_last_4weeks,
	COALESCE(organic_traffic_last_4weeks, 0) AS organic_traffic_last_4weeks,
	CASE
		WHEN organic_conversion_per_product_last4weeks > 1 
			THEN 1
		ELSE organic_conversion_per_product_last4weeks
	END AS organic_conversion_per_product_last4weeks,
	CASE
		WHEN paid_conversion_per_product_last4weeks > 1 
			THEN 1
		ELSE paid_conversion_per_product_last4weeks
	END AS paid_conversion_per_product_last4weeks,
	CASE
		WHEN organic_conversion_per_product_last2weeks > 1 
			THEN 1
		ELSE organic_conversion_per_product_last2weeks
	END AS organic_conversion_per_product_last2weeks,
	CASE
		WHEN paid_conversion_per_product_last2weeks > 1 
			THEN 1
		ELSE paid_conversion_per_product_last2weeks
	END AS paid_conversion_per_product_last2weeks,
	CASE
		WHEN organic_conversion_per_product_lastweek > 1 
			THEN 1
		ELSE organic_conversion_per_product_lastweek
	END organic_conversion_per_product_lastweek,
	CASE
		WHEN paid_conversion_per_product_lastweek > 1 
			THEN 1
		ELSE paid_conversion_per_product_lastweek
	END AS paid_conversion_per_product_lastweek,
	COALESCE(active_sub_value_last_week, 0) AS active_sub_value_last_week ,
	COALESCE(active_subs_last_week, 0) AS active_subs_last_week,
	paid_orders_last_week,
	paid_orders_last_2weeks,
	paid_orders_last_4weeks,
	t.pageviews_per_product_last_week,
	t.pageviews_per_product_last_2weeks,
	t.pageviews_per_product_last_4weeks,
	asset_investment_amount,
	collected_revenue,
	current_asset_market_value,
	a.purchase_price_stock,
	a.purchase_price_on_book,
	a.stock_transfer_from_ingram,
	a.purchase_price_transfer_from_ingram,
	pf.model_family,
	pf.product_type,
	pf.model_name,
	NULLIF(pss.storage_capacity,'') AS storage_capacity
FROM ods_production.variant v
LEFT JOIN ods_production.product p 
 ON v.product_id = p.product_id
LEFT JOIN subs s 
 ON s.variant_sku = v.variant_sku
LEFT JOIN orders o 
 ON o.variant_sku = v.variant_sku
LEFT JOIN assets a 
 ON a.variant_sku = v.variant_sku
LEFT JOIN rental_plans rp 
 ON rp.product_id = p.product_id
LEFT JOIN inbound i 
 ON i.variant_sku = v.variant_sku
LEFT JOIN last_values_grouped lg 
 ON lg.variant_sku = v.variant_sku
LEFT JOIN traffic t 
 ON v.product_id = t.product_id
LEFT JOIN conversion c 
 ON c.product_id = v.product_id
LEFT JOIN last_week_active_sub lw 
 ON lw.variant_sku = v.variant_sku
LEFT JOIN collected_revenue cr
 ON v.variant_sku = cr.variant_sku
LEFT JOIN current_market_value mv
	ON v.variant_sku = mv.variant_sku
LEFT JOIN product_families pf
	ON pf.product_sku = p.product_sku 
LEFT JOIN dm_catman.v_product_structured_specifications pss
	ON pss.product_sku = p.product_sku
;

BEGIN TRANSACTION;

DELETE FROM hightouch_sources.product_data_livefeed_historical
WHERE product_data_livefeed_historical.date = current_date::date
	OR product_data_livefeed_historical.date <= dateadd('year', -2, current_date);

INSERT INTO hightouch_sources.product_data_livefeed_historical
SELECT 
	variant_sku,
	ean,
	upcs,
	product_id,
	product_sku,
	category_name,
	subcategory_name,
	brand,
	product_name,
	"rank",
	variant_color,
	pdp_url,
	Availability,
	article_number,
	rental_plans,
	rental_plan_price_1_month,
	rental_plan_price_3_months,
	rental_plan_price_6_months,
	rental_plan_price_12_months,
	rental_plan_price_18_months,
	rental_plan_price_24_months,
	market_price,
	stock_on_book,
	avg_purchase_price_commercial,
	avg_purchase_price_commercial_last_3months,
	price_stock_on_hand,
	price_stock_only_in_stock,
	price_stock_inbound_unallocable,
	purchase_price_stock_waiting_wh_transfer,
	price_on_rent,
	marketable_utilisation,
	stock_on_hand,
	stock_only_in_stock,
	stock_inbound_unallocable,
	stock_waiting_wh_transfer,
	avg_days_in_stock,
	submitted_orders_last_3months,
	paid_orders_last_3months,
	created_asv_last_week,
	created_asv_last_2weeks,
	created_asv_last_4weeks,
	created_subs_last_3months,
	cancelled_subs_last_3months,
	avg_sub_value,
	last_purchase_price_commercial,
	months_since_last_purchase,
	incoming_qty,
	created_subs_last_week,
	created_subs_last_2weeks,
	created_subs_last_4weeks,
	cancelled_subs_last_week,
	cancelled_subs_last_2weeks,
	cancelled_subs_last_4weeks,
	paid_traffic_last_week,
	organic_traffic_last_week,
	paid_traffic_last_2weeks,
	organic_traffic_last_2weeks,
	paid_traffic_last_4weeks,
	organic_traffic_last_4weeks,
	organic_conversion_per_product_last4weeks,
	paid_conversion_per_product_last4weeks,
	organic_conversion_per_product_last2weeks,
	paid_conversion_per_product_last2weeks,
	organic_conversion_per_product_lastweek,
	paid_conversion_per_product_lastweek,
	active_sub_value_last_week,
	active_subs_last_week,
	paid_orders_last_week,
	paid_orders_last_2weeks,
	paid_orders_last_4weeks,
	pageviews_per_product_last_week,
	pageviews_per_product_last_2weeks,
	pageviews_per_product_last_4weeks,
	asset_investment_amount,
	collected_revenue,
	current_asset_market_value,
	purchase_price_stock,
	purchase_price_on_book,
	stock_transfer_from_ingram,
	purchase_price_transfer_from_ingram,
	model_family,
	product_type,
	model_name,
	current_date AS date,
	storage_capacity
FROM hightouch_sources.product_data_livefeed;
 
END TRANSACTION;


GRANT SELECT ON hightouch_sources.product_data_livefeed TO hightouch;

GRANT SELECT ON hightouch_sources.product_data_livefeed TO GROUP pricing;

GRANT SELECT ON hightouch_sources.product_data_livefeed TO hightouch_pricing;

GRANT SELECT ON hightouch_sources.product_data_livefeed TO catman;

GRANT SELECT ON hightouch_sources.product_data_livefeed TO redash_pricing;

GRANT SELECT ON hightouch_sources.product_data_livefeed TO hightouch_recommerce;

GRANT SELECT ON hightouch_sources.product_data_livefeed TO recommerce_redash;

GRANT SELECT ON hightouch_sources.product_data_livefeed  TO group recommerce;

GRANT SELECT ON hightouch_sources.product_data_livefeed TO tableau;

GRANT SELECT ON hightouch_sources.product_data_livefeed TO GROUP mckinsey;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical TO hightouch;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical TO GROUP pricing;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical TO hightouch_pricing;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical TO catman;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical TO redash_pricing;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical TO hightouch_recommerce;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical TO recommerce_redash;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical  TO group recommerce;

GRANT SELECT ON hightouch_sources.product_data_livefeed_historical TO tableau;
