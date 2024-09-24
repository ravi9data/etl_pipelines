-- dm_weekly_monthly.v_category_performance source

CREATE OR REPLACE VIEW dm_weekly_monthly.v_category_performance AS 
WITH product_reporting_mix AS 
(
	SELECT CAST("product_reporting"."account_name" AS TEXT) AS "account_name",
	  "product_reporting"."acquired_committed_subscription_value" AS "acquired_committed_subscription_value",
	  "product_reporting"."acquired_subscription_value" AS "acquired_subscription_value",
	  "product_reporting"."acquired_subscriptions" AS "acquired_subscriptions",
	  "product_reporting"."active_subscription_value" AS "active_subscription_value",
	  "product_reporting"."active_subscriptions" AS "active_subscriptions",
	  "product_reporting"."asset_invest_in_period" AS "asset_invest_in_period",
	  "product_reporting"."asset_investment" AS "asset_investment",
	  "product_reporting"."asset_market_value_todate" AS "asset_market_value_todate",
	  "product_reporting"."assets_purchased_in_period" AS "assets_purchased_in_period",
	  "product_reporting"."avg_duration" AS "avg_duration",
	  "product_reporting"."avg_price" AS "avg_price",
	  CAST("product_reporting"."brand" AS TEXT) AS "brand",
	  "product_reporting"."cancelled_subscription_value" AS "cancelled_subscription_value",
	  "product_reporting"."cancelled_subscriptions" AS "cancelled_subscriptions",
	  "product_reporting"."carts" AS "carts",
	  CAST("product_reporting"."category_name" AS TEXT) AS "category_name",
	  "product_reporting"."completed_new_orders" AS "completed_new_orders",
	  "product_reporting"."completed_orders" AS "completed_orders",
	  "custom sql query"."fact_day" AS "fact_day (custom sql query)",
	  "product_reporting"."fact_day" AS "fact_day",
	  "product_reporting"."inventory_debt_collection" AS "inventory_debt_collection",
	  "product_reporting"."inventory_in_stock" AS "inventory_in_stock",
	  "product_reporting"."inventory_irreparable" AS "inventory_irreparable",
	  "product_reporting"."inventory_lost" AS "inventory_lost",
	  "product_reporting"."inventory_on_rent" AS "inventory_on_rent",
	  "product_reporting"."inventory_others" AS "inventory_others",
	  "product_reporting"."inventory_refurbishment" AS "inventory_refurbishment",
	  "product_reporting"."inventory_repair" AS "inventory_repair",
	  "product_reporting"."inventory_selling" AS "inventory_selling",
	  "product_reporting"."inventory_sold" AS "inventory_sold",
	  "product_reporting"."inventory_sold_to_customers" AS "inventory_sold_to_customers",
	  "product_reporting"."inventory_sold_to_3rdparty" AS "inventory_sold_to_3rdparty",
	  "product_reporting"."inventory_sold_others" AS "inventory_sold_others",
	  "product_reporting"."inventory_inbound" AS "inventory_inbound",
	  "product_reporting"."inventory_inbound_unallocable" AS "inventory_inbound_unallocable",
	  "product_reporting"."inventory_transition" AS "inventory_transition",
	  "product_reporting"."inventory_writtenoff_ops" AS "inventory_writtenoff_ops",
	  "product_reporting"."inventory_writtenoff_dc" AS "inventory_writtenoff_dc",
	  "product_reporting"."inventory_writtenoff" AS "inventory_writtenoff",
	  "custom sql query"."investment_capital" AS "investment_capital",
	  "product_reporting"."nb_assets_inportfolio_todate" AS "nb_assets_inportfolio_todate",
	  "product_reporting"."order_created_date" AS "order_created_date",
	  "product_reporting"."pageview_unique_sessions" AS "pageview_unique_sessions",
	  "product_reporting"."pageviews" AS "pageviews",
	  "product_reporting"."pageviews_w_url" AS "pageviews_w_url",
	  "product_reporting"."paid_long_term_orders" AS "paid_long_term_orders",
	  "product_reporting"."paid_new_orders" AS "paid_new_orders",
	  "product_reporting"."paid_orders" AS "paid_orders",
	  "product_reporting"."paid_short_term_orders" AS "paid_short_term_orders",
	  "product_reporting"."paid_traffic_sessions" AS "paid_traffic_sessions",
	  CAST("product_reporting"."product_name" AS TEXT) AS "product_name",
	  CAST("custom sql query"."product_sku" AS TEXT) AS "product_sku (custom sql query)",
	  CAST("product_reporting"."product_sku" AS TEXT) AS "product_sku",
	  "product_reporting"."returned_within_7_days" AS "returned_within_7_days",
	  "custom sql query"."status" AS "status",
	  "product_reporting"."stock_at_hand" AS "stock_at_hand",
	  CAST("product_reporting"."store_country" AS TEXT) AS "store_country",
	  CAST("product_reporting"."store_id" AS TEXT) AS "store_id",
	  CAST("product_reporting"."store_label" AS TEXT) AS "store_label",
	  "custom sql query"."store_name" AS "store_name (custom sql query)",
	  CAST("product_reporting"."store_name" AS TEXT) AS "store_name",
	  "product_reporting"."store_short" AS "store_short",
	  CAST("product_reporting"."subcategory_name" AS TEXT) AS "subcategory_name",
	  CASE WHEN "product_reporting".store_name LIKE '%B2B%' AND "product_reporting".store_country = 'Germany' 
					THEN 'DE - B2B'
				 WHEN "product_reporting".store_name LIKE '%B2B%' AND "product_reporting".store_country = 'Austria' 
				 	THEN 'AT - B2B'
				 WHEN "product_reporting".store_name LIKE '%B2B%' AND "product_reporting".store_country = 'Netherlands' 
				 	THEN 'NL - B2B'
				 WHEN "product_reporting".store_name LIKE '%B2B%' AND "product_reporting".store_country = 'Spain' 
				 	THEN 'ES - B2B'
				 WHEN "product_reporting".store_name LIKE '%B2B%' AND "product_reporting".store_country = 'United States' 
				 	THEN 'US - B2B'
				 WHEN "product_reporting".store_short LIKE '%Partners%' AND "product_reporting".store_country = 'Germany'
				 	THEN 'DE - Retail'
				 WHEN "product_reporting".account_name = 'Grover - Germany' 
				 	THEN 'DE - B2C'
				 WHEN "product_reporting".account_name = 'Grover - Austria' 
				 	THEN 'AT - B2C'
				 WHEN "product_reporting".account_name = 'Grover - Netherlands' 
				 	THEN 'NL - B2C'
				 WHEN "product_reporting".account_name = 'Grover - Spain' 
				 	THEN 'ES - B2C'
				 WHEN "product_reporting".account_name = 'Grover - United States' 
				 	THEN 'US - B2C'
				 END AS store_targets
	FROM "dwh"."product_reporting" "product_reporting" 
	LEFT JOIN (
	  WITH fact_days AS (
	  SELECT DISTINCT datum AS fact_day FROM public.dim_dates
	  WHERE datum<=CURRENT_DATE)
	  SELECT 
	  	fact_day,
	  	product_sku,
	  	'Germany' AS store_name,
	  	'Excluding Sold Assets' AS status,
	  	sum(s.initial_price) AS investment_capital
	  FROM fact_days f, ods_production.asset AS s
	  WHERE f.fact_day>=s.purchased_date AND s.asset_status_grouped<>'NEVER PURCHASED' AND (f.fact_day<=s.sold_date OR s.sold_date IS NULL) 	
	  GROUP BY 1,2,3,4
	) "custom sql query" ON (("product_reporting"."fact_day" = "custom sql query"."fact_day") AND (CAST("product_reporting"."product_sku" AS TEXT) = CAST("custom sql query"."product_sku" AS TEXT)) AND (CAST("product_reporting"."store_name" AS TEXT) = "custom sql query"."store_name"))
)
,
category_targets AS 
(
	SELECT
		to_date AS fact_day,
		store_label AS country,
		channel_type AS channel_type,
		categories AS category_name,
		sum(amount) AS active_subscription_value_target
	FROM dm_commercial.r_commercial_daily_targets_since_2022
	WHERE measures = 'ASV'
	GROUP BY 1,2,3,4
)
SELECT 
		p.*,
		c.active_subscription_value_target
FROM product_reporting_mix p
LEFT JOIN category_targets c 
	ON p.category_name = c.category_name
		AND p.fact_day = c.fact_day
		AND c.country + ' - ' + c.channel_type = p.store_targets
WHERE p.account_name NOT IN ('Grover - USA old' , 'Grover - UK')
WITH NO SCHEMA BINDING;