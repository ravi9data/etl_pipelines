CREATE OR REPLACE VIEW dm_commercial.v_ranking_report_export AS 
WITH current_week AS (
	SELECT
		'WoW_group_A' + CAST(current_date - 7 AS varchar(10)) + '_' + CAST(current_date - 1 AS varchar(10)) AS time_definition,
		store_id, 
		store_name,
		store_country,
		product_sku,
		CASE 
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
		sum(paid_orders) AS paid_orders_current_week,
		sum(completed_orders) AS submitted_orders_current_week,
       	sum(pageviews) AS page_views_current_week,
       	sum(acquired_subscription_value) AS acquired_subscription_value_current_week,
       	sum(cancelled_subscriptions) AS cancelled_subscriptions_current_week,
       	sum(acquired_subscriptions) AS acquired_subscriptions_current_week,
       	sum(cancelled_subscription_value) AS cancelled_subscription_value_current_week
	FROM dwh.product_reporting
	WHERE (fact_day BETWEEN DATEADD('day', -7, DATE_TRUNC('week', current_date))::date AND DATEADD('day', -1, DATE_TRUNC('week', current_date))::date)
	GROUP BY 
		store_name,
		store_id,
		store_country,
		product_sku,
		customer_type,
		store_short
)
,
current_month AS (
	SELECT
		'MoM_group_A' + CAST(current_date - 28 AS varchar(10)) + '_' + CAST(current_date - 1 AS varchar(10)) AS time_definition,
		store_id,
		store_name,
		store_country,
		product_sku,
		CASE 
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
		sum(paid_orders) AS paid_orders_current_month,
		sum(completed_orders) AS submitted_orders_current_month,
       	sum(pageviews) AS page_views_current_month,
       	sum(acquired_subscription_value) AS acquired_subscription_value_current_month,
       	sum(cancelled_subscriptions) AS cancelled_subscriptions_current_month,
       	sum(acquired_subscriptions) AS acquired_subscriptions_current_month,
       	sum(cancelled_subscription_value) AS cancelled_subscription_value_current_month
	FROM dwh.product_reporting
	WHERE (fact_day BETWEEN DATEADD('day', -28, DATE_TRUNC('week', current_date))::date AND DATEADD('day', -1, DATE_TRUNC('week', current_date))::date)
	GROUP BY 
		store_name,
		store_id,
		store_country,
		product_sku,
		customer_type,
		store_short
)
,
previous_week AS (
	SELECT
		'WoW_group_B' + CAST(current_date - 14 AS varchar(10)) + '_' + CAST(current_date - 8 AS varchar(10)) AS time_definition,
		store_id,
		store_name,
		store_country,
		product_sku,
		CASE 
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
		sum(paid_orders) AS paid_orders_previous_week,
		sum(completed_orders) AS submitted_orders_previous_week,
       	sum(pageviews) AS page_views_previous_week,
       	sum(acquired_subscription_value) AS acquired_subscription_value_previous_week,
       	sum(cancelled_subscriptions) AS cancelled_subscriptions_previous_week,
       	sum(acquired_subscriptions) AS acquired_subscriptions_previous_week,
       	sum(cancelled_subscription_value) AS cancelled_subscription_value_previous_week
	FROM dwh.product_reporting
	WHERE (fact_day BETWEEN DATEADD('day', -14, DATE_TRUNC('week', current_date))::date AND DATEADD('day', -8, DATE_TRUNC('week', current_date))::date)
	GROUP BY 
		store_name,
		store_id,
		store_country,
		product_sku,
		customer_type,
		store_short
)
,
previous_month AS (
	SELECT
		'MoM_group_B' + CAST(current_date - 56 AS varchar(10)) + '_' + CAST(current_date - 29 AS varchar(10)) AS time_definition,
		store_name,
		store_id,
		store_country,
		product_sku,
		CASE 
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
		sum(paid_orders) AS paid_orders_previous_month,
		sum(completed_orders) AS submitted_orders_previous_month,
       	sum(pageviews) AS page_views_previous_month,
       	sum(acquired_subscription_value) AS acquired_subscription_value_previous_month,
       	sum(cancelled_subscriptions) AS cancelled_subscriptions_previous_month,
       	sum(acquired_subscriptions) AS acquired_subscriptions_previous_month,
       	sum(cancelled_subscription_value) AS cancelled_subscription_value_previous_month
	FROM dwh.product_reporting
	WHERE (fact_day BETWEEN DATEADD('day', -56, DATE_TRUNC('week', current_date))::date AND DATEADD('day', -29, DATE_TRUNC('week', current_date))::date) 
	GROUP BY 
		store_name,
		store_id,
		store_country,
		product_sku,
		customer_type,
		store_short
)
, 
before_previous_week AS (
	SELECT
		'WoW_group_C' + CAST(current_date - 21 AS varchar(10)) + '_' + CAST(current_date - 15 AS varchar(10)) AS time_definition,
		store_name,
		store_id,
		store_country,
		product_sku,
		CASE
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
       	sum(acquired_subscription_value) AS acquired_subscription_value_before_previous_week,
       	sum(acquired_subscriptions) AS acquired_subscription_before_previous_week
	FROM dwh.product_reporting
	WHERE (fact_day BETWEEN DATEADD('day', -21, DATE_TRUNC('week', current_date))::date AND DATEADD('day', -15, DATE_TRUNC('week', current_date))::date)
	GROUP BY
		store_name,
		store_id,
		store_country,
		product_sku,
		customer_type,
		store_short 
)
,
weeks_back_5 AS (
	SELECT
		'WoW_group_D' + CAST(current_date - 21 AS varchar(10)) + '_' + CAST(current_date - 15 AS varchar(10)) AS time_definition,
		store_name,
		store_id,
		store_country,
		product_sku,
		CASE
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
       	sum(acquired_subscription_value) AS acquired_subscription_value_5_weeks_back,
       	sum(acquired_subscriptions) AS acquired_subscription_5_weeks_back
	FROM dwh.product_reporting
	WHERE (fact_day BETWEEN DATEADD('day', -35, DATE_TRUNC('week', current_date))::date AND DATEADD('day', -29, DATE_TRUNC('week', current_date))::date)
	GROUP BY
		store_name,
		store_id,
		store_country,
		product_sku,
		customer_type,
		store_short 
)
,
end_of_period AS (
	SELECT 
		store_name,
		store_id,
		store_country,
		product_sku,
		CASE 
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
		sum(active_subscriptions) AS active_subscriptions_eop,
		sum(active_subscription_value) AS active_subscription_value_eop,
		sum(nb_assets_inportfolio_todate) AS assets_on_books_eop,
		sum(asset_investment) AS asset_investment_eop,
		sum(asset_market_value_todate) AS total_market_evaluation_eop,
		sum(stock_at_hand) AS stock_on_hand_eop
	FROM dwh.product_reporting
	WHERE fact_day = (SELECT max(fact_day) FROM dwh.product_reporting)
	GROUP BY 1,2,3,4,5,6
)
,
end_of_period_previous_month AS (
	SELECT 
		store_name,
		store_id,
		store_country,
		product_sku,
		CASE 
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
		sum(active_subscription_value) AS active_subscription_value_eop_previous_month,
		sum(nb_assets_inportfolio_todate) AS assets_on_books_eop_previous_month,
		sum(stock_at_hand) AS stock_on_hand_eop_previous_month
	FROM dwh.product_reporting 
	WHERE fact_day = DATEADD('day',-1,DATE_TRUNC('month',current_date))::date
	GROUP BY 1,2,3,4,5,6
)
,
active_subscription_value_29_days_back AS (
	SELECT 
		store_name,
		store_id,
		store_country,
		product_sku,
		CASE 
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
		sum(active_subscription_value) AS active_subscription_value_29_days_back
	FROM dwh.product_reporting
	WHERE fact_day = DATEADD('day',-29,date_trunc('day',current_date))::date
	GROUP BY 1,2,3,4,5,6
)
,
this_week_ly_asv AS (
	SELECT 
		store_name,
		store_id,
		store_country,
		product_sku,
		CASE 
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
		sum(active_subscription_value) AS active_subscription_value_this_week_ly
	FROM dwh.product_reporting
	WHERE fact_day = DATEADD('week',-53,dateadd('day',-1,date_trunc('week',current_date)))::date
	GROUP BY 1,2,3,4,5,6
)
, 
this_week_ly_subs AS (
	SELECT
		store_name,
		store_id,
		store_country,
		product_sku,
		CASE
			WHEN store_name LIKE '%B2B%'
				THEN 'Business_customer'
			ELSE 'Normal_customer'
		END AS customer_type,
		CASE 
			WHEN store_short LIKE '%Partners%'
				THEN 'Retail'
			ELSE 'Normal'
		END AS store_short,
       	sum(acquired_subscription_value) AS acquired_subscription_value_this_week_ly,
       	sum(acquired_subscriptions) AS acquired_subscription_this_week_ly
	FROM dwh.product_reporting
	WHERE (fact_day BETWEEN DATEADD('day', -371, DATE_TRUNC('week', current_date))::date AND  DATEADD('day', -365, DATE_TRUNC('week', current_date))::date)
	GROUP BY
		store_name,
		store_id,
		store_country,
		product_sku,
		customer_type,
		store_short 
)
SELECT 
		st.store_type,
		w1.store_country,
		w1.store_short,
		w1.customer_type,
		w1.product_sku,
		w1.paid_orders_current_week AS paid_orders_WoW_Group_A,
		w2.paid_orders_previous_week AS paid_orders_WoW_Group_B,
		m1.paid_orders_current_month AS paid_orders_MoM_Group_A,
		m2.paid_orders_previous_month AS paid_orders_MoM_Group_B,
		w1.submitted_orders_current_week AS submitted_orders_WoW_Group_A,
		w2.submitted_orders_previous_week AS submitted_orders_WoW_Group_B,
		m1.submitted_orders_current_month AS submitted_orders_MoM_Group_A,
		m2.submitted_orders_previous_month AS submitted_orders_MoM_Group_B,
       	w1.page_views_current_week AS page_views_WoW_Group_A,
       	w2.page_views_previous_week AS page_views_WoW_Group_B,
       	m1.page_views_current_month AS page_views_MoM_Group_A,
       	m2.page_views_previous_month AS page_views_MoM_Group_B,
       	w1.acquired_subscription_value_current_week AS acquired_subscription_value_WoW_Group_A,
       	w2.acquired_subscription_value_previous_week AS acquired_subscription_value_WoW_Group_B,
       	m1.acquired_subscription_value_current_month AS acquired_subscription_value_MoM_Group_A,
       	m2.acquired_subscription_value_previous_month AS acquired_subscription_value_MoM_Group_B,
       	w1.cancelled_subscriptions_current_week AS cancelled_subscriptions_WoW_Group_A,
       	w2.cancelled_subscriptions_previous_week AS cancelled_subscriptions_WoW_Group_B,
       	m1.cancelled_subscriptions_current_month AS cancelled_subscriptions_MoM_Group_A,
       	m2.cancelled_subscriptions_previous_month AS cancelled_subscriptions_MoM_Group_B,
       	w1.acquired_subscriptions_current_week AS acquired_subscriptions_WoW_Group_A,
       	w2.acquired_subscriptions_previous_week AS acquired_subscriptions_WoW_Group_B,
       	w3.acquired_subscription_value_before_previous_week AS acquired_subscription_value_WoW_Group_C,
       	w3.acquired_subscription_before_previous_week AS acquired_subscriptions_WoW_Group_C,
       	w5.acquired_subscription_value_5_weeks_back AS acquired_subscription_value_WoW_5weeks_back,
       	m1.acquired_subscriptions_current_month AS acquired_subscriptions_MoM_Group_A,
       	m2.acquired_subscriptions_previous_month AS acquired_subscriptions_MoM_Group_B,
       	w1.cancelled_subscription_value_current_week AS cancelled_subscription_value_WoW_Group_A,
       	w2.cancelled_subscription_value_previous_week AS cancelled_subscription_value_WoW_Group_B,
       	m1.cancelled_subscription_value_current_month AS cancelled_subscription_value_MoM_Group_A,
       	m2.cancelled_subscription_value_previous_month AS cancelled_subscription_value_MoM_Group_B,
       	e.active_subscriptions_eop,
		e.active_subscription_value_eop,-----A
		e.assets_on_books_eop,
		e.asset_investment_eop,
		e.total_market_evaluation_eop,
		pm.active_subscription_value_eop_previous_month,----B
		db.active_subscription_value_29_days_back AS active_subscription_value_29_days_back,
		pm.assets_on_books_eop_previous_month,
		e.stock_on_hand_eop AS stock_on_hand_eop,
       	pm.stock_on_hand_eop_previous_month AS stock_on_hand_eop_previous_month,
       	COALESCE(e.active_subscription_value_eop,0) - COALESCE(pm.active_subscription_value_eop_previous_month,0) AS asv_month_to_date,------A-B
       	COALESCE(ly.active_subscription_value_this_week_ly) AS active_subscription_value_this_week_ly,
       	COALESCE(ls.acquired_subscription_value_this_week_ly) AS acquired_subscription_value_this_week_ly,
       	COALESCE(ls.acquired_subscription_this_week_ly) AS acquired_subscription_this_week_ly
FROM current_week w1 
LEFT JOIN current_month m1
	ON w1.store_name = m1.store_name 
	AND w1.store_country = m1.store_country
	AND w1.product_sku = m1.product_sku
	AND w1.store_name = m1.store_name
	AND w1.store_short = m1.store_short
	AND w1.store_id = m1.store_id
LEFT JOIN previous_week w2
	ON w1.store_name = w2.store_name 
	AND w1.store_country = w2.store_country
	AND w1.product_sku = w2.product_sku
	AND w1.store_name = w2.store_name
	AND w1.store_short = w2.store_short
	AND w1.store_id = w2.store_id
LEFT JOIN previous_month m2
	ON w1.store_name = m2.store_name 
	AND w1.store_country = m2.store_country
	AND w1.product_sku = m2.product_sku
	AND w1.store_name = m2.store_name
	AND w1.store_short = m2.store_short
	AND w1.store_id = m2.store_id
LEFT JOIN before_previous_week w3
	ON w1.store_name = w3.store_name 
	AND w1.store_country = w3.store_country
	AND w1.product_sku = w3.product_sku
	AND w1.store_name = w3.store_name
	AND w1.store_short = w3.store_short
	AND w1.store_id = w3.store_id
LEFT JOIN weeks_back_5 w5
	ON w1.store_name = w5.store_name 
	AND w1.store_country = w5.store_country
	AND w1.product_sku = w5.product_sku
	AND w1.store_name = w5.store_name
	AND w1.store_short = w5.store_short
	AND w1.store_id = w5.store_id
LEFT JOIN end_of_period e 
	ON w1.store_name = e.store_name 
	AND w1.store_country = e.store_country 
	AND w1.product_sku = e.product_sku
	AND w1.store_name = e.store_name
	AND w1.store_short = e.store_short
	AND w1.store_id = e.store_id
LEFT JOIN end_of_period_previous_month pm
	ON w1.store_name = pm.store_name 
	AND w1.store_country = pm.store_country 
	AND w1.product_sku = pm.product_sku
	AND w1.store_name = pm.store_name
	AND w1.store_short = pm.store_short
	AND w1.store_id = pm.store_id
LEFT JOIN active_subscription_value_29_days_back db
	ON w1.store_name = db.store_name 
	AND w1.store_country = db.store_country 
	AND w1.product_sku = db.product_sku
	AND w1.store_name = db.store_name
	AND w1.store_short = db.store_short
	AND w1.store_id = db.store_id
LEFT JOIN this_week_ly_asv ly	
	ON w1.store_name = ly.store_name 
	AND w1.store_country = ly.store_country 
	AND w1.product_sku = ly.product_sku
	AND w1.store_name = ly.store_name
	AND w1.store_short = ly.store_short
	AND w1.store_id = ly.store_id
LEFT JOIN this_week_ly_subs ls	
	ON w1.store_name = ls.store_name 
	AND w1.store_country = ls.store_country 
	AND w1.product_sku = ls.product_sku
	AND w1.store_name = ls.store_name
	AND w1.store_short = ls.store_short
	AND w1.store_id = ls.store_id
LEFT JOIN ods_production.store st 
	ON w1.store_id = st.id
WITH NO SCHEMA BINDING 
;
