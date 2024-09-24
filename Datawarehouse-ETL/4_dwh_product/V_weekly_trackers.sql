CREATE OR REPLACE VIEW dm_commercial.v_weekly_trackers AS
WITH dates AS (
SELECT
	DISTINCT 
 datum AS fact_date,
	date_trunc('month', datum)::DATE AS month_bom,
	LEAST(DATE_TRUNC('MONTH', DATEADD('MONTH', 1, DATUM))::DATE-1, CURRENT_DATE) AS MONTH_EOM
FROM
	public.dim_dates
WHERE
	datum <= current_date 
)
, fx AS (
	SELECT 
		dateadd('day',1,date_) as fact_month,
		date_,
		currency,
		exchange_rate_eur
	FROM trans_dev.daily_exchange_rate  er
	LEFT JOIN public.dim_dates dd on er.date_ = dd.datum 
	WHERE day_is_last_of_month
)
,
active_subs AS (
SELECT
	fact_date,
	CASE
		WHEN s.store_commercial LIKE ('%B2B%')
			AND fm.is_freelancer = 1
  THEN 'B2B-Freelancers'
			WHEN s.store_commercial LIKE ('%B2B%')
  THEN 'B2B-Non Freelancers'
			WHEN s.store_commercial LIKE ('%Partnerships%') 
  THEN 'Retail'
			ELSE 'B2C'
		END AS channel_type,
		CASE
			WHEN s.country = 'Germany'
  THEN 'DE'
			WHEN s.country = 'Netherlands' 
  THEN 'NL'
			WHEN s.country = 'Austria'
  THEN 'AT'
			WHEN s.country = 'Spain'
  THEN 'ES'
			WHEN s.country = 'United States'
  THEN 'US'
			ELSE 'DE'
		END AS store_label,
		month_bom,
		month_eom,
		s.category_name,
		s.subcategory_name,
		--s.currency,
		--COALESCE(country, 'No Info') AS country,
		count(DISTINCT s.subscription_id) AS active_subscriptions,
		count(DISTINCT 
  CASE 
   WHEN s.cancellation_reason_churn <> 'failed delivery' 
    AND last_valid_payment_category LIKE ('%DEFAULT%') 
    AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
    AND default_date < fact_date 
   THEN subscription_id 
  END) AS active_default_subscriptions,
		count(DISTINCT s.customer_id) AS active_customers,
		count(DISTINCT 
  CASE 
    WHEN s.cancellation_reason_churn <> 'failed delivery' 
   AND last_valid_payment_category LIKE ('%DEFAULT%') 
    AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
    AND default_date < fact_date 
   THEN customer_id 
  END) AS active_default_customers,
		sum(s.subscription_value_euro) AS active_subscription_value,
		sum(s.subscription_value) AS asv_in_currency,
		sum( 
  CASE 
    WHEN s.cancellation_reason_churn <> 'failed delivery' 
   AND last_valid_payment_category LIKE ('%DEFAULT%') 
    AND last_valid_payment_category NOT LIKE ('%RECOVERY%') 
    AND default_date < fact_date 
   THEN s.subscription_value_euro 
  END) AS active_default_subscription_value,
		count(DISTINCT CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS active_subscriptions_failed_delivery,
		count(DISTINCT CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.customer_id END) AS active_customers_failed_delivery,
		sum(CASE WHEN s.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value_euro END) AS active_subscription_value_failed_delivery
	FROM
		dates d
	LEFT JOIN (
		SELECT
			s.subscription_id,
			s.store_label,
			s.store_commercial,
			s.customer_id,
			s.category_name,
			s.subcategory_name,
			c2.company_type_name,
			s.subscription_value_eur AS subscription_value_euro,
			s.subscription_value_lc AS subscription_value,
			--s.currency,
			s.end_date,
			s.fact_day,
			s.cancellation_date,
			c.cancellation_reason_churn,
			cf.last_valid_payment_category,
			cf.default_date + 31 AS default_date,
			s.country_name AS country
		FROM
			ods_production.subscription_phase_mapping s
		LEFT JOIN ods_production.subscription_cancellation_reason c 
 ON
			s.subscription_id = c.subscription_id
		LEFT JOIN ods_production.subscription_cashflow cf 
 ON
			s.subscription_id = cf.subscription_id
		LEFT JOIN ods_production.companies c2 
 ON
			c2.customer_id = s.customer_id
 ) s 
ON d.fact_date::date >= s.fact_Day::date 
		AND d.fact_date::date <= coalesce(s.end_date::date, d.fact_date::date+1)
	LEFT JOIN dm_risk.b2b_freelancer_mapping fm
 ON
		fm.company_type_name = s.company_type_name
	LEFT JOIN fx 
		ON fx.fact_month = 
			(CASE WHEN DATE_TRUNC('month',d.fact_date) <= '2021-04-01' THEN '2021-04-01'
					ELSE DATE_TRUNC('month',d.fact_date) END)
	WHERE
		s.store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
			AND fact_date >= dateadd('year',-1,current_date)
		GROUP BY
			1,2,3,4,5,6,7--,8
		ORDER BY
			1 DESC
)
,
active_subs_wow AS (
SELECT
		fact_date,
		channel_type, 
		store_label, 
		month_bom, 
		month_eom, 
		category_name, 
		subcategory_name,
		active_subscription_value,
		asv_in_currency,
		active_subscriptions,
	LAG(active_subscription_value, 7) OVER (PARTITION BY channel_type,
	store_label,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_value_wow,
	LAG(asv_in_currency, 7) OVER (PARTITION BY channel_type,
	store_label,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_value_wow_in_currency,
	LAG(active_subscriptions, 7) OVER (PARTITION BY channel_type,
	store_label,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_units_wow,	
	LAG(active_subscription_value, 28) OVER (PARTITION BY channel_type,
	store_label,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_value_mom,
	LAG(asv_in_currency, 28) OVER (PARTITION BY channel_type,
	store_label,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_value_mom_in_currency,
	LAG(active_subscriptions, 28) OVER (PARTITION BY channel_type,
	store_label,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_units_mom,
	LAG(active_subscription_value, DATEDIFF('day', date_trunc('month', fact_date), fact_date)::int) OVER (PARTITION BY channel_type,
	store_label,
	month_bom,
	month_eom,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_value_mtd,
	LAG(asv_in_currency, DATEDIFF('day', date_trunc('month', fact_date), fact_date)::int) OVER (PARTITION BY channel_type,
	store_label,
	month_bom,
	month_eom,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_value_mtd_in_currency,
	LAG(active_subscriptions, DATEDIFF('day', date_trunc('month', fact_date), fact_date)::int) OVER (PARTITION BY channel_type,
	store_label,
	month_bom,
	month_eom,
	category_name,
	subcategory_name
ORDER BY
	fact_date) AS sub_units_mtd
FROM
	active_subs
)
, asv_yoy AS (
SELECT 
		channel_type, 
		store_label, 
		category_name, 
		subcategory_name,
		active_subscription_value,
		asv_in_currency,
		active_subscriptions
FROM
	active_subs
WHERE
	dateadd('year',
	-1,
	current_date) = fact_date
)
,
acquisition AS (
SELECT
	s.start_date::date AS start_date,
	CASE
		WHEN s.store_commercial LIKE ('%B2B%')
			AND fm.is_freelancer = 1 
  THEN 'B2B-Freelancers'
			WHEN s.store_commercial LIKE ('%B2B%')
  THEN 'B2B-Non Freelancers'
			WHEN s.store_commercial LIKE ('%Partnerships%') 
  THEN 'Retail'
			ELSE 'B2C'
		END AS channel_type,
		CASE
			WHEN s.country_name = 'Germany'
  THEN 'DE'
			WHEN s.country_name = 'Netherlands' 
  THEN 'NL'
			WHEN s.country_name = 'Austria'
  THEN 'AT'
			WHEN s.country_name = 'Spain'
  THEN 'ES'
			WHEN s.country_name = 'United States'
  THEN 'US'
			ELSE 'DE'
		END AS store_label,
		s.category_name,
		s.subcategory_name,
		count(s.subscription_id) AS new_subscriptions,
		count(DISTINCT s.order_id) AS new_orders,
		sum(CASE WHEN s.store_label IN ('Grover - United States online') 
						THEN (s.subscription_value * exchange_rate_eur)::decimal(10,2)
						ELSE s.subscription_value_euro END) AS acquired_subscription_value,
		sum(s.subscription_value) AS acquired_asv_in_currency,
		sum(s.committed_sub_value) AS acquired_committed_sub_value,
		count(CASE WHEN retention_group = 'RECURRING, UPSELL' THEN s.subscription_id END) AS upsell_subscriptions,
		count(CASE WHEN retention_group = 'RECURRING, REACTIVATION' THEN s.subscription_id END) AS reactivation_subscriptions,
		sum(CASE WHEN new_recurring = 'NEW' THEN s.subscription_value_euro END) AS new_subscription_value,
		sum(CASE WHEN retention_group = 'RECURRING, UPSELL' THEN s.subscription_value_euro END) AS upsell_subscription_value,
		sum(CASE WHEN retention_group = 'RECURRING, REACTIVATION' THEN s.subscription_value_euro END) AS reactivation_subscription_value,
		count(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND new_recurring = 'NEW' THEN s.subscription_id END) AS new_subscriptions_failed_delivery,
		count(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND retention_group = 'RECURRING, UPSELL' THEN s.subscription_id END) AS upsell_subscriptions_failed_delivery,
		count(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND retention_group = 'RECURRING, REACTIVATION' THEN s.subscription_id END) AS reactivation_subscriptions_failed_delivery,
		sum(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND new_recurring = 'NEW' THEN s.subscription_value_euro END) AS new_subscription_value_failed_delivery,
		sum(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND retention_group = 'RECURRING, UPSELL' THEN s.subscription_value_euro END) AS upsell_subscription_value_failed_delivery,
		sum(CASE WHEN c.cancellation_reason_churn = 'failed delivery' AND retention_group = 'RECURRING, REACTIVATION' THEN s.subscription_value_euro END) AS reactivation_subscription_value_failed_delivery
	FROM
		ods_production.subscription s
	LEFT JOIN ods_production.order_retention_group o 
 ON
		o.order_id = s.order_id
	LEFT JOIN ods_production.subscription_cancellation_reason c 
 ON
		s.subscription_id = c.subscription_id
	LEFT JOIN ods_production.companies c2 
 ON
		c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping fm 
 ON
		fm.company_type_name = c2.company_type_name
	LEFT JOIN fx 
			ON fx.fact_month = 
				(CASE WHEN DATE_TRUNC('month',s.start_date) <= '2021-04-01' THEN '2021-04-01'
					ELSE DATE_TRUNC('month',s.start_date) END)
	WHERE
		store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY
		1,2,3,4,5
)
,
churn AS (
SELECT
	s.cancellation_date::date AS cancellation_date,
	CASE
		WHEN s.store_commercial LIKE ('%B2B%')
			AND fm.is_freelancer = 1 
  THEN 'B2B-Freelancers'
			WHEN s.store_commercial LIKE ('%B2B%')
  THEN 'B2B-Non Freelancers'
			WHEN s.store_commercial LIKE ('%Partnerships%') 
  THEN 'Retail'
			ELSE 'B2C'
		END AS channel_type,
		CASE
			WHEN s.country_name = 'Germany'
  THEN 'DE'
			WHEN s.country_name = 'Netherlands' 
  THEN 'NL'
			WHEN s.country_name = 'Austria'
  THEN 'AT'
			WHEN s.country_name = 'Spain'
  THEN 'ES'
			WHEN s.country_name = 'United States'
  THEN 'US'
			ELSE 'DE'
		END AS store_label,
		s.category_name,
		s.subcategory_name,
		count(s.subscription_id) AS cancelled_subscriptions,
		sum(CASE WHEN s.store_label IN ('Grover - United States online') 
					THEN (s.subscription_value * exchange_rate_eur)::decimal(10,2)
					ELSE s.subscription_value_euro END) AS cancelled_subscription_value,
		sum(s.subscription_value) AS cancelled_asv_in_currency,
		count(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_id END) AS cancelled_subscriptions_failed_delivery,
		sum(CASE WHEN c.cancellation_reason_churn = 'failed delivery' THEN s.subscription_value_euro END) AS cancelled_subscription_value_failed_delivery,
		count(CASE WHEN c.cancellation_reason_churn = 'customer request' THEN s.subscription_id END) AS cancelled_subscriptions_customer_request,
		sum(CASE WHEN c.cancellation_reason_churn = 'customer request' THEN s.subscription_value_euro END) AS cancelled_subscription_value_customer_request
	FROM
		ods_production.subscription s
	LEFT JOIN ods_production.subscription_cancellation_reason c ON
		s.subscription_id = c.subscription_id
	LEFT JOIN ods_production.companies c2 ON
		c2.customer_id = s.customer_id
	LEFT JOIN dm_risk.b2b_freelancer_mapping fm
 ON
		fm.company_type_name = c2.company_type_name
LEFT JOIN fx 
			ON fx.fact_month = 
				(CASE WHEN DATE_TRUNC('month',s.cancellation_date) <= '2021-04-01' THEN '2021-04-01'
					ELSE DATE_TRUNC('month',s.cancellation_date) END)
	WHERE
		store_label NOT IN ('Grover - USA old online', 'Grover - UK online')
	GROUP BY
		1,2,3,4,5
)
, 
asv_targets AS (
	SELECT 
		to_date AS datum,
		store_label,
		CASE 
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Freelancers'
				THEN 'B2B-Freelancers'
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Non-Freelancers'
				THEN 'B2B-Non Freelancers'
			WHEN channel_type = 'B2C' 
				THEN 'B2C'
			ELSE 'Retail'
		END AS channel_type,
		categories AS category_name,
		subcategories AS subcategory_name,
		amount AS asv_target,
		CASE WHEN store_label = 'US' THEN (asv_target/fx.exchange_rate_eur)::decimal(10,2)
			 ELSE asv_target END AS asv_target_in_currency
	FROM
		dm_commercial.r_commercial_daily_targets_since_2022
	LEFT JOIN fx
		ON (CASE WHEN DATE_TRUNC('month',to_date) > DATE_TRUNC('month',current_date) THEN DATE_TRUNC('month',current_date)
				ELSE DATE_TRUNC('month',to_date) END) = fx.fact_month
	WHERE
		measures = 'ASV'	
)
,
acquired_asv_targets AS (
	SELECT 
		to_date AS datum,
		store_label,
		CASE 
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Freelancers'
				THEN 'B2B-Freelancers'
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Non-Freelancers'
				THEN 'B2B-Non Freelancers'
			WHEN channel_type = 'B2C' 
				THEN 'B2C'
			ELSE 'Retail'
		END AS channel_type,
		categories AS category_name,
		subcategories AS subcategory_name,
		amount AS acquired_asv_target,
		CASE WHEN store_label = 'US' THEN (acquired_asv_target/fx.exchange_rate_eur)::decimal(10,2)
			 ELSE acquired_asv_target END AS acquired_asv_target_in_currency
	FROM
		dm_commercial.r_commercial_daily_targets_since_2022
	LEFT JOIN fx
		ON (CASE WHEN DATE_TRUNC('month',to_date) > DATE_TRUNC('month',current_date) THEN DATE_TRUNC('month',current_date)
				ELSE DATE_TRUNC('month',to_date) END) = fx.fact_month
	WHERE
		measures = 'Acquired ASV'
)
,
cancelled_asv_targets AS (
	SELECT 
		to_date AS datum,
		store_label,
		CASE 
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Freelancers'
				THEN 'B2B-Freelancers'
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Non-Freelancers'
				THEN 'B2B-Non Freelancers'
			WHEN channel_type = 'B2C' 
				THEN 'B2C'
			ELSE 'Retail'
		END AS channel_type,
		categories AS category_name,
		subcategories AS subcategory_name,
		amount AS cancelled_asv_target,
		CASE WHEN store_label = 'US' THEN (cancelled_asv_target/fx.exchange_rate_eur)::decimal(10,2)
			 ELSE cancelled_asv_target END AS cancelled_asv_target_in_currency
	FROM
		dm_commercial.r_commercial_daily_targets_since_2022
	LEFT JOIN fx
		ON (CASE WHEN DATE_TRUNC('month',to_date) > DATE_TRUNC('month',current_date) THEN DATE_TRUNC('month',current_date)
				ELSE DATE_TRUNC('month',to_date) END) = fx.fact_month
	WHERE
		measures = 'Cancelled ASV'
)
,acquired_subscriptions_target AS (
	SELECT 
		to_date AS datum,
		store_label,
		CASE 
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Freelancers'
				THEN 'B2B-Freelancers'
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Non-Freelancers'
				THEN 'B2B-Non Freelancers'
			WHEN channel_type = 'B2C' 
				THEN 'B2C'
			ELSE 'Retail'
		END AS channel_type,
		categories AS category_name,
		subcategories AS subcategory_name,
		amount AS acquired_subscriptions_target
	FROM dm_commercial.r_commercial_daily_targets_since_2022
	WHERE measures = 'Acquired Subscriptions'
)
,cancelled_subscriptions_target AS (
	SELECT 
		to_date AS datum,
		store_label,
		CASE 
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Freelancers'
				THEN 'B2B-Freelancers'
			WHEN channel_type = 'B2B'
			AND b2b_split = 'Non-Freelancers'
				THEN 'B2B-Non Freelancers'
			WHEN channel_type = 'B2C' 
				THEN 'B2C'
			ELSE 'Retail'
		END AS channel_type,
		categories AS category_name,
		subcategories AS subcategory_name,
		amount AS cancelled_subscriptions_target
	FROM dm_commercial.r_commercial_daily_targets_since_2022
	WHERE measures = 'Cancelled Subscriptions'
)
---new part to sort target discrepancies
, union_for_all_subcategories AS (
	SELECT DISTINCT 
		store_label,
		channel_type,
		category_name,
		subcategory_name
	FROM active_subs s
	UNION ALL
	SELECT DISTINCT 
		store_label,
		channel_type,
		category_name,
		subcategory_name
	FROM asv_targets
)
, cross_join_for_all AS (
	SELECT DISTINCT 
		s.fact_date,
		s.month_bom,
		s.month_eom,
		u.store_label,
		u.channel_type,
		u.category_name,
		u.subcategory_name
	FROM active_subs s 
	CROSS JOIN union_for_all_subcategories u
)
SELECT
	DISTINCT 
 	fa.fact_date,
	fa.store_label,
	fa.channel_type,
	fa.month_bom,
	fa.month_eom,
	fa.category_name,
	fa.subcategory_name,
	COALESCE(l.active_subscriptions, 0) AS active_subscriptions,
	COALESCE(l.active_subscription_value, 0) AS active_subscription_value, 
	COALESCE(a.new_subscriptions) AS acquired_subscriptions,
	COALESCE(acquired_subscription_value, 0) AS acquired_subscription_value,
	COALESCE(cancelled_subscriptions, 0) AS cancelled_subscriptions,
	COALESCE(cancelled_subscription_value, 0) AS cancelled_subscription_value, 
	----- metrics in currency for USD amount
	COALESCE(l.asv_in_currency, 0) AS active_subscription_value_in_currency,
	COALESCE(acquired_asv_in_currency, 0) AS acquired_subscription_value_in_currency, 
	COALESCE(cancelled_asv_in_currency, 0) AS cancelled_subscription_value_in_currency, 
	t.asv_target,
	t.asv_target_in_currency,
	att.acquired_asv_target, 
	att.acquired_asv_target_in_currency,  
	cat.cancelled_asv_target, 
	cat.cancelled_asv_target_in_currency,
	st.acquired_subscriptions_target,
	ct.cancelled_subscriptions_target,
	wow.sub_value_wow, 
	wow.sub_value_mom, 
	wow.sub_value_mtd,
	wow.sub_units_wow,
	wow.sub_units_mom,
	wow.sub_units_mtd,
	yoy.active_subscription_value AS sub_value_yoy, 
	yoy.asv_in_currency AS asv_sub_value_yoy_in_currency,
	yoy.active_subscriptions AS active_subscriptions_yoy,
	----- metrics in currency for USD amount
	wow.sub_value_wow_in_currency, 
	wow.sub_value_mom_in_currency, 
	wow.sub_value_mtd_in_currency
FROM cross_join_for_all fa
LEFT JOIN active_subs l
 ON l.fact_date::date = fa.fact_date::date
	AND l.store_label = fa.store_label
	AND l.channel_type = fa.channel_type
	AND l.category_name = fa.category_name
	AND l.subcategory_name = fa.subcategory_name
LEFT JOIN acquisition a 
 ON fa.fact_date::date = a.start_date::date
	AND fa.store_label = a.store_label
	AND fa.channel_type = a.channel_type
	AND fa.category_name = a.category_name
	AND fa.subcategory_name = a.subcategory_name
LEFT JOIN churn c 
 ON
	fa.fact_date::date = c.cancellation_date::date
	AND fa.store_label = c.store_label
	AND fa.channel_type = c.channel_type
	AND fa.category_name = c.category_name
	AND fa.subcategory_name = c.subcategory_name
LEFT JOIN asv_targets t
 ON
	fa.fact_date::date = t.datum::date
	AND fa.store_label = t.store_label
	AND fa.channel_type = t.channel_type
	AND fa.category_name = t.category_name
	AND fa.subcategory_name = t.subcategory_name
LEFT JOIN acquired_asv_targets att
 ON
	fa.fact_date::date = att.datum::date
	AND fa.store_label = att.store_label
	AND fa.channel_type = att.channel_type
	AND fa.category_name = att.category_name
	AND fa.subcategory_name = att.subcategory_name
LEFT JOIN cancelled_asv_targets cat
 ON
	fa.fact_date::date = cat.datum::date
	AND fa.store_label = cat.store_label
	AND fa.channel_type = cat.channel_type
	AND fa.category_name = cat.category_name
	AND fa.subcategory_name = cat.subcategory_name
LEFT JOIN acquired_subscriptions_target st
 ON 
 	fa.fact_date::date = st.datum::date
	AND fa.store_label = st.store_label
	AND fa.channel_type = st.channel_type
	AND fa.category_name = st.category_name
	AND fa.subcategory_name = st.subcategory_name
LEFT JOIN cancelled_subscriptions_target ct
 ON
 	fa.fact_date::date = ct.datum::date
	AND fa.store_label = ct.store_label
	AND fa.channel_type = ct.channel_type
	AND fa.category_name = ct.category_name
	AND fa.subcategory_name = ct.subcategory_name
LEFT JOIN active_subs_wow wow
 ON
	fa.fact_date::date = wow.fact_date::date
	AND fa.store_label = wow.store_label
	AND fa.channel_type = wow.channel_type
	AND fa.category_name = wow.category_name
	AND fa.subcategory_name = wow.subcategory_name
LEFT JOIN asv_yoy yoy 
 ON
	fa.store_label = yoy.store_label
	AND fa.channel_type = yoy.channel_type
	AND fa.category_name = yoy.category_name
	AND fa.subcategory_name = yoy.subcategory_name
WITH NO SCHEMA BINDING;