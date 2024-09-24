CREATE OR REPLACE VIEW dm_risk.v_portfolio_snapshot_report AS
WITH payments AS 
(
SELECT
	sp.subscription_id, 
	sp.currency,
	-----------------------------
	-- Overall Payment Metrics --
	-----------------------------
	---- Current rates
	SUM(COALESCE(sp.amount_due,0)) AS amount_due_current,
	COUNT(DISTINCT sp.payment_id) AS no_dues_current,
	SUM(COALESCE(sp.amount_paid,0)) AS amount_paid_current,
	SUM(CASE WHEN sp.paid_date::DATE <= sp.due_date::DATE THEN sp.amount_paid ELSE 0 END) AS amount_paid_dayofdue,
	----- Fully cured rates (will be null due/paid if, for example, for a 90dpd measurement 90 days haven't lapsed
	---------2 DPD
	COUNT(DISTINCT CASE WHEN DATE_ADD('day', 2, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_2dpd,
	SUM(CASE WHEN DATE_ADD('day', 2, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_2dpd,
	SUM(CASE WHEN DATE_ADD('day', 2, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 2, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_2dpd,
	---------10 DPD
	COUNT(DISTINCT CASE WHEN DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_10dpd,
	SUM(CASE WHEN DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_10dpd,
	SUM(CASE WHEN DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 10, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_10dpd,
	---------30 DPD
	COUNT(DISTINCT CASE WHEN DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_30dpd,
	SUM(CASE WHEN DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_30dpd,
	SUM(CASE WHEN DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 30, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_30dpd,	
	---------60 DPD
	COUNT(DISTINCT CASE WHEN DATE_ADD('day', 60, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_60dpd,
	SUM(CASE WHEN DATE_ADD('day', 60, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_60dpd,
	SUM(CASE WHEN DATE_ADD('day', 60, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 60, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_60dpd,	
	---------90 DPD
	COUNT(DISTINCT CASE WHEN DATE_ADD('day', 90, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_90dpd,
	SUM(CASE WHEN DATE_ADD('day', 90, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_90dpd,
	SUM(CASE WHEN DATE_ADD('day', 90, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 90, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_90dpd,	
	-------------------------
	-- 2nd Payment Metrics --
	-------------------------
	----- Current rates
	SUM(CASE WHEN payment_number = 2 THEN sp.amount_due ELSE 0 END) AS amount_due_2ndpay_current,
	COUNT(DISTINCT CASE WHEN payment_number = 2 THEN sp.payment_id END) AS no_dues_2ndpay_current,
	SUM(CASE WHEN payment_number = 2 THEN sp.amount_paid ELSE 0 END) AS amount_paid_2ndpay_current,
	SUM(CASE WHEN payment_number = 2 AND sp.paid_date::DATE <= sp.due_date::DATE THEN sp.amount_paid ELSE 0 END) AS amount_paid_2ndpay_dayofdue,
	----- Fully cured rates (will be null due/paid if, for example, for a 90dpd measurement 90 days haven't lapsed
	---------2 DPD
	COUNT(DISTINCT CASE WHEN payment_number = 2
		AND DATE_ADD('day', 2, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_2ndpay_2dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 2, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_2ndpay_2dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 2, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 2, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_2ndpay_2dpd,
	---------10 DPD
	COUNT(DISTINCT CASE WHEN payment_number = 2
		AND DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_2ndpay_10dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_2ndpay_10dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 10, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 10, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_2ndpay_10dpd,
	---------30 DPD
	COUNT(DISTINCT CASE WHEN payment_number = 2
		AND DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_2ndpay_30dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_2ndpay_30dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 30, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 30, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_2ndpay_30dpd,
	---------60 DPD
	COUNT(DISTINCT CASE WHEN payment_number = 2
		AND DATE_ADD('day', 60, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_2ndpay_60dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 60, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_2ndpay_60dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 60, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 60, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_2ndpay_60dpd,
	---------90 DPD
	COUNT(DISTINCT CASE WHEN payment_number = 2
		AND DATE_ADD('day', 90, due_date)::DATE <= CURRENT_DATE THEN payment_id END) AS no_dues_2ndpay_90dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 90, due_date)::DATE <= CURRENT_DATE THEN amount_due END) AS amount_due_2ndpay_90dpd,
	SUM(CASE WHEN payment_number = 2  
		AND DATE_ADD('day', 90, due_date)::DATE <= CURRENT_DATE
			AND sp.paid_date::DATE <= DATE_ADD('day', 90, sp.due_date)::DATE THEN sp.amount_paid END) AS amount_paid_2ndpay_90dpd
FROM master.subscription_payment sp
WHERE sp.currency IN ('EUR', 'USD')
	-- Not filtering by country name as this field does not seem to be fully complete in Subscription_payment.
	AND sp.status NOT IN ('PLANNED', 'HELD')
	AND payment_number::INT != 1 -- Recurrant payments ONLY.
	AND sp.subscription_id IS NOT NULL -- One NULL FOUND.
	AND due_date::DATE <= CURRENT_DATE -- SOME Future paid and unpaid payments (mostly unpaid) not classified as "planned". 
GROUP BY 1,2
-- Have run checks and subscription id is unique here (i.e. there are no subscriptions with multiple currencies). 
)
-- EUR / USD currency exchange rates for re-converting USD subscription values from EUR back into USD. 
, fx as 
(
SELECT
	dateadd('day',1,date_) as fact_month,
	date_,
	currency,
	exchange_rate_eur
FROM trans_dev.daily_exchange_rate  er
LEFT JOIN public.dim_dates dd on er.date_ = dd.datum
WHERE day_is_last_of_month
	and fact_month >= '2022-07-01'
)
, phase_mapping_clean AS 
(
SELECT 
	pm.order_id,
	pm.subscription_id, 
	pm.start_date,
--	pm.subscription_value , -- IN euros
	CASE
		WHEN pm.country_name = 'United States' THEN  -- Note that this US "back-conversion" may have a margin OF error OF 0.01 USD. Most likely negligable.
			CASE 
				WHEN fact_month >= '2022-08-01' 
					THEN ROUND((subscription_value_eur * (1 / fx.exchange_rate_eur)),2)::decimal(10,2) 
				WHEN fact_month < '2022-08-01' OR fact_month IS NULL
					THEN ROUND((subscription_value_eur * (1 / er.exchange_rate_eur)),2)::decimal(10,2)  
			END
	ELSE subscription_value_eur END AS subscription_value, -- IN EUR FOR EU, USD FOR US subs.
	ROW_NUMBER() OVER (PARTITION BY pm.subscription_id ORDER BY pm.fact_day ASC) AS row_num  -- Phase idx field IS NOT SET up properly so USING this. 
FROM ods_production.subscription_phase_mapping pm 
INNER JOIN master."order" o
	ON o.order_id = pm.order_id
	AND o.submitted_date >= DATEADD('year', -4, current_date) 
--- We want to reverse the currency conversion here. 	
LEFT JOIN fx 
	ON fx.fact_month = DATE_TRUNC('month', pm.fact_day)::DATE -- SPM already ON same granularity LEVEL AS fx, so can DO 1:1 joins here.
LEFT JOIN trans_dev.daily_exchange_rate  er 
	ON pm.start_date::date = er.date_ 
WHERE pm.subscription_value_eur IS NOT NULL -- A few strange cancelled subscriptions WITH NO subs value. 
)
, orders AS  
(
SELECT
	pm.order_id,
	-- Subscription level information
	COUNT(DISTINCT pm.subscription_id) AS total_subscriptions_per_order,
	SUM(subscription_value) AS total_subscription_value,
	MIN(pm.start_date) AS min_sub_start_date,
	-- Payment Information
	CASE -- We want complete currency information ON the ORDER LEVEL. 
		WHEN pd.currency IS NOT NULL THEN pd.currency
		WHEN o.store_country = 'Germany' THEN 'EUR'
		WHEN o.store_country = 'Netherlands' THEN 'EUR'
		WHEN o.store_country = 'Austria' THEN 'EUR'
		WHEN o.store_country = 'Spain' THEN 'EUR'
		WHEN o.store_country = 'United States' THEN 'USD'
	END AS payment_currency,
	-- Overall (recurrent payment only)
	SUM(amount_due_current) amount_due_current,
	SUM(no_dues_current) no_dues_current,
	SUM(amount_paid_current) amount_paid_current,
	SUM(amount_paid_dayofdue) amount_paid_dayofdue,
	SUM(no_dues_2dpd) no_dues_2dpd,
	SUM(amount_due_2dpd) amount_due_2dpd,
	SUM(amount_paid_2dpd) amount_paid_2dpd,
	SUM(no_dues_10dpd) no_dues_10dpd,
	SUM(amount_due_10dpd) amount_due_10dpd,
	SUM(amount_paid_10dpd) amount_paid_10dpd,
	SUM(no_dues_30dpd) no_dues_30dpd,
	SUM(amount_due_30dpd) amount_due_30dpd,
	SUM(amount_paid_30dpd) amount_paid_30dpd,
	SUM(no_dues_60dpd) no_dues_60dpd,
	SUM(amount_due_60dpd) amount_due_60dpd,
	SUM(amount_paid_60dpd) amount_paid_60dpd,	
	SUM(no_dues_90dpd) no_dues_90dpd,
	SUM(amount_due_90dpd) amount_due_90dpd,
	SUM(amount_paid_90dpd) amount_paid_90dpd,
	-- 2nd Payment Metrics
	SUM(amount_due_2ndpay_current) amount_due_2ndpay_current,
	SUM(no_dues_2ndpay_current) no_dues_2ndpay_current,
	SUM(amount_paid_2ndpay_current) amount_paid_2ndpay_current,
	SUM(amount_paid_2ndpay_dayofdue) amount_paid_2ndpay_dayofdue,
	SUM(no_dues_2ndpay_2dpd) no_dues_2ndpay_2dpd,
	SUM(amount_due_2ndpay_2dpd) amount_due_2ndpay_2dpd,
	SUM(amount_paid_2ndpay_2dpd) amount_paid_2ndpay_2dpd,
	SUM(no_dues_2ndpay_10dpd) no_dues_2ndpay_10dpd,
	SUM(amount_due_2ndpay_10dpd) amount_due_2ndpay_10dpd,
	SUM(amount_paid_2ndpay_10dpd) amount_paid_2ndpay_10dpd,
	SUM(no_dues_2ndpay_30dpd) no_dues_2ndpay_30dpd,
	SUM(amount_due_2ndpay_30dpd) amount_due_2ndpay_30dpd,
	SUM(amount_paid_2ndpay_30dpd) amount_paid_2ndpay_30dpd,
	SUM(no_dues_2ndpay_60dpd) no_dues_2ndpay_60dpd,
	SUM(amount_due_2ndpay_60dpd) amount_due_2ndpay_60dpd,
	SUM(amount_paid_2ndpay_60dpd) amount_paid_2ndpay_60dpd,	
	SUM(no_dues_2ndpay_90dpd) no_dues_2ndpay_90dpd,
	SUM(amount_due_2ndpay_90dpd) amount_due_2ndpay_90dpd,
	SUM(amount_paid_2ndpay_90dpd) amount_paid_2ndpay_90dpd
FROM phase_mapping_clean pm 
LEFT JOIN payments pd  
	ON pm.subscription_id = pd.subscription_id
INNER JOIN master."order" o
	ON o.order_id = pm.order_id 
	AND o.store_country IN ('Germany', 'Netherlands', 'Spain', 'Austria', 'United States') 
WHERE pm.row_num = 1 
	-- Note we are using row_num here instead of phase_idx = 1 as this field is broken (mainly for US subscriptions).
	-- Take FIRST phase OF subscription ONLY FOR ASV measurement. However, there will be multiple phase 1 subscriptions per ORDER, which we ARE summing here. 
	AND o.store_country || '/' || payment_currency IN ('Austria/EUR', 'Germany/EUR', 'Netherlands/EUR', 'Spain/EUR', 'United States/USD')
	-- We want to remove edge cases where payments are in the wrong currency for that store country (e.g. some US EUR payments appear to occur in US B2B)
GROUP BY 1,5
)
, order_category_raw AS
(
	SELECT 
		o.order_id,
		s.product_sku ,
		s.category_name ,
		s.price
	FROM master."order" o 
	INNER JOIN ods_production.order_item s 
	  ON o.order_id = s.order_id
	WHERE o.submitted_date  > DATEADD(YEAR, -2, current_date)
),
order_category_enhanced AS
(
	SELECT 
		order_id,
		category_name,
		COUNT(DISTINCT product_sku) AS total_products,
		sum(price) AS total_price
	FROM order_category_raw
	GROUP BY 1,2
),
main_order_category AS
(
	SELECT 
		order_id, 
		category_name, 
		ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY total_price DESC, total_products DESC, category_name) row_num
	FROM order_category_enhanced 
),
mr_data_eu AS
(
	SELECT DISTINCT order_id
	FROM stg_curated.risk_internal_eu_manual_review_result_v1 mr
	WHERE "result" IN ('APPROVED', 'DECLINED')
),
mr_data_us AS
(
	SELECT DISTINCT order_id
	FROM stg_curated.risk_internal_us_manual_review_result_v1 mr
	WHERE "result" IN ('APPROVED', 'DECLINED')
)
SELECT
	o.customer_id,
	o.order_id,
	-- Order level data
	---- Statistics
	o.submitted_date::DATE,
	o.paid_date::DATE, -- Calculate paid rate. 
	oj.order_journey_mapping_risk, -- TO determine proper ORDER status AND Approval rate. 
	cat.category_name AS main_order_category, 
	---- General
	o.customer_type,
	o.new_recurring,
	o.new_recurring_risk,
	o.store_country,
	o.store_label,
	o.voucher_code,
	CASE
     	WHEN mreu.order_id IS NOT NULL 
     		OR mrus.order_id IS NOT NULL 
     			THEN 'Manual Review'
     	ELSE 'System'
     END AS review_path,
	---- Score Values
	sd.es_experian_score_rating,
	sd.es_experian_score_value,
	sd.es_equifax_score_rating,
	sd.es_equifax_score_value,
	sd.de_schufa_score_rating,
	sd.de_schufa_score_value,
	sd.at_crif_score_rating,
	sd.at_crif_score_value,
	sd.nl_focum_score_rating,
	sd.nl_focum_score_value,
	sd.nl_experian_score_rating,
	sd.nl_experian_score_value,
	sd.us_precise_score_rating,
	sd.us_precise_score_value,
	sd.us_fico_score_rating,
	sd.us_fico_score_value,
	sd.global_seon_fraud_score,
	sd.eu_ekata_identity_risk_score,
	sd.eu_ekata_identity_network_score,
	sd.us_clarity_score_value,
	sd.us_clarity_score_rating,
	sd.us_vantage_score_value,
	sd.us_vantage_score_rating,
	os.score AS model_score,
	os.model_name, 
	-- Subscription level data
	oo.total_subscriptions_per_order,
	oo.total_subscription_value, -- sum of all phase 1 subscriptions
	oo.min_sub_start_date::DATE AS earliest_subscription_start_date,
	-- Payment level data 
	CASE -- We want complete currency information ON the ORDER LEVEL. 
		WHEN oo.payment_currency IS NOT NULL THEN oo.payment_currency
		WHEN o.store_country = 'Germany' THEN 'EUR'
		WHEN o.store_country = 'Netherlands' THEN 'EUR'
		WHEN o.store_country = 'Austria' THEN 'EUR'
		WHEN o.store_country = 'Spain' THEN 'EUR'
		WHEN o.store_country = 'United States' THEN 'USD'
	END AS currency,
	---- Overall payment statistics
	-- Overall (recurrent payment only)
	oo.amount_due_current,
	oo.no_dues_current,
	oo.amount_paid_current,
	oo.amount_paid_dayofdue,
	oo.no_dues_2dpd,
	oo.amount_due_2dpd,
	oo.amount_paid_2dpd,
	oo.no_dues_10dpd,
	oo.amount_due_10dpd,
	oo.amount_paid_10dpd,
	oo.no_dues_30dpd,
	oo.amount_due_30dpd,
	oo.amount_paid_30dpd,
	oo.no_dues_60dpd,
	oo.amount_due_60dpd,
	oo.amount_paid_60dpd,	
	oo.no_dues_90dpd,
	oo.amount_due_90dpd,
	oo.amount_paid_90dpd,
	-- 2nd Payment Metrics
	oo.amount_due_2ndpay_current,
	oo.no_dues_2ndpay_current,
	oo.amount_paid_2ndpay_current,
	oo.amount_paid_2ndpay_dayofdue,
	oo.no_dues_2ndpay_2dpd,
	oo.amount_due_2ndpay_2dpd,
	oo.amount_paid_2ndpay_2dpd,
	oo.no_dues_2ndpay_10dpd,
	oo.amount_due_2ndpay_10dpd,
	oo.amount_paid_2ndpay_10dpd,
	oo.no_dues_2ndpay_30dpd,
	oo.amount_due_2ndpay_30dpd,
	oo.amount_paid_2ndpay_30dpd,
	oo.no_dues_2ndpay_60dpd,
	oo.amount_due_2ndpay_60dpd,
	oo.amount_paid_2ndpay_60dpd,	
	oo.no_dues_2ndpay_90dpd,
	oo.amount_due_2ndpay_90dpd,
	oo.amount_paid_2ndpay_90dpd
FROM master."order" o
LEFT JOIN ods_production.order_journey oj
	ON o.order_id = oj.order_id 
LEFT JOIN orders oo
	ON oo.order_id = o.order_id
LEFT JOIN main_order_category cat
	ON cat.order_id = o.order_id 
	AND cat.row_num = 1
LEFT JOIN ods_data_sensitive.external_provider_order_score sd
    ON o.customer_id = sd.customer_id
   AND o.order_id = sd.order_id
LEFT JOIN ods_production.order_scoring os 
	ON os.order_id = o.order_id
LEFT JOIN mr_data_eu mreu
	ON o.order_id = mreu.order_id
LEFT JOIN mr_data_us mrus
	ON o.order_id = mrus.order_id
WHERE o.store_country IN ('Spain', 'Germany', 'Austria', 'Netherlands', 'United States')
	AND o.completed_orders = 1 -- Required FOR approval rate calc. 
	AND o.submitted_date >= DATEADD('year', -4, current_date)
WITH NO SCHEMA BINDING ;