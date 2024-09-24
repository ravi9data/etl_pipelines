DROP TABLE IF EXISTS budget_dates;
CREATE TEMP TABLE budget_dates
SORTKEY(
	fact_date
)
DISTKEY(
	fact_date
)
AS 
	SELECT DISTINCT  
		datum as fact_date, 
	 	date_trunc('month',datum)::DATE AS month_bom,
		LEAST(DATE_TRUNC('month',DATEADD('month',1,datum))::DATE-1,CURRENT_DATE) AS month_eom
	FROM public.dim_dates
	WHERE datum <= current_date
	AND datum >= '2022-01-01'
	ORDER BY 1 DESC 
;
	
DROP TABLE IF EXISTS budget_cross_table;
CREATE TEMP TABLE budget_cross_table
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
	SELECT DISTINCT  
	 	d.month_bom,
		s.store_country,
		c.customer_type
	FROM (SELECT DISTINCT month_bom FROM budget_dates) d
	CROSS JOIN (
		SELECT DISTINCT 
			store_country 
		FROM master.ORDER 
		WHERE store_country IS NOT NULL AND store_country NOT IN ('Andorra', 'United Kingdom')
		UNION
		SELECT 
			'No Info' AS store_country
		) s
	CROSS JOIN (
		SELECT DISTINCT 
			customer_type 
		FROM master.ORDER
		WHERE customer_type IS NOT null
		UNION
		SELECT 
			'No Info' AS customer_type
		) c
;

DROP TABLE IF EXISTS countries_customers;
CREATE TEMP TABLE countries_customers
SORTKEY(
	customer_id
)
DISTKEY(
	customer_id
)
AS
WITH countries_customers_sub AS (
	SELECT 
		customer_id,
		store_country,
		ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_date) AS row_num
	FROM master.ORDER
	WHERE store_country IS NOT NULL 
	AND store_country NOT IN ('Andorra','United Kingdom')
)
SELECT 
	customer_id,
	store_country
FROM countries_customers_sub
WHERE row_num = 1
;


DROP TABLE IF EXISTS budget_active_subs;
CREATE TEMP TABLE budget_active_subs
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
	SELECT 
		d.fact_date,
		d.month_bom,
		CASE WHEN d.month_bom = d.fact_date THEN 'bom' 
			 WHEN d.month_eom = d.fact_date THEN 'eom' 
			 END AS month_date,
		COALESCE(s.country_name,'No Info') AS store_country,
		COALESCE(s.customer_type,'No Info') AS customer_type,
		COUNT(DISTINCT s.subscription_id) AS active_subscriptions,
		SUM(s.subscription_value_eur) AS active_subscription_value
	FROM budget_dates d
	LEFT JOIN ods_production.subscription_phase_mapping s
		ON d.fact_date::DATE >= s.fact_day::DATE 
		AND  d.fact_date::DATE <= coalesce(s.end_date::DATE, d.fact_date::DATE+1)
 	WHERE s.store_label NOT ilike '%old%'
 		AND (d.fact_date::DATE = d.month_eom
 			OR d.fact_date::DATE = d.month_bom) -- TO calculate churn subscription rate, so we need the active_subscriptions IN the bom
	GROUP BY 1,2,3,4,5
	ORDER BY 3,4,1
;



DROP TABLE IF EXISTS budget_active_customers;
CREATE TEMP TABLE budget_active_customers
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
	SELECT 
		d.month_bom,
		COALESCE(cc.store_country,'No Info') AS store_country,
		COALESCE(s.customer_type,'No Info') AS customer_type,
	 	COUNT(DISTINCT s.customer_id) AS active_customers 
	FROM budget_dates d
	LEFT JOIN ods_production.subscription_phase_mapping s
		ON d.fact_date::DATE >= s.fact_day::DATE 
		AND  d.fact_date::DATE <= coalesce(s.end_date::DATE, d.fact_date::DATE+1)
	LEFT JOIN countries_customers cc 
		ON s.customer_id = cc.customer_id
 	WHERE s.store_label NOT ilike '%old%'
 		AND d.fact_date::DATE = d.month_eom
	GROUP BY 1,2,3
	ORDER BY 1 DESC
;
	
	
DROP TABLE IF EXISTS budget_customers;
CREATE TEMP TABLE budget_customers
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS	
-- dm_b2b.v_business_customers_kpis_tableau
	SELECT DISTINCT 
		 d.month_bom,
		 COALESCE(o.store_country,'No Info') AS store_country,
		 COALESCE(o.customer_type,'No Info') AS customer_type, 
		 COALESCE(COUNT(DISTINCT CASE WHEN d.fact_date = cac.customer_acquisition_cohort THEN o.customer_id END),0) 
		 	AS new_customers 
	FROM budget_dates d
	  LEFT JOIN 
		   (SELECT 
				 d.fact_date AS fact_date,
				 o.customer_id, 
				 cc.store_country,
				 o.customer_type ,
				 COALESCE(COUNT(CASE WHEN paid_date IS NOT NULL THEN order_id  END),0) AS paid_orders
			  FROM budget_dates d
			    LEFT JOIN master.order o
			      ON d.fact_date >= o.created_date::DATE 
			    LEFT JOIN countries_customers cc 
				  ON o.customer_id = cc.customer_id
			  GROUP BY 1, 2, 3, 4--, 5
			  ) o 
		ON d.fact_date = o.fact_date
	  LEFT JOIN ods_production.customer_acquisition_cohort cac 
		   ON o.customer_id = cac.customer_id
	  GROUP BY 1,2,3
;	
	

DROP TABLE IF EXISTS budget_widerruf_and_failed_delivery_subs;
CREATE TEMP TABLE budget_widerruf_and_failed_delivery_subs
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS	
	SELECT 
		DATE_TRUNC('month',cancellation_date)::date AS month_bom,
		COALESCE(s.customer_type,'No Info') AS customer_type,
		COALESCE(s.country_name,'No Info') AS store_country,
		count(DISTINCT CASE WHEN cancellation_reason_churn = 'failed delivery' 
			THEN subscription_id END) AS cancelled_subs_failed_delivery,
		count(DISTINCT CASE WHEN cancellation_reason ILIKE '%widerruf%' 
				OR cancellation_reason ILIKE '%wideruf%' 
				OR cancellation_reason ILIKE '%wiederruf%' 
				OR cancellation_reason ILIKE '%wirderruf%' 
				OR cancellation_reason ILIKE '%revocation%'
				OR cancellation_reason ILIKE '%revokation%'
				OR cancellation_reason ILIKE '%revoked%'
				OR cancellation_reason ILIKE '%revocated%'
				THEN subscription_id END) AS cancelled_subs_widerruf,
		count(DISTINCT CASE WHEN cancellation_reason_new = '7 DAY TRIAL'
				OR cancellation_reason_new = 'CANCELLED BEFORE ALLOCATION - CUSTOMER REQUEST'
				OR cancellation_reason_new = 'CANCELLED BEFORE ALLOCATION - OTHERS'
				OR cancellation_reason_new = 'CANCELLED BEFORE ALLOCATION - PROCUREMENT'
				OR cancellation_reason_new = 'CANCELLED BEFORE SHIPMENT'
				OR cancellation_reason_new = 'FAILED DELIVERY'
				OR cancellation_reason_new = 'LOST DURING OUTBOUND'
				OR cancellation_reason_new = 'RETURNED MULTIPLE ALLOCATIONS'
				OR cancellation_reason_new = 'REVOCATION'
				THEN subscription_id END) AS cancelled_subs_fulfillment_errors
				-- definition by Vaidehi
	FROM master.subscription s
	WHERE cancellation_date::date >= '2022-01-01'
	GROUP BY 1,2,3
;	


DROP TABLE IF EXISTS budget_new_subscriptions;
CREATE TEMP TABLE budget_new_subscriptions
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
	SELECT 
		DATE_TRUNC('month',start_date)::date AS month_bom,
		COALESCE(s.customer_type,'No Info') AS customer_type,
		COALESCE(s.country_name,'No Info') AS store_country,
		count(DISTINCT subscription_id) AS new_subscriptions,
		count(DISTINCT CASE WHEN new_recurring = 'NEW' 
			THEN subscription_id END) AS subscriptions_new_customers,
		count(DISTINCT CASE WHEN new_recurring = 'RECURRING' 
			THEN subscription_id END) AS subscriptions_recurring_customers
	FROM master.subscription s
	WHERE start_date::date >= '2022-01-01'
	GROUP BY 1,2,3
;



DROP TABLE IF EXISTS budget_new_subscriptions_customers;
CREATE TEMP TABLE budget_new_subscriptions_customers
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
	SELECT 
		DATE_TRUNC('month',s.start_date)::date AS month_bom,
		COALESCE(s.customer_type,'No Info') AS customer_type,
		COALESCE(cc.store_country,'No Info') AS store_country,
		count(DISTINCT s.customer_id) AS customers_with_subscriptions, 
		count(DISTINCT CASE WHEN s.new_recurring = 'NEW' 
			THEN s.customer_id END) AS customers_with_subscriptions_new_customers, 
		count(DISTINCT CASE WHEN s.new_recurring = 'RECURRING' 
			THEN s.customer_id END) AS customers_with_subscriptions_recurring_customers 
	FROM master.subscription s
	LEFT JOIN countries_customers cc 
		ON s.customer_id = cc.customer_id
	WHERE start_date::date >= '2022-01-01'
	GROUP BY 1,2,3
;



DROP TABLE IF EXISTS budget_refurbishment;
CREATE TEMP TABLE budget_refurbishment
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
	SELECT 
		DATE_TRUNC('month',a.refurbishment_start_at)::date AS month_bom,
		COALESCE(s.customer_type,'No Info') AS customer_type,
		COALESCE(s.country_name,'No Info') AS store_country,
		SUM(DATEDIFF('hour',a.refurbishment_start_at::timestamp,a.refurbishment_end_at::timestamp)::float/24.00) AS total_days_in_refurbishment,
		count(DISTINCT CASE WHEN a.refurbishment_start_at IS NOT NULL THEN a.allocation_id END) AS total_assets_in_refurbishment
	FROM ods_production.allocation a
	LEFT JOIN master.subscription s 
		ON a.subscription_id = s.subscription_id 
	WHERE month_bom >= '2022-01-01'
	GROUP BY 1,2,3
;
	

DROP TABLE IF EXISTS budget_returned_assets;
CREATE TEMP TABLE budget_returned_assets
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
	SELECT
		DATE_TRUNC('month', coalesce (return_delivery_date, return_delivery_date_old)) AS month_bom,
		COALESCE(s.customer_type,'No Info') AS customer_type,
		COALESCE(s.country_name,'No Info') AS store_country,
		count(1) AS return_assets
	FROM ods_production.allocation  a 
	LEFT JOIN master.subscription s 
		ON a.subscription_id = s.subscription_id
	WHERE month_bom >= '2022-01-01'
	GROUP BY 1,2,3
;	
	

DROP TABLE IF EXISTS budget_purchase_prices;
CREATE TEMP TABLE budget_purchase_prices
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
SELECT 
	DATE_TRUNC('month',a.purchased_date)::date AS month_bom,
	COALESCE(s.customer_type,'No Info') AS customer_type,
	COALESCE(s.country_name,'No Info') AS store_country,
	count(DISTINCT a.asset_id) AS total_purchase_assets,
	SUM(a.initial_price) AS total_purchase_price,
	SUM(a.purchase_price_commercial) AS total_purchase_price_commercial
FROM master.asset a
LEFT JOIN ods_production.allocation al
	ON a.asset_allocation_id = al.allocation_id
LEFT JOIN master.subscription s 
	ON al.subscription_id = s.subscription_id 
WHERE a.purchased_date::date >= '2022-01-01'
GROUP BY 1,2,3
;


--


DROP TABLE IF EXISTS budget_payments;
CREATE TEMP TABLE budget_payments
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
	SELECT 
		DATE_TRUNC('month',sp.paid_date)::date AS month_bom,
		COALESCE(sp.country_name,'No Info') AS store_country,
		COALESCE(sp.customer_type,'No Info') AS customer_type,
		round(AVG(sp.amount_subscription),2) AS avg_subscription_price, -- value OF subscription BEFORE applying the voucher -- higher value
		round(AVG(sp.amount_paid),2) AS avg_amount_paid, -- value OF subscription AFTER applying the voucher -- lower value
		round(AVG(sp.amount_voucher),2) AS avg_amount_voucher, -- value OF the voucher
		round(AVG(CASE WHEN payment_type = 'FIRST' THEN sp.amount_voucher END),2) AS avg_amount_voucher_first_payment,
		round(AVG(CASE WHEN payment_type = 'RECURRENT' THEN sp.amount_voucher END),2) AS avg_amount_voucher_recurrent_payment,
		SUM(sp.amount_tax)::float AS sum_amount_tax,
		SUM(sp.amount_paid) AS sum_amount_paid
	FROM master.subscription_payment sp
	WHERE month_bom >= '2022-01-01'
	GROUP BY 1,2,3
;


DROP TABLE IF EXISTS budget_subs_value_price;
CREATE TEMP TABLE budget_subs_value_price
SORTKEY(
	month_bom,
	store_country,
	customer_type
)
DISTKEY(
	month_bom
)
AS
WITH prep_subs_value AS (
	SELECT 
		start_date,
		customer_type,
		customer_id,
		subscription_value_eur AS subscription_value,
		subscription_value_lc AS actual_subscription_value,
		subscription_id,
		ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY fact_day) AS row_num
	FROM ods_production.subscription_phase_mapping
	WHERE start_date::date >= '2022-01-01'
)
SELECT 
	DATE_TRUNC('month',s.start_date)::date AS month_bom,
	COALESCE(s.customer_type,'No Info') AS customer_type,
	COALESCE(cc.country_name,'No Info') AS store_country, 
	sum(s.subscription_value) AS sum_acquired_subscription_value_euro,
	sum(s.actual_subscription_value) AS sum_acquired_subscription_value_actual
FROM prep_subs_value s
LEFT JOIN master.subscription cc 
		ON s.subscription_id = cc.subscription_id 
WHERE row_num = 1 
GROUP BY 1,2,3
;


DROP TABLE IF EXISTS dm_finance.budget_metrics;
CREATE TABLE dm_finance.budget_metrics AS
WITH metrics AS (	
SELECT DISTINCT 
	c.store_country,
	c.month_bom,
	c.customer_type,
	--------- customers
	SUM(m.new_customers) AS new_customers,
	SUM(ba.active_customers) AS active_customers,
	SUM(gg.gross_churn_customers) AS gross_churn_customers_including_1_month_rental_plan,
	round(SUM(gg.gross_churn_customers)::float/SUM(gg.active_customers_bom)::float,4) AS rate_gross_churn_customers_including_1_month_rental_plan,
	----------- subscriptions 
	SUM(n.new_subscriptions) AS new_subscriptions,
	SUM(n.subscriptions_new_customers) AS subscriptions_new_customers,
	SUM(n.subscriptions_recurring_customers) AS subscriptions_recurring_customers,
	SUM(l.active_subscriptions) AS active_subscriptions,
	CASE WHEN COALESCE(SUM(nc.customers_with_subscriptions),0) = 0 THEN 0
		ELSE round(SUM(n.new_subscriptions)::float/SUM(nc.customers_with_subscriptions)::float,2) 
		END AS subscriptions_per_customer,
	CASE WHEN COALESCE(SUM(nc.customers_with_subscriptions_new_customers),0) = 0 THEN 0
		ELSE round(SUM(n.subscriptions_new_customers)::float/SUM(nc.customers_with_subscriptions_new_customers)::float,2) 
		END AS subscriptions_per_new_customer,
	CASE WHEN COALESCE(SUM(nc.customers_with_subscriptions_recurring_customers),0) = 0 THEN 0
		ELSE round(SUM(n.subscriptions_recurring_customers)::float/SUM(nc.customers_with_subscriptions_recurring_customers)::float,2) 
		END AS subscriptions_per_recurring_customer,
	SUM(sci.cancelled_subscriptions) AS gross_churn_subscriptions_including_1_month_rental_plan,
	round(SUM(sci.cancelled_subscriptions)::float/SUM(lee.active_subscriptions)::float,4) AS rate_gross_churn_subscriptions_including_1_month_rental_plan,
	SUM(w.cancelled_subs_failed_delivery) AS cancelled_subs_failed_delivery,
	SUM(w.cancelled_subs_widerruf) AS cancelled_subs_widerruf,
	SUM(w.cancelled_subs_fulfillment_errors) AS cancelled_subs_fulfillment_errors,
	----------- revenue
	SUM(l.active_subscription_value) AS active_subscription_value, -- only in EUR
	AVG(bp.avg_subscription_price) AS avg_subscription_price, -- EUR AND USD
	SUM(svp.sum_acquired_subscription_value_euro) AS sum_acquired_subscription_value_euro,
	sum(svp.sum_acquired_subscription_value_actual) AS sum_acquired_subscription_value_actual,
	AVG(bp.avg_amount_paid) AS avg_amount_paid, -- EUR AND USD
	AVG(bp.avg_amount_voucher) AS avg_amount_voucher, -- EUR AND USD
	AVG(bp.avg_amount_voucher_first_payment) AS avg_amount_voucher_first_payment, -- EUR AND USD
	AVG(bp.avg_amount_voucher_recurrent_payment) AS avg_amount_voucher_recurrent_payment, -- EUR AND USD
	CASE WHEN COALESCE(SUM(sum_amount_paid),0) = 0 THEN 0
		ELSE round(SUM(sum_amount_tax)/SUM(sum_amount_paid),4) 
		END AS percentage_tax_in_amount_paid,
	----------- assets
	SUM(ra.return_assets) AS return_assets,
	round(SUM(r.total_days_in_refurbishment)::float/SUM(r.total_assets_in_refurbishment)::float,2) AS avg_refurbishment_in_days,
	SUM(pp.total_purchase_price) AS total_purchase_price, -- EUR AND USD
	SUM(pp.total_purchase_price_commercial) AS total_purchase_price_commercial, -- EUR AND USD
	SUM(pp.total_purchase_assets) AS total_purchase_assets
FROM budget_cross_table c 
LEFT JOIN budget_active_subs l
	ON c.store_country = l.store_country
	AND c.month_bom = l.month_bom
	AND c.customer_type = l.customer_type
	AND l.month_date = 'eom'
LEFT JOIN budget_active_customers ba
	ON c.store_country = ba.store_country
	AND c.month_bom = ba.month_bom
	AND c.customer_type = ba.customer_type
LEFT JOIN budget_customers m 
	ON c.store_country = m.store_country
	AND c.month_bom = m.month_bom
	AND c.customer_type = m.customer_type
LEFT JOIN budget_widerruf_and_failed_delivery_subs w 
	ON c.store_country = w.store_country
	AND c.month_bom = w.month_bom
	AND c.customer_type = w.customer_type
LEFT JOIN budget_new_subscriptions n
	ON c.store_country = n.store_country
	AND c.month_bom = n.month_bom
	AND c.customer_type = n.customer_type
LEFT JOIN budget_new_subscriptions_customers nc
	ON c.store_country = nc.store_country
	AND c.month_bom = nc.month_bom
	AND c.customer_type = nc.customer_type
LEFT JOIN budget_refurbishment r
	ON c.store_country = r.store_country
	AND c.month_bom = r.month_bom
	AND c.customer_type = r.customer_type
LEFT JOIN budget_returned_assets ra
	ON c.store_country = ra.store_country
	AND c.month_bom = ra.month_bom
	AND c.customer_type = ra.customer_type
LEFT JOIN budget_purchase_prices pp
	ON c.store_country = pp.store_country
	AND c.month_bom = pp.month_bom
	AND c.customer_type = pp.customer_type
LEFT JOIN budget_payments bp
	ON c.store_country = bp.store_country
	AND c.month_bom = bp.month_bom
	AND c.customer_type = bp.customer_type
LEFT JOIN dm_finance.v_customer_churn_report_by_countries gg -- gross churn customer INCLUDING 1 MONTH plan
	ON c.store_country = gg.country_name
	AND c.month_bom = gg.bom
	AND c.customer_type = gg.customer_type
	AND gg.one_month_rental = 'INCLUDE'
LEFT JOIN dm_finance.v_customer_churn_report_by_countries ge -- gross churn customer excluding 1 MONTH plan
	ON c.store_country = ge.country_name
	AND c.month_bom = ge.bom
	AND c.customer_type = ge.customer_type
	AND ge.one_month_rental = 'EXCLUDE'
LEFT JOIN dm_finance.v_customer_churn_report_sub_churn  sci -- gross churn customer INCLUDING 1 MONTH plan
	ON c.store_country = sci.country_name
	AND c.month_bom = sci.month_bom
	AND c.customer_type = sci.customer_type
	AND sci.one_month_rental_exclude IS FALSE 
LEFT JOIN dm_finance.v_customer_churn_report_sub_churn  sce -- gross churn customer excluding 1 MONTH plan
	ON c.store_country = sce.country_name
	AND c.month_bom = sce.month_bom
	AND c.customer_type = sce.customer_type
	AND sce.one_month_rental_exclude IS TRUE  
LEFT JOIN budget_active_subs lee
	ON c.store_country = lee.store_country
	AND c.month_bom = lee.month_bom
	AND c.customer_type = lee.customer_type
	AND lee.month_date = 'bom'
LEFT JOIN budget_subs_value_price svp 
	ON c.store_country = svp.store_country
	AND c.month_bom = svp.month_bom
	AND c.customer_type = svp.customer_type
GROUP BY 1,2,3
)
SELECT 
	*
FROM metrics  
WHERE -- TO clean the TABLE, WHERE the ROWS does NOT SHOW ANY value
	--------- customers
	COALESCE(new_customers,0) <> 0 OR
	COALESCE(active_customers,0) <> 0 OR
	COALESCE(gross_churn_customers_including_1_month_rental_plan,0) <> 0 OR
	COALESCE(rate_gross_churn_customers_including_1_month_rental_plan,0) <> 0 OR 
	----------- subscriptions 
	COALESCE(new_subscriptions,0) <> 0 OR
	COALESCE(subscriptions_new_customers,0) <> 0 OR
	COALESCE(subscriptions_recurring_customers,0) <> 0 OR
	COALESCE(active_subscriptions,0) <> 0 OR
	COALESCE(subscriptions_per_customer,0) <> 0 OR
	COALESCE(subscriptions_per_new_customer,0) <> 0 OR
	COALESCE(subscriptions_per_recurring_customer,0) <> 0 OR
	COALESCE(gross_churn_subscriptions_including_1_month_rental_plan,0) <> 0 OR
	COALESCE(rate_gross_churn_subscriptions_including_1_month_rental_plan,0) <> 0 OR
	COALESCE(cancelled_subs_failed_delivery,0) <> 0 OR
	COALESCE(cancelled_subs_widerruf,0) <> 0 OR
	COALESCE(cancelled_subs_fulfillment_errors,0) <> 0 OR
	----------- revenue
	COALESCE(active_subscription_value,0) <> 0 OR -- only in EUR
	COALESCE(avg_subscription_price,0) <> 0 OR -- EUR AND USD
	COALESCE(sum_acquired_subscription_value_euro,0) <> 0 OR 
	COALESCE(sum_acquired_subscription_value_actual,0) <> 0 OR 
	COALESCE(avg_amount_paid,0) <> 0 OR -- EUR AND USD
	COALESCE(avg_amount_voucher,0) <> 0 OR -- EUR AND USD
	COALESCE(avg_amount_voucher_first_payment,0) <> 0 OR -- EUR AND USD
	COALESCE(avg_amount_voucher_recurrent_payment,0) <> 0 OR -- EUR AND USD
	COALESCE(percentage_tax_in_amount_paid,0) <> 0 OR
	----------- assets
	COALESCE(return_assets,0) <> 0 OR
	COALESCE(avg_refurbishment_in_days,0) <> 0 OR
	COALESCE(total_purchase_price,0) <> 0 OR -- EUR AND USD
	COALESCE(total_purchase_price_commercial,0) <> 0 OR -- EUR AND USD
	COALESCE(total_purchase_assets,0) <> 0
ORDER BY 1,2 DESC,3;

GRANT SELECT ON dm_finance.budget_metrics TO tableau;
