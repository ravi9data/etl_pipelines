CREATE OR REPLACE VIEW dm_risk.v_churn_analysis AS
WITH a AS (
	SELECT DISTINCT
		"date",
		DATE_TRUNC('week',"date") AS week_date,
		customer_id,
		crm_label,
		signup_country AS country,
		customer_type
	FROM master.customer_historical c 
	LEFT JOIN public.dim_dates d 
		ON d.datum::DATE=c."date"::DATE
	WHERE day_name = 'Sunday'
		AND DATE_TRUNC('week',"date") > DATE_TRUNC('week',DATEADD('week',-25,CURRENT_DATE))
)
,prep AS (
	SELECT 
		a."date",
		a.week_date,
		a.customer_id,
		a.crm_label, 
		LAG(crm_label) OVER (PARTITION BY a.customer_id ORDER BY date) AS previous_month_status,
		country,
		customer_type
	FROM a
)
, crm_weekly_report_disagg AS (
	SELECT DISTINCT 
		DATE_TRUNC('week',date)::DATE AS fact_date_week,
		customer_id
	FROM prep
	WHERE 
		CASE WHEN crm_label IN ('Inactive') 
			AND previous_month_status IN ('Active') 
			THEN 1 ELSE 0 END = 1
			AND fact_date_week IS NOT NULL	
)
, agg_numbers AS (
	SELECT
		NULL AS customer_id,
		customer_type,
		NULL AS store_label,
		NULL AS store_id,
		NULL::date AS cancellation_date,
		NULL AS cancellation_reason,
		NULL AS cancellation_reason_new,
		NULL AS cancellation_reason_churn,
		NULL AS payment_method,
		NULL AS product_sku,
		NULL AS product_name,
		NULL AS category_name,
		NULL AS subcategory_name,
		NULL AS brand,
		NULL::date AS minimum_cancellation_date,
		NULL AS minimum_term_months,
		NULL::date AS created_date,
		country AS country_name,
		DATE_TRUNC('week',date)::DATE AS fact_date_week,
		'Agg_numbers' AS table_name,
		COUNT(DISTINCT customer_id)::text AS active_customers_bom
	FROM prep
	WHERE 
		CASE WHEN previous_month_status IN ('Active') 
			THEN 1 ELSE 0 END = 1
			AND "date" IS NOT NULL
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
)
, detail_cancel_customers AS (
	SELECT 
		s.customer_id::text,
		s.customer_type,
		s.store_label,
		s.store_id,
		s.cancellation_date::date,
		s.cancellation_reason,
		s.cancellation_reason_new,
		s.cancellation_reason_churn,
		s.payment_method,
		s.product_sku,
		s.product_name,
		s.category_name,
		s.subcategory_name,
		s.brand,
		s.minimum_cancellation_date::date ,
		s.minimum_term_months::text,
		s.created_date::date,
		s.country_name,
		c.fact_date_week::date,
		'detail_cancel_customers' AS table_name,
		CAST(NULL AS text) AS active_customers_bom
	FROM master.subscription s 
	INNER JOIN crm_weekly_report_disagg c
		ON s.customer_id = c.customer_id
	WHERE 
		CASE WHEN s.cancellation_date BETWEEN c.fact_date_week AND DATEADD('day',7,c.fact_date_week) THEN 'OK'
			ELSE 'NOK' END = 'OK'
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20	
)
SELECT * FROM detail_cancel_customers
UNION ALL
SELECT * FROM agg_numbers
WITH NO SCHEMA BINDING;
