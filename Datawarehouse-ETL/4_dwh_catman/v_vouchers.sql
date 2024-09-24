CREATE OR REPLACE VIEW dm_commercial.v_vouchers AS 
WITH top_20 AS (
    SELECT
        CASE WHEN voucher_code ilike 'MILITARY%' THEN 'MILITARY'
			WHEN voucher_code ilike 'UNI-%' THEN 'Unidays'
			WHEN voucher_code ilike 'UNIDE%' THEN 'Unidays'
			WHEN voucher_code ilike 'UNIES%' THEN 'Unidays'
			WHEN voucher_code ilike 'COBE-%' THEN 'Corporate Benefits'
			WHEN voucher_code ilike 'COBEDE%' THEN 'Corporate Benefits'
			WHEN voucher_code ilike 'COBEAT%' THEN 'Corporate Benefits'
			WHEN voucher_code ilike 'WOW-%' THEN 'WOW'
			WHEN voucher_code ilike 'SB%' THEN 'Studentbeans'
			WHEN voucher_code ilike 'STUDENTS%' THEN 'Student US'
			WHEN voucher_code ilike 'N26%' THEN 'N26'
			WHEN voucher_code ilike 'RF%' THEN 'RF'
			WHEN voucher_code ilike 'DP%' THEN 'DP'
			WHEN voucher_code ilike 'SWAP-%' THEN 'SWAP'
			WHEN voucher_code ilike 'STUDENT22-%' THEN 'STUDENT22'
			WHEN voucher_code ilike 'SNDE%' THEN 'SNDE'
			WHEN voucher_code ilike 'MSHXGR-%' THEN 'MSH'
			WHEN voucher_code ilike 'GOENNXGROVER%' THEN 'Goenn'
			WHEN voucher_code ilike 'internal%' THEN 'internal'
			ELSE voucher_code
			END AS voucher_code_group,
        COUNT(DISTINCT CASE WHEN new_recurring = 'NEW' THEN order_id END) AS total_paid_orders_new,
        COUNT(DISTINCT CASE WHEN new_recurring = 'RECURRING' THEN order_id END) AS total_paid_orders_recurring,
        ROW_NUMBER() OVER ( ORDER BY total_paid_orders_new DESC) AS row_num_new,
        ROW_NUMBER() OVER ( ORDER BY total_paid_orders_recurring DESC) AS row_num_recurring
    FROM master."order" o
    WHERE DATE_TRUNC('week',o.submitted_date::date) = DATEADD('week',-1,DATE_TRUNC('week',current_date))
		AND o.paid_orders > 0
		AND o.voucher_code IS NOT NULL
    GROUP BY 1   
)
SELECT
	o.submitted_date::date,
	o.store_label,
	o.store_country AS country,
	o.store_commercial, 
	o.status,
	o.is_in_salesforce,
	o.voucher_code,
	CASE WHEN o.voucher_code ilike 'MILITARY%' THEN 'MILITARY'
		WHEN o.voucher_code ilike 'UNI-%' THEN 'Unidays'
		WHEN o.voucher_code ilike 'UNIDE%' THEN 'Unidays'
		WHEN o.voucher_code ilike 'UNIES%' THEN 'Unidays'
		WHEN o.voucher_code ilike 'COBE-%' THEN 'Corporate Benefits'
		WHEN o.voucher_code ilike 'COBEDE%' THEN 'Corporate Benefits'
		WHEN o.voucher_code ilike 'COBEAT%' THEN 'Corporate Benefits'
		WHEN o.voucher_code ilike 'WOW-%' THEN 'WOW'
		WHEN o.voucher_code ilike 'SB%' THEN 'Studentbeans'
		WHEN o.voucher_code ilike 'STUDENTS%' THEN 'Student US'
		WHEN o.voucher_code ilike 'N26%' THEN 'N26'
		WHEN o.voucher_code ilike 'RF%' THEN 'RF'
		WHEN o.voucher_code ilike 'DP%' THEN 'DP'
		WHEN o.voucher_code ilike 'SWAP-%' THEN 'SWAP'
		WHEN o.voucher_code ilike 'STUDENT22-%' THEN 'STUDENT22'
		WHEN o.voucher_code ilike 'SNDE%' THEN 'SNDE'
		WHEN o.voucher_code ilike 'MSHXGR-%' THEN 'MSH'
		WHEN o.voucher_code ilike 'GOENNXGROVER%' THEN 'Goenn'
		WHEN o.voucher_code ilike 'internal%' THEN 'internal'
		WHEN o.voucher_code IS NULL  THEN 'No Voucher'
		ELSE o.voucher_code END AS voucher_code_group,
	CASE WHEN o.voucher_code ilike 'MILITARY%' THEN 'MILITARY'
		WHEN o.voucher_code ilike 'UNI-%' THEN 'Unidays'
		WHEN o.voucher_code ilike 'UNIDE%' THEN 'Unidays'
		WHEN o.voucher_code ilike 'UNIES%' THEN 'Unidays'
		WHEN o.voucher_code ilike 'COBE-%' THEN 'Corporate Benefits'
		WHEN o.voucher_code ilike 'COBEDE%' THEN 'Corporate Benefits'
		WHEN o.voucher_code ilike 'COBEAT%' THEN 'Corporate Benefits'
		WHEN o.voucher_code ilike 'WOW-%' THEN 'WOW'
		WHEN o.voucher_code ilike 'SB%' THEN 'Studentbeans'
		WHEN o.voucher_code ilike 'STUDENTS%' THEN 'Student US'
		WHEN o.voucher_code ilike 'N26%' THEN 'N26'
		WHEN o.voucher_code ilike 'RF%' THEN 'RF'
		WHEN o.voucher_code ilike 'DP%' THEN 'DP'
		WHEN o.voucher_code ilike 'SWAP-%' THEN 'SWAP'
		WHEN o.voucher_code ilike 'STUDENT22-%' THEN 'STUDENT22'
		WHEN o.voucher_code ilike 'SNDE%' THEN 'SNDE'
		WHEN o.voucher_code ilike 'MSHXGR-%' THEN 'MSH'
		WHEN o.voucher_code ilike 'GOENNXGROVER%' THEN 'Goenn'
		WHEN o.voucher_code ilike 'internal%' THEN 'internal'
		WHEN o.voucher_code IS NULL  THEN 'No Voucher'
		ELSE 'Others' END AS voucher_code_group2,
	o2.is_special_voucher,
	o.marketing_channel,
	o.completed_orders,
	o.canceled_date::date, 
	o.cancelled_orders,
	o.cancellation_reason, 
	o2.first_charge_date::date,
	o.paid_date::date, 
	o.paid_orders,
	o.new_recurring,
	o2.ordered_product_category AS categories,
	CASE WHEN t20.row_num_new <= 20 THEN t20.row_num_new END row_num_new,
	CASE WHEN t20.row_num_recurring <= 20 THEN t20.row_num_recurring END row_num_recurring,
	sum(o.voucher_discount) AS voucher_discount,--LAST aggregation built
	sum(o.basket_size) AS basket_size,
	sum(o.avg_plan_duration) AS avg_plan_duration,
	count(DISTINCT o.order_id) AS orders,
	count(DISTINCT o.customer_id) AS customers,
	COALESCE(SUM(o.voucher_discount * COALESCE(exc.exchange_rate_eur, exc_last.exchange_rate_eur, 1)),0) AS total_spent_eur
FROM master."order" o
LEFT JOIN ods_production.order o2 
	ON o2.order_id = o.order_id
LEFT JOIN trans_dev.daily_exchange_rate exc
	ON o.paid_date::date = exc.date_
    	AND o.store_label = 'Grover - United States online'
LEFT JOIN trans_dev.v_latest_daily_exchange_rate exc_last
    ON o.store_label = 'Grover - United States online'	
LEFT JOIN top_20 t20
	ON t20.voucher_code_group = CASE WHEN o.voucher_code ilike 'MILITARY%' THEN 'MILITARY'
									WHEN o.voucher_code ilike 'UNI-%' THEN 'Unidays'
									WHEN o.voucher_code ilike 'UNIDE%' THEN 'Unidays'
									WHEN o.voucher_code ilike 'UNIES%' THEN 'Unidays'
									WHEN o.voucher_code ilike 'COBE-%' THEN 'Corporate Benefits'
									WHEN o.voucher_code ilike 'COBEDE%' THEN 'Corporate Benefits'
									WHEN o.voucher_code ilike 'COBEAT%' THEN 'Corporate Benefits'
									WHEN o.voucher_code ilike 'WOW-%' THEN 'WOW'
									WHEN o.voucher_code ilike 'SB%' THEN 'Studentbeans'
									WHEN o.voucher_code ilike 'STUDENTS%' THEN 'Student US'
									WHEN o.voucher_code ilike 'N26%' THEN 'N26'
									WHEN o.voucher_code ilike 'RF%' THEN 'RF'
									WHEN o.voucher_code ilike 'DP%' THEN 'DP'
									WHEN o.voucher_code ilike 'SWAP-%' THEN 'SWAP'
									WHEN o.voucher_code ilike 'STUDENT22-%' THEN 'STUDENT22'
									WHEN o.voucher_code ilike 'SNDE%' THEN 'SNDE'
									WHEN o.voucher_code ilike 'MSHXGR-%' THEN 'MSH'
									WHEN o.voucher_code ilike 'GOENNXGROVER%' THEN 'Goenn'
									WHEN o.voucher_code ilike 'internal%' THEN 'internal'
									WHEN o.voucher_code IS NULL  THEN 'No Voucher'
									ELSE o.voucher_code END
	AND (t20.row_num_new <= 20 OR t20.row_num_recurring <= 20)
WHERE (o.submitted_date::date >= '2023-01-01'
	OR o.paid_date::date >= '2023-01-01')
	AND o.submitted_date < current_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
WITH NO SCHEMA BINDING
;
