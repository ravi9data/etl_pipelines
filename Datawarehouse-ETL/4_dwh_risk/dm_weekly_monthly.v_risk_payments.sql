CREATE OR REPLACE VIEW dm_weekly_monthly.v_risk_payments AS 
SELECT 
	'weekly'::varchar AS report,
	DATE_TRUNC('week',CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
    		THEN DATEADD('day', 14, sp.due_date)
		ELSE sp.due_date
		END)::date AS effective_due_date,
	sp.payment_type,
	sp.payment_method,
	sp.category_name,
	s.retention_group,
	sp.burgel_risk_category,
	CASE WHEN sp.payment_number::int BETWEEN 1 AND 5 THEN sp.payment_number
		 WHEN sp.payment_number::int BETWEEN 6 AND 12 THEN '6-12'
		 WHEN sp.payment_number::int > 12 THEN '12+'
		 END AS payment_number_group,
	CASE WHEN sp.subscription_plan IN ('1 Months', 'Pay As You Go') THEN '1'
		 WHEN sp.subscription_plan IN ('3 Months', '6 Months', '12 Months', '18 Months', '24 Months') 
		 	THEN split_part(sp.subscription_plan, ' ',1)
		 ELSE 'Other' END AS subscription_plan_group,
	CASE WHEN sp.customer_type = 'business_customer' THEN 'B2B' 
		 WHEN sp.store_short ='Grover International' then sp.store_label
		 ELSE sp.store_short END AS store_new,
	CASE sp.customer_type WHEN 'business_customer' THEN 'B2B' ELSE 'B2C' END AS customer_type_cleaned,
	CASE WHEN (c.active_subscriptions = 1 OR c.active_subscriptions = 0) THEN '1'
		 WHEN c.active_subscriptions = 2 THEN '2'
		 WHEN (c.active_subscriptions = 3 or c.active_subscriptions = 4) THEN '3-4'
		 WHEN c.active_subscriptions >= 5 THEN '5+'
		 ELSE 'others'	END AS active_subs_group,
	SUM(sp.amount_due) AS earned_revenue,
	SUM(CASE WHEN DATEDIFF('day', 
				(CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
		    				THEN DATEADD('day', 14, sp.due_date)
					  ELSE sp.due_date END) --effective_due_date
			, sp.paid_date)  <= 14 THEN sp.amount_paid END) AS payment_within_14_days, 
	SUM(CASE WHEN DATEDIFF('day', 
				(CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
		    				THEN DATEADD('day', 14, sp.due_date)
					  ELSE sp.due_date END) --effective_due_date
				, sp.paid_date)  <= 30 
			AND date_trunc('week', effective_due_date) < dateadd('week', -4, date_trunc('week',current_date))
				THEN sp.amount_paid END) AS payment_within_30_days		
FROM master.subscription_payment sp
LEFT JOIN master.subscription s 
	ON sp.subscription_id = s.subscription_id
LEFT JOIN master.order o 
	ON o.order_id=s.order_id
LEFT JOIN master.customer c
	ON c.customer_id = s.customer_id 
WHERE effective_due_date::date >= dateadd('week',-9,date_trunc('week',current_date))
	AND (CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice' THEN DATEADD('day', 14, sp.due_date)
			  ELSE sp.due_date END)::date < current_date --effective_due_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
--
UNION 
--
SELECT 
	'monthly'::varchar AS report,
	DATE_TRUNC('month',CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
    		THEN DATEADD('day', 14, sp.due_date)
		ELSE sp.due_date
		END)::date AS effective_due_date,
	sp.payment_type,
	sp.payment_method,
	sp.category_name,
	s.retention_group,
	sp.burgel_risk_category,
	CASE WHEN sp.payment_number::int BETWEEN 1 AND 5 THEN sp.payment_number
		 WHEN sp.payment_number::int BETWEEN 6 AND 12 THEN '6-12'
		 WHEN sp.payment_number::int > 12 THEN '12+'
		 END AS payment_number_group,
	CASE WHEN sp.subscription_plan IN ('1 Months', 'Pay As You Go') THEN '1'
		 WHEN sp.subscription_plan IN ('3 Months', '6 Months', '12 Months', '18 Months', '24 Months') 
		 	THEN split_part(sp.subscription_plan, ' ',1)
		 ELSE 'Other' END AS subscription_plan_group,
	CASE WHEN sp.customer_type = 'business_customer' THEN 'B2B' 
		 WHEN sp.store_short ='Grover International' then sp.store_label
		 ELSE sp.store_short END AS store_new,
	CASE sp.customer_type WHEN 'business_customer' THEN 'B2B' ELSE 'B2C' END AS customer_type_cleaned,
	CASE WHEN (c.active_subscriptions = 1 OR c.active_subscriptions = 0) THEN '1'
		 WHEN c.active_subscriptions = 2 THEN '2'
		 WHEN (c.active_subscriptions = 3 or c.active_subscriptions = 4) THEN '3-4'
		 WHEN c.active_subscriptions >= 5 THEN '5+'
		 ELSE 'others'	END AS active_subs_group,
	SUM(sp.amount_due) AS earned_revenue,
	SUM(CASE WHEN DATEDIFF('day', 
				(CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
		    			THEN DATEADD('day', 14, sp.due_date)
					  ELSE sp.due_date END) --effective_due_date
				, sp.paid_date)  <= 14 THEN sp.amount_paid END) AS payment_within_14_days, 
	SUM(CASE WHEN DATEDIFF('day', 
				(CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
		    			THEN DATEADD('day', 14, sp.due_date)
					  ELSE sp.due_date END) --effective_due_date
				, sp.paid_date)  <= 30 
			AND date_trunc('week', effective_due_date) < dateadd('week', -4, date_trunc('week',current_date))
			AND DATE_TRUNC('month', effective_due_date) <>  DATE_TRUNC('month', current_date)
			AND DATE_TRUNC('month', effective_due_date) <> DATEADD('month', -1, DATE_TRUNC('month', current_date))
				THEN sp.amount_paid END) AS payment_within_30_days		
FROM master.subscription_payment sp
LEFT JOIN master.subscription s 
	ON sp.subscription_id = s.subscription_id
LEFT JOIN master.order o 
	ON o.order_id=s.order_id
LEFT JOIN master.customer c
	ON c.customer_id = s.customer_id 
WHERE effective_due_date::date >= dateadd('month',-14,date_trunc('week',current_date))
	AND (CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice' THEN DATEADD('day', 14, sp.due_date)
			  ELSE sp.due_date END)::date < current_date --effective_due_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
WITH NO SCHEMA BINDING	;

GRANT SELECT ON dm_weekly_monthly.v_risk_payments TO tableau;

