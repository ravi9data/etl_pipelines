DROP TABLE IF EXISTS tmp_weekly_monthly_risk_payments;
CREATE TEMP TABLE tmp_weekly_monthly_risk_payments
SORTKEY(report, effective_due_date, payment_type, category_name, retention_group, burgel_risk_category, payment_number_group, subscription_plan_group, customer_type_cleaned, active_subs_group)
DISTKEY(effective_due_date)
AS
SELECT 
	'weekly'::varchar AS report,
	DATE_TRUNC('week',CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice'
    		THEN DATEADD('day', 14, sp.due_date)
		ELSE sp.due_date
		END)::date AS effective_due_date,
	sp.payment_type,
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
		 WHEN sp.store_short ='Grover International' THEN sp.store_label
		 ELSE sp.store_short END AS store_new,
	CASE sp.customer_type WHEN 'business_customer' THEN 'B2B' ELSE 'B2C' END AS customer_type_cleaned,
	CASE WHEN (c.active_subscriptions = 1 OR c.active_subscriptions = 0) THEN '1'
		 WHEN c.active_subscriptions = 2 THEN '2'
		 WHEN (c.active_subscriptions = 3 or c.active_subscriptions = 4) THEN '3-4'
		 WHEN c.active_subscriptions >= 5 THEN '5+'
		 ELSE 'others'	END AS active_subs_group,
	CASE 
		WHEN s.store_commercial like ('%B2B%')
		 	THEN 'B2B-Total'
		WHEN csd.first_subscription_acquisition_channel like ('%Partners%') 
			THEN 'Partnerships-Total'
		WHEN csd.first_subscription_store like 'Grover - Germany%'
			THEN 'Grover-DE'
		WHEN csd.first_subscription_store in ('Grover - UK online')
			THEN 'Grover-UK'
		WHEN csd.first_subscription_store in ('Grover - Netherlands online')
			THEN 'Grover-NL'
		WHEN csd.first_subscription_store in ('Grover - Austria online')
			THEN 'Grover-Austria'
		WHEN csd.first_subscription_store in ('Grover - Spain online')
			THEN 'Grover-Spain'
		WHEN csd.first_subscription_store in ('Grover - United States online')
			THEN 'Grover-US'
		ELSE 'Grover-DE'
	END as first_subscription_store,
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
LEFT JOIN ods_production.customer_subscription_details csd 
	on csd.customer_id = s.customer_id
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
		 WHEN sp.store_short ='Grover International' THEN sp.store_label
		 ELSE sp.store_short END AS store_new,
	CASE sp.customer_type WHEN 'business_customer' THEN 'B2B' ELSE 'B2C' END AS customer_type_cleaned,
	CASE WHEN (c.active_subscriptions = 1 OR c.active_subscriptions = 0) THEN '1'
		 WHEN c.active_subscriptions = 2 THEN '2'
		 WHEN (c.active_subscriptions = 3 or c.active_subscriptions = 4) THEN '3-4'
		 WHEN c.active_subscriptions >= 5 THEN '5+'
		 ELSE 'others'	END AS active_subs_group,
	CASE 
		WHEN s.store_commercial like ('%B2B%')
		 	THEN 'B2B-Total'
		WHEN csd.first_subscription_acquisition_channel like ('%Partners%') 
			THEN 'Partnerships-Total'
		WHEN csd.first_subscription_store like 'Grover - Germany%'
			THEN 'Grover-DE'
		WHEN csd.first_subscription_store in ('Grover - UK online')
			THEN 'Grover-UK'
		WHEN csd.first_subscription_store in ('Grover - Netherlands online')
			THEN 'Grover-NL'
		WHEN csd.first_subscription_store in ('Grover - Austria online')
			THEN 'Grover-Austria'
		WHEN csd.first_subscription_store in ('Grover - Spain online')
			THEN 'Grover-Spain'
		WHEN csd.first_subscription_store in ('Grover - United States online')
			THEN 'Grover-US'
		ELSE 'Grover-DE'
	END as first_subscription_store,
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
LEFT JOIN ods_production.customer_subscription_details csd 
	on csd.customer_id = s.customer_id
WHERE effective_due_date::date >= dateadd('month',-14,date_trunc('week',current_date))
	AND (CASE WHEN sp.customer_type = 'business_customer' AND sp.payment_method = 'pay-by-invoice' THEN DATEADD('day', 14, sp.due_date)
			  ELSE sp.due_date END)::date < current_date --effective_due_date
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
;

BEGIN TRANSACTION;

DROP TABLE IF EXISTS dwh.weekly_monthly_risk_payments;
CREATE TABLE dwh.weekly_monthly_risk_payments AS
SELECT *
FROM tmp_weekly_monthly_risk_payments;

END TRANSACTION;

GRANT SELECT ON dwh.weekly_monthly_risk_payments TO tableau;
