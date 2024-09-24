CREATE OR REPLACE VIEW dwh.v_monthly_reporting_order AS
SELECT DISTINCT 
	date_trunc('month', created_date) AS fact_date,
	CASE WHEN o.device = 'App' THEN 'APP'
		 WHEN o.store_commercial ILIKE '%b2b%' THEN 'B2B'
		 WHEN o.store_short = 'Partners Offline' THEN 'Retail Offline'
		 WHEN o.store_short = 'Partners Online' THEN 'Retail Online'
		 WHEN o.store_short = 'Grover International' THEN 'International'
		 WHEN o.store_short = 'Grover' THEN 'Grover Germany'
		 ELSE o.store_short END store_channel,
	o.new_recurring,
	CASE WHEN o.new_recurring = 'RECURRING' THEN o.retention_group
		 ELSE 'NEW' END AS retention_group,
	CASE WHEN o.voucher_type ILIKE '%no voucher%' THEN 'No Voucher'
		 WHEN o.voucher_type ILIKE 'referals%' OR o.voucher_type ILIKE 'referrals%' THEN 'Referrals'
		 ELSE 'With Vouchers' END AS voucher_type,
	CASE WHEN COALESCE(o.customer_type,'No Info') = 'normal_customer' THEN 'B2C'
		     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer = 1 THEN 'B2B freelancer'
		     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' AND fre.is_freelancer != 1 THEN 'B2B nonfreelancer'
		     WHEN COALESCE(o.customer_type,'No Info') = 'business_customer' THEN 'B2B Unknown Split'
		  ELSE 'n/a' END AS customer_type_freelancer_split,
	count(DISTINCT CASE WHEN o.paid_orders > 0 THEN order_id end) AS paid_orders
FROM master.ORDER o 
LEFT JOIN master.customer c 
	ON o.customer_id=c.customer_id
LEFT JOIN dm_risk.b2b_freelancer_mapping fre 
	ON c.company_type_name = fre.company_type_name
WHERE date_trunc('month', created_date) >= DATEADD('month',-13,date_trunc('month',current_date))
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA BINDING;  

GRANT SELECT ON dwh.v_monthly_reporting_order TO tableau;
