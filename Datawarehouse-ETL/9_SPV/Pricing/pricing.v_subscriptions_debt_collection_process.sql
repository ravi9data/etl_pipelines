CREATE OR REPLACE VIEW pricing.v_subscriptions_debt_collection_process AS
SELECT 
	CASE WHEN dpd <= 0 THEN 0
		 ELSE dpd 
		END AS dpd,
	CASE WHEN dpd <= 0 THEN 'No Debt Collection'
		  WHEN dpd <= 30 THEN 'Until 1 month'
		  WHEN dpd <= 90 THEN '1 - 3 months'
		  WHEN dpd <= 360 THEN '3 - 12 months'
		  WHEN dpd > 360 THEN 'More than 12 months'
		END AS dpd_classification,
	customer_type,
	country_name,
	subcategory_name,
	category_name,
	store_id,
	count(DISTINCT subscription_id) AS number_of_subscriptions
FROM master.subscription
WHERE status = 'ACTIVE'
AND country_name <> 'United Kingdom'
AND dpd IS NOT NULL
AND start_date::date >= DATEADD('year', -1, current_date)::date
GROUP BY 1,2,3,4,5,6,7
WITH NO SCHEMA BINDING;
