WITH a AS (
	SELECT
		date,
		start_date::date AS start_date,
		subscription_id,
		status,
		subscription_value,
		rental_period,
		category_name,
		subcategory_name,
		product_sku,
		product_name,
		subscription_bo_id,
		country_name,
		customer_type,
		currency, 
		lag(subscription_value) OVER (PARTITION BY subscription_id ORDER BY date ASC) AS previous_subs_value, --used to build bucket for the 2 different types of incentives  
		lag(rental_period) OVER (PARTITION BY subscription_id ORDER BY date ASC) AS previous_rental_period --
	FROM master.subscription_historical sh 
	WHERE status = 'ACTIVE'  --only checking on subs with a change   
		AND date >= '2023-01-01' --stakeholder request
)
SELECT 	
	date,
	start_date, 
	subscription_id,
	status,
	subscription_value,
	rental_period,
	category_name,
	subcategory_name,
	product_sku,
	product_name,
	subscription_bo_id,
	country_name,
	customer_type,
	currency, 
	previous_subs_value,
	previous_rental_period
FROM a 
WHERE subscription_value <> previous_subs_value --removed subscription_value = 0 filter as there is another incentive where 3 months after minimum cancellation date we offer 80% discount
WITH NO SCHEMA BINDING
;

