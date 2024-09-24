SELECT 
	order_submitted_date::date,
	gc.country,
	COALESCE(s.category_name,v.category_name) AS category_name,
	COALESCE(s.subcategory_name,v.subcategory_name) AS subcategory_name,
	gc.new_recurring,
	CASE
		WHEN oc.os ilike 'web' THEN 'web'
		WHEN oc.os in ('android','ios') THEN 'app'
		WHEN o.device = 'n/a' THEN 'app'
		ELSE NULL
	END AS app_web,
	-- Subscriptions Submitted
	-- Subscriptions Paid
	-- Subscriptions Paid Price
FROM 
LEFT JOIN 
	ods_production.subscription s 
	ON gc.subscription_id = s.subscription_id
LEFT JOIN 
	master.order o
	ON o.order_id = gc.order_id
LEFT JOIN 
	traffic.order_conversions oc  
    ON o.order_id = oc.order_id
LEFT JOIN -- for category and subcategory for old infra data
	master.variant v 
	ON gc.variant_sku = v.variant_sku
WHERE 
	gc.country in ('Austria','United States','Germany')
	AND order_submitted_date < (current_Date-1)  
GROUP BY 
	1,2,3,4,5,6
WITH NO SCHEMA BINDING
;

