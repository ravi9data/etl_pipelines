CREATE OR REPLACE VIEW dm_sustainability.v_multi_items_orders AS
WITH subs_per_order AS (
SELECT 
	s.order_id,
	date_trunc('month', o.submitted_date)::date AS month_submitted_date,
	count(DISTINCT s.subscription_id) AS nr_subscriptions 
FROM master.subscription s 
LEFT JOIN master.ORDER o 
	ON s.order_id = o.order_id
WHERE date_trunc('year', o.submitted_date) = '2023-01-01'
GROUP BY 1,2
)
, category_per_orders AS (
	SELECT 
		s.order_id,
		s.month_submitted_date,
		CASE WHEN s.nr_subscriptions = 1 THEN 'single item orders'
			 ELSE 'multi items orders'
			 END AS type_orders,
		s.nr_subscriptions,
		LISTAGG(DISTINCT ss.category_name, ', ') WITHIN GROUP (ORDER BY category_name)  AS categories
	FROM subs_per_order s
	LEFT JOIN master.subscription ss 
		ON s.order_id = ss.order_id
	GROUP BY 1,2,3,4
)
, subcategory_per_orders AS (
	SELECT 
		s.order_id,
		s.month_submitted_date,
		CASE WHEN s.nr_subscriptions = 1 THEN 'single item orders'
			 ELSE 'multi items orders'
			 END AS type_orders,
		s.nr_subscriptions,
		LISTAGG(DISTINCT ss.subcategory_name, ', ') WITHIN GROUP (ORDER BY subcategory_name)  AS subcategories
	FROM subs_per_order s
	LEFT JOIN master.subscription ss 
		ON s.order_id = ss.order_id
	GROUP BY 1,2,3,4
)
SELECT 
	c.month_submitted_date,
	c.type_orders,
	c.categories,
	s.subcategories,
--	c.nr_subscriptions,
	count(DISTINCT c.order_id) AS orders,
	sum(c.nr_subscriptions) AS total_subs
FROM category_per_orders c 
LEFT JOIN subcategory_per_orders s
	ON c.order_id = s.order_id
GROUP BY 1,2,3,4--,5
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_sustainability.v_multi_items_orders TO tableau;

 