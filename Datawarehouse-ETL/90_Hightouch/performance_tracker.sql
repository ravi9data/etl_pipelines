DROP TABLE IF EXISTS hightouch_sources.performance_tracker;
CREATE TABLE hightouch_sources.performance_tracker AS 
SELECT s.variant_sku, 
		s.product_sku,
		s.start_date::date AS subscription_start_date,
		1 AS quantity,
		s.store_name,
		s.subscription_value AS rental_plan_price,
		s.subscription_plan AS rental_plan_type,
		s.subscription_id AS subscription_id,
		s.category_name AS category,
		s.subcategory_name AS subcategory
FROM ods_production.subscription s
WHERE subscription_start_date > '2021-12-31';
GRANT SELECT ON hightouch_sources.performance_tracker TO hightouch;
GRANT SELECT ON hightouch_sources.performance_tracker TO group pricing;
GRANT SELECT ON hightouch_sources.performance_tracker TO hightouch_pricing;
