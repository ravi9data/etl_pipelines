BEGIN;
TRUNCATE TABLE master.addon;

INSERT INTO master.addon
SELECT addon_id, 
order_id, 
customer_id, 
related_variant_sku, 
related_product_sku, 
addon_name, 
product_name, 
category_name, 
subcategory_name, 
add_on_variant_id, 
country, 
add_on_status, 
order_status, 
initial_scoring_decision, 
submitted_date, 
approved_date, 
paid_date, 
order_amount, 
addon_amount, 
duration, 
avg_plan_duration, 
quantity
FROM ods_production.addon;

COMMIT;