CREATE OR REPLACE VIEW dm_product.v_conse_subcategory_cohort AS
WITH valid_subs AS (
SELECT DISTINCT
	s.customer_id, 
	s.subscription_id, 
	s.start_date,
	s.cancellation_date,  
	s.subcategory_name,
	p.brand,
	CASE WHEN s.cancellation_reason_new LIKE '%SOLD%' THEN 1 END AS is_product_bought
FROM master.subscription s
LEFT JOIN ods_production.product p 
  ON s.product_sku = p.product_sku 
WHERE s.status = 'CANCELLED'
  --AND paid_subscriptions = rental_period /*identify the closed subscriptions - reduces the number of customers a lot*/  
  AND s.paid_subscriptions = s.payment_count /*this only means the customer has paid on time, in combination with outstanding_asset = 0 should be fine*/
  AND s.outstanding_assets = 0
  AND s.last_valid_payment_category = 'PAID_TIMELY' /* this condition might be checked with the risk classification of the customer later, so might be redundant */
  AND s.subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers')
)
, raw_ AS (
SELECT 
	vs.*,
	s.start_date next_start_date,
	s.subcategory_name next_subcategory,
	p.brand next_brand,
	ROW_NUMBER() OVER (PARTITION BY vs.subscription_id ORDER BY s.start_date) AS next_general_subscription,
	CASE WHEN vs.subcategory_name = s.subcategory_name THEN ROW_NUMBER() OVER (PARTITION BY vs.subscription_id, s.subcategory_name ORDER BY s.start_date) END AS next_subcategory_subscription
FROM valid_subs vs 
LEFT JOIN master.subscription s
  ON vs.customer_id = s.customer_id 
 AND s.start_date > vs.cancellation_date 
LEFT JOIN ods_production.product p 
  ON s.product_sku = p.product_sku 
)
SELECT DISTINCT
	customer_id,
	subscription_id ,
	is_product_bought ,
	cancellation_date ,
	subcategory_name ,
	brand,
	CASE WHEN next_subcategory_subscription = 1 THEN next_start_date END next_subcategory_start_date,
	DATEDIFF('month', cancellation_date, next_subcategory_start_date) AS diff_subcategory,
	CASE WHEN next_subcategory_subscription = 1 THEN next_subcategory END next_sub_within_subcategory,
	CASE WHEN next_subcategory_subscription = 1 THEN next_brand END next_brand_within_subcategory,
	next_start_date,
	DATEDIFF('month', cancellation_date, next_start_date) AS diff_to_next_sub,
	next_brand,
	next_subcategory 
FROM raw_ 
WHERE (next_subcategory_subscription = 1 OR next_general_subscription = 1)
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_product.v_conse_subcategory_cohort TO tableau;
