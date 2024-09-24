CREATE VIEW dm_product.v_conse_analysis_customer_sample AS
WITH valid_customers AS (
SELECT DISTINCT customer_id--, subscription_id, start_date
FROM master.subscription 
WHERE status = 'CANCELLED'
  --AND paid_subscriptions = rental_period /*identify the closed subscriptions - reduces the number of customers a lot*/  
  AND paid_subscriptions = payment_count /*this only means the customer has paid on time, in combination with outstanding_asset = 0 should be fine*/
  AND outstanding_assets = 0
  AND last_valid_payment_category = 'PAID_TIMELY' /* this condition might be checked with the risk classification of the customer later, so might be redundant */
  AND subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers')  /* essential tech */
)
, active_smartphone_subscription AS (
SELECT customer_id, subscription_id, subcategory_name
FROM master.subscription s 
WHERE subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers')
  AND status = 'ACTIVE'
)
, base AS (
SELECT 
	s2.customer_id ,
	s2.country_name ,
	s2.subscription_id ,
	s2.start_date ,
	s2.outstanding_duration ,
	s2.category_name AS category ,
	s2.subcategory_name AS subcategory ,
	s2.product_name ,
	CASE WHEN s2.status = 'CANCELLED' 
    	  AND s2.paid_subscriptions = s2.payment_count 
	      AND s2.outstanding_assets = 0 
	      AND s2.last_valid_payment_category = 'PAID_TIMELY' 
	THEN TRUE 
	ELSE FALSE
	END AS eligibility,
	ISNULL(LAG(eligibility) OVER (PARTITION BY s2.customer_id ORDER BY s2.start_date, s2.subscription_id), FALSE) AS prev_eligibility,
	ISNULL(LAG(eligibility) OVER (PARTITION BY s2.customer_id, s2.subcategory_name ORDER BY s2.start_date, s2.subscription_id), FALSE) AS prev_eligibility_subcategory,
	CASE WHEN altr.subscription_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_active_subscription,
	s2.rental_period,
	ROW_NUMBER() OVER (PARTITION BY s2.customer_id, s2.subcategory_name ORDER BY s2.start_date, s2.subscription_id) AS rank_subcategory_subscription,
	ROW_NUMBER() OVER (PARTITION BY s2.customer_id ORDER BY s2.start_date, s2.subscription_id) AS rank_subscription
FROM master.subscription s2
INNER JOIN valid_customers vc 
  ON vc.customer_id = s2.customer_id 
LEFT JOIN master."order" o
  ON o.order_id = s2.order_id 
LEFT JOIN active_smartphone_subscription altr 
  ON s2.subscription_id = altr.subscription_id
 AND s2.subcategory_name = altr.subcategory_name 
WHERE s2.subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers')
)
, customer_risk AS (
SELECT DISTINCT c.customer_id, cs.customer_label_new AS customer_risk_class
FROM master.customer c
LEFT JOIN ods_production.customer_scoring cs 
  ON c.customer_id = cs.customer_id 
WHERE cs.customer_label_new IN ('good', 'uncertain')
)
, final_ AS (
SELECT b.*, cr.customer_risk_class
FROM base b
INNER JOIN customer_risk cr 
  ON b.customer_id = cr.customer_id
WHERE is_active_subscription = TRUE
  AND (prev_eligibility = TRUE OR prev_eligibility_subcategory = TRUE)
)
SELECT * FROM final_ f
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_product.v_conse_analysis_customer_sample TO tableau;