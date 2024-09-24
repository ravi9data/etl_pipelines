CREATE OR REPLACE VIEW dm_product.v_overlap_analysis AS
SELECT 
	s.customer_id,
	s.subscription_id,
	subcategory_name,
	s.start_date,
	s.cancellation_date,
	COALESCE(nsd.next_start_date, LEAD(s.start_date) OVER (PARTITION BY s.customer_id, s.subcategory_name ORDER BY s.start_date, s.subscription_id)) AS next_subscription_startdate,
	status,
	LEAD(status) OVER (PARTITION BY s.customer_id, subcategory_name ORDER BY s.start_date, s.subscription_id ) AS next_subscription_status,
	LAST_VALUE(status) OVER (PARTITION BY s.customer_id, subcategory_name ORDER BY s.start_date, s.subscription_id rows between unbounded preceding and unbounded following) AS mostrecent_subscription_status,
	ROW_NUMBER() OVER (PARTITION BY s.customer_id, subcategory_name ORDER BY s.start_date, s.subscription_id) AS rank_subscription,
	CASE WHEN next_subscription_startdate < s.cancellation_date THEN 1 ELSE 0 END AS is_overlap_overall,
	CASE WHEN next_subscription_startdate < s.cancellation_date THEN DATEDIFF(day, next_subscription_startdate, s.cancellation_date) END AS overlap_days,
	CASE WHEN overlap_days BETWEEN 0 AND 3 THEN '1. 0 - 3'
		WHEN overlap_days BETWEEN 4 AND 7 THEN '2. 4 - 7'
		WHEN overlap_days BETWEEN 8 AND 14 THEN '3. 8 - 14'
		WHEN overlap_days BETWEEN 15 AND 30 THEN '4. 15 - 30'
		WHEN overlap_days BETWEEN 31 AND 60 THEN '5. 31 - 60'
		WHEN overlap_days BETWEEN 61 AND 90 THEN '6. 61 - 90'
		WHEN overlap_days > 90 THEN '7. > 90'
		WHEN s.cancellation_date IS NULL THEN 'active subscribtion'
		WHEN next_subscription_startdate IS NULL THEN 'stopped renting'
		ELSE NULL
	END AS overlap_buckets,
    CASE WHEN next_subscription_startdate < s.cancellation_date
        AND DATEDIFF('day', next_subscription_startdate, s.cancellation_date) BETWEEN 0 AND 14 THEN 1 ELSE 0 END AS is_reasonable_overlap_within_2w,
    CASE WHEN next_subscription_startdate < s.cancellation_date
        AND DATEDIFF('day', next_subscription_startdate, s.cancellation_date) BETWEEN 0 AND 30 THEN 1 ELSE 0 END AS is_reasonable_overlap_within_1m,
	cancellation_reason,
	cancellation_reason_new,
	cancellation_reason_churn,
	subscription_plan,
	rental_period,
	product_name,
	brand
FROM master.subscription s
LEFT JOIN (
	SELECT s.customer_id, s.subscription_id , s.start_date, s.cancellation_date, MIN(s2.start_date) AS next_start_date
	FROM master.subscription s
	LEFT JOIN master.subscription s2 
	ON s.customer_id = s2.customer_id 
	AND  s.cancellation_date < s2.start_date
	GROUP BY 1,2,3,4
) nsd
  ON nsd.customer_id = s.customer_id 
  AND nsd.subscription_id = s.subscription_id 
  AND nsd.start_date = s.start_date 
  AND nsd.cancellation_date = s.cancellation_date 
WHERE subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers') 
  AND s.start_date > '2021-01-01'
-- AND status = 'CANCELLED'
-- AND ssubscriptions_per_customer > 1
-- AND cancellation_reason_new NOT ILIKE 'CANCELLED BEFORE%' AND cancellation_reason_new NOT IN ('FAILED DELIVERY', 'REVOCATION') 
WITH NO SCHEMA BINDING;

GRANT SELECT ON dm_product.v_overlap_analysis TO tableau;