-- marcel_weber.v_conse_gap_analysis source

CREATE OR REPLACE VIEW dm_product.v_conse_gap_analysis AS 
WITH next_start_date AS (
    SELECT s.customer_id, s.subscription_id, s.start_date, s.cancellation_date, min(s2.start_date) AS next_start_date
    FROM master.subscription s
    LEFT JOIN master.subscription s2 
        ON s.customer_id = s2.customer_id 
        AND s.cancellation_date < s2.start_date
        AND s2.subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers') 
    WHERE s.subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers') 
    GROUP BY s.customer_id, s.subscription_id, s.start_date, s.cancellation_date
)
, num_cancellations AS (
    SELECT 
        customer_id,
        count(DISTINCT CASE WHEN cancellation_date IS NOT NULL THEN subscription_id END) num_cancellations, 
        max(rank_subscriptions) num_rentals 
    FROM master.subscription 
    --WHERE subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers') 
    GROUP BY 1
)
SELECT 	
	s.customer_id,
	s.subscription_id, 
	s.subcategory_name, 
	s.start_date, 
	s.cancellation_date, 	
	COALESCE(nsd.next_start_date, lead(s.start_date) OVER(PARTITION BY s.customer_id, s.subcategory_name ORDER BY s.start_date, s.subscription_id)) AS next_subscription_startdate, 
	row_number() OVER (PARTITION BY s.customer_id, s.subcategory_name ORDER BY s.start_date, s.subscription_id) AS rank_subscription, 
    CASE
   		WHEN next_subscription_startdate > s.cancellation_date 
    		THEN 1
        ELSE 0
    END AS is_subsgap_overall, 
    CASE
        WHEN is_subsgap_overall = 1 
        	THEN date_diff('day', s.cancellation_date, COALESCE(nsd.next_start_date, lead(s.start_date) OVER(PARTITION BY s.customer_id, s.subcategory_name ORDER BY s.start_date, s.subscription_id)))
    END AS subgap_days, 
    CASE
        WHEN subgap_days >= 0 AND subgap_days <= 3 
            THEN '1. 0 - 3'
        WHEN subgap_days >= 4 AND subgap_days <= 7 
            THEN '2. 4 - 7'
        WHEN subgap_days >= 8 AND subgap_days <= 14 
            THEN '3. 8 - 14'
        WHEN subgap_days >= 15 AND subgap_days <= 30 
            THEN '4. 15 - 30'
        WHEN subgap_days >= 31 AND subgap_days <= 60 
            THEN '5. 31 - 60'
        WHEN subgap_days >= 61 AND subgap_days <= 90 
            THEN '6. 61 - 90'
        WHEN subgap_days > 90
            THEN '7. > 90'
        WHEN s.cancellation_date IS NULL
            THEN 'active subscribtion'
        WHEN next_subscription_startdate IS NULL OR sr.num_cancellations = sr.num_rentals
            THEN 'stopped renting'
        WHEN subgap_days IS NULL
            THEN 'overlap'	 
        ELSE NULL::text
      END AS subgap_buckets, 
      s.cancellation_reason,
      s.cancellation_reason_new, 
      s.cancellation_reason_churn, 
      s.subscription_plan, 
      s.rental_period, 
      s.product_name, 
      s.brand
   FROM master.subscription s
   LEFT JOIN next_start_date nsd 
    ON nsd.customer_id = s.customer_id 
    AND nsd.subscription_id::text = s.subscription_id::text 
    AND nsd.start_date = s.start_date 
    AND nsd.cancellation_date = s.cancellation_date
   LEFT JOIN num_cancellations sr 
   	ON s.customer_id = sr.customer_id 
  WHERE s.subcategory_name IN ('Smartphones', '2-in-1 laptops', 'Laptops', 'Desktop Computers', 'Gaming Computers') 
--  ORDER BY customer_id, start_date 
--  ORDER BY s.start_date
  WITH NO SCHEMA BINDING;
 
 GRANT SELECT ON dm_product.v_conse_gap_analysis TO tableau;
 