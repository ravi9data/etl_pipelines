DROP TABLE dm_product.conse_gap_analysis_new;
CREATE TABLE dm_product.conse_gap_analysis_new AS 
WITH valid_subs AS (
    SELECT 
        s.customer_id,
        s.subscription_id,
        s.start_date,
        s.cancellation_date,
        s.category_name,
        s.subcategory_name,
        s.subscription_value_euro,
        s.rental_period,
        s.effective_duration,
        CASE WHEN s.cancellation_reason_churn = 'customer request' 
        		AND s.cancellation_reason_new = 'SOLD 1-EUR' THEN 'customer request - Sold 1 EUR'
        	 WHEN s.cancellation_reason_churn = 'customer request' 
        		AND s.cancellation_reason_new = 'SOLD EARLY' THEN 'customer request - Sold Early'
        	 WHEN s.cancellation_reason_churn = 'customer request' THEN 'customer request - Other'
        	 ELSE s.cancellation_reason_churn END AS cancellation_reason_churn
    FROM master.subscription s 
)
, next_start_date AS (
    SELECT 
        s.subscription_id, 
        min(s2.start_date) AS next_general_start_date
    FROM valid_subs s
    LEFT JOIN valid_subs s2 
        ON s.customer_id = s2.customer_id 
        AND s.cancellation_date < s2.start_date
    GROUP BY 1
)
, next_start_date_category AS (
    SELECT 
        s.subscription_id,
        min(s3.start_date) AS next_category_start_date
    FROM valid_subs s
    LEFT JOIN valid_subs s3
        ON s.customer_id = s3.customer_id 
        AND s.category_name = s3.category_name
        AND s.cancellation_date < s3.start_date
    GROUP BY 1
)
, next_start_date_subcategory AS (
    SELECT 
        s.subscription_id, 
        min(s4.start_date) AS next_subcategory_start_date
    FROM valid_subs s
    LEFT JOIN valid_subs s4 
        ON s.customer_id = s4.customer_id 
        AND s.subcategory_name = s4.subcategory_name
        AND s.cancellation_date < s4.start_date
    GROUP BY 1
)
, num_cancellations AS (
    SELECT 
        customer_id,
        count(DISTINCT CASE WHEN cancellation_date IS NOT NULL THEN subscription_id END) num_cancellations, 
        count(DISTINCT subscription_id) num_rentals 
    FROM valid_subs
    GROUP BY 1
)
SELECT  
    s.customer_id,
    s.subscription_id, 
    s.subscription_value_euro,
    s.category_name,
    s.subcategory_name, 
    s.start_date, 
    s.rental_period,
    s.effective_duration,
    s.cancellation_date, 
    s.cancellation_reason_churn,
    lead(s.start_date) OVER(PARTITION BY s.customer_id ORDER BY s.start_date, s.subscription_id) next_general_start_date_lead,
    lead(s.start_date) OVER(PARTITION BY s.customer_id, s.category_name ORDER BY s.start_date, s.subscription_id) next_category_start_date_lead,
    lead(s.start_date) OVER(PARTITION BY s.customer_id, s.subcategory_name ORDER BY s.start_date, s.subscription_id) next_subcategory_start_date_lead,
    COALESCE(nsd.next_general_start_date, next_general_start_date_lead) AS next_general_startdate, 
    COALESCE(nsdc.next_category_start_date, next_category_start_date_lead) AS next_category_startdate, 
    COALESCE(nsds.next_subcategory_start_date, next_subcategory_start_date_lead) AS next_subcategory_startdate, 
--  row_number() OVER (PARTITION BY s.customer_id ORDER BY s.start_date, s.subscription_id) AS rank_general,
--  row_number() OVER (PARTITION BY s.customer_id, s.category_name ORDER BY s.start_date, s.subscription_id) AS rank_category,
--  row_number() OVER (PARTITION BY s.customer_id, s.subcategory_name ORDER BY s.start_date, s.subscription_id) AS rank_subcategory,
    CASE WHEN next_general_startdate > s.cancellation_date THEN 1 ELSE 0 END AS is_subsgap_overall, 
    CASE WHEN next_category_startdate > s.cancellation_date THEN 1 ELSE 0 END AS is_subsgap_category, 
    CASE WHEN next_subcategory_startdate > s.cancellation_date THEN 1 ELSE 0 END AS is_subsgap_subcategory, 
    CASE WHEN is_subsgap_overall = 1 
        THEN date_diff('day', s.cancellation_date, next_general_startdate) 
        ELSE date_diff('day', s.cancellation_date, next_general_start_date_lead) 
    END AS general_gap_days, 
    CASE WHEN is_subsgap_category = 1 
        THEN date_diff('day', s.cancellation_date, next_category_startdate) 
        ELSE date_diff('day', s.cancellation_date, next_category_start_date_lead) 
    END AS category_gap_days, 
    CASE WHEN is_subsgap_subcategory = 1 
        THEN date_diff('day', s.cancellation_date, next_subcategory_startdate) 
        ELSE date_diff('day', s.cancellation_date, next_subcategory_start_date_lead) 
    END AS subcategory_gap_days,
    sr.num_cancellations,
    sr.num_rentals
FROM valid_subs s
LEFT JOIN next_start_date nsd 
  ON nsd.subscription_id = s.subscription_id
LEFT JOIN next_start_date_category nsdc
  ON nsdc.subscription_id = s.subscription_id
LEFT JOIN next_start_date_subcategory nsds 
  ON nsds.subscription_id = s.subscription_id
LEFT JOIN num_cancellations sr 
  ON s.customer_id = sr.customer_id;
  
GRANT SELECT ON dm_product.conse_gap_analysis_new TO tableau;
