DROP VIEW IF EXISTS dm_b2b.b2b_scalability;

CREATE VIEW dm_b2b.b2b_scalability AS 
WITH base AS (
    SELECT DISTINCT 
        date_trunc ('month',s.fact_date)::date AS fact_month, 
        s.customer_id,
        CASE WHEN c.consolidation_day IS NULL OR c.consolidation_day = '' 
                THEN  'without' 
            ELSE 'with' 
        END AS consolidated_billing_status,
        c.consolidation_day,
        s.active_subscription_value
    FROM  ods_finance.active_subscriptions_overview s
        LEFT JOIN ods_production.customer c
            ON s.customer_id = c.customer_id
    WHERE s.fact_month >= '2020-01-01'
         AND c.customer_type = 'business_customer'
)
SELECT 
    b.fact_month,
    CASE WHEN u.full_name = 'B2B Support Grover' THEN 'Sales'
         ELSE 'Self-service'
    END AS segment, 
    CASE WHEN a.numberofemployees = 0 THEN '0'
         WHEN a.numberofemployees BETWEEN 1 AND 10 THEN  '1-10'
         WHEN a.numberofemployees BETWEEN 11 AND 50 THEN  '11-50'
         WHEN a.numberofemployees BETWEEN 51 AND 200 THEN  '51-200'
         WHEN a.numberofemployees BETWEEN 201 AND 500 THEN  '201-500'
         WHEN a.numberofemployees BETWEEN 501 AND 1000 THEN  '501-1000'
         WHEN a.numberofemployees > 1000 THEN '>1000'
         WHEN a.numberofemployees IS NULL THEN 'null'
    END AS number_of_employees,
    COUNT(DISTINCT b.customer_id) total_customers,
    COUNT (DISTINCT 
            CASE WHEN b.consolidated_billing_status = 'with' 
                    THEN  b.customer_id 
            END) AS customers_with_consolidated_billing,
    COUNT (DISTINCT 
            CASE WHEN b.consolidated_billing_status = 'without' 
                    THEN  b.customer_id 
            END) AS customers_without_consolidated_billing,         
    SUM(b.active_subscription_value) AS active_subscription_value  
FROM base b
    LEFT JOIN ods_b2b.account a
        ON b.customer_id = a.customer_id
    LEFT JOIN ods_b2b.USER u
        ON u.user_id = a.account_owner
GROUP BY 1,2,3
ORDER BY 1,2,3
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_b2b.b2b_scalability TO tableau;