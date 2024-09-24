DROP VIEW IF EXISTS dwh.v_daily_kpi_report_payments;
CREATE VIEW dwh.v_daily_kpi_report_payments AS
WITH 
    account_data AS (
    SELECT DISTINCT c.customer_id
          ,ka.account_owner
          ,COALESCE(segment,'Self Service') AS segment
    FROM master.customer c
             LEFT JOIN dm_b2b.v_key_accounts ka ON ka.customer_id = c.customer_id
    WHERE customer_type = 'business_customer'
)
,payments AS (
    SELECT
        sp.due_date::date AS due_date
         ,s.country_name
         ,CASE
              WHEN s.country_name in ('Austria','Netherlands','Spain')
                  THEN 'Rest of Europe'
              WHEN s.country_name = 'United Kingdom'
                  THEN 'UK'
              WHEN s.country_name =  'United States'
                  THEN 'United States Region'
              ELSE 'Germany Region' END AS region
         ,CASE
              WHEN s.store_commercial = 'Grover International'
                  THEN 'Grover' || ' ' || s.country_name
              WHEN s.store_commercial = 'Partnerships International'
                  THEN 'Partnerships' || ' ' || s.country_name
              WHEN s.store_commercial = 'B2B International' 
                  THEN 'B2B ' || s.country_name
              ELSE s.store_commercial END AS store_commercial_split
         ,sp.payment_type
         ,ad.segment
         ,SUM(sp.amount_due) AS amount_due
         ,COALESCE(SUM(CASE
                           WHEN sp.paid_date IS NOT NULL AND sp.subscription_payment_category NOT LIKE '%PAID_CHARGEBACK%'
                               THEN sp.amount_paid END),0) AS amount_paid
    FROM master.subscription_payment sp
             LEFT JOIN master.subscription s
                       ON sp.subscription_id  = s.subscription_id
             LEFT JOIN account_data ad on ad.customer_id = sp.customer_id
    WHERE TRUE
      AND sp.due_date::date BETWEEN current_date - 65 AND current_date - 1
      AND s.store_label not ilike '%old%'
      AND s.country_name <> 'United Kingdom'
    GROUP BY 1,2,3,4,5,6
)
   ,payment_country AS (
    SELECT
        due_date
         ,country_name
         ,region
         ,country_name || ' ' || 'Total' AS store_commercial_split
         ,payment_type
         ,segment
         ,SUM(amount_due) AS amount_due
         ,SUM(amount_paid) AS amount_paid
    FROM payments
    GROUP BY 1,2,3,4,5,6
)
   ,payment_region AS (
    SELECT
        due_date
         ,region AS country_name
         ,region
         ,region || ' ' || 'Total' AS store_commercial_split
         ,payment_type
         ,segment
         ,SUM(amount_due) AS amount_due
         ,SUM(amount_paid) AS amount_paid
    FROM payments
    GROUP BY 1,2,3,4,5,6
)
   ,payment_total AS (
    SELECT
        due_date
         ,'Total'::text AS country_name
         ,'Total'::text AS region
         ,'Total'::text AS store_commercial_split
         ,payment_type
         ,segment
         ,SUM(amount_due) AS amount_due
         ,SUM(amount_paid) AS amount_paid
    FROM payment_country
    GROUP BY 1,2,3,4,5,6
)
   ,payment_combined AS (
    SELECT * FROM payments
    UNION ALL
    SELECT * FROM payment_country
    UNION ALL
    SELECT * FROM payment_region
    UNION ALL
    SELECT * FROM payment_total
)
SELECT *
FROM payment_combined
UNION ALL
SELECT
    due_date
     ,country_name
     ,region
     ,store_commercial_split
     ,'TOTAL'::text AS payment_type
     ,segment
     ,SUM(amount_due) AS amount_due
     ,SUM(amount_paid) AS amount_paid
FROM payment_combined
GROUP BY 1,2,3,4,5,6
WITH NO SCHEMA BINDING;
GRANT SELECT ON TABLE dwh.v_daily_kpi_report_payments TO matillion;