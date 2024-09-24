DROP VIEW IF EXISTS dwh.v_daily_kpi_report_conversions;
CREATE VIEW dwh.v_daily_kpi_report_conversions AS
WITH 
    account_data AS (
    SELECT DISTINCT c.customer_id
          ,ka.account_owner
          ,COALESCE(segment,'Self Service') AS segment
    FROM master.customer c
             LEFT JOIN dm_b2b.v_key_accounts ka ON ka.customer_id = c.customer_id
    WHERE customer_type = 'business_customer'
)
    ,orders AS (
    SELECT
        o.submitted_date::DATE AS submitted_date
         ,o.store_country as country_name
         ,CASE
              WHEN o.store_country in ('Austria','Netherlands','Spain')
                  THEN 'Rest of Europe'
              WHEN o.store_country =  'United States'
                  THEN 'United States Region'
              ELSE 'Germany Region' END AS region
         ,CASE
              WHEN o.store_commercial = 'Grover International'
                  THEN 'Grover' || ' ' || o.store_country
              WHEN o.store_commercial = 'Partnerships International'
                  THEN 'Partnerships' || ' ' || o.store_country
              WHEN o.store_commercial = 'B2B International' 
                  THEN 'B2B ' || o.store_country
              ELSE o.store_commercial END AS store_commercial_split
         ,o.new_recurring AS retention_group
         ,ad.segment
         ,COUNT(DISTINCT CASE
                             WHEN o.completed_orders >= 1
                                 THEN o.order_id
        END) AS submitted_orders
         ,COUNT(DISTINCT
                CASE
                    WHEN o.paid_orders >= 1
                        THEN o.order_id
                    END) AS paid_orders
    FROM master.order o
             LEFT JOIN account_data ad on ad.customer_id = o.customer_id
    WHERE TRUE
      AND o.submitted_date::date BETWEEN current_date - 65 AND current_date - 1
      AND o.store_label NOT ILIKE '%old%'
      AND o.store_country <> 'United Kingdom'
    GROUP BY 1,2,3,4,5,6
)
   ,order_country AS (
    SELECT
        submitted_date
         ,country_name
         ,region
         ,country_name || ' ' || 'Total' AS store_commercial_split
         ,retention_group
         ,segment
         ,SUM(submitted_orders) AS submitted_orders
         ,SUM(paid_orders) AS paid_orders
    FROM orders
    GROUP BY 1,2,3,4,5,6
)
   ,order_region AS (
    SELECT
        submitted_date
         ,region AS country_name
         ,region
         ,region || ' ' || 'Total' AS store_commercial_split
         ,retention_group
         ,segment
         ,SUM(submitted_orders) AS submitted_orders
         ,SUM(paid_orders) AS paid_orders
    FROM orders
    GROUP BY 1,2,3,4,5,6
)
   ,orders_total AS (
    SELECT
        submitted_date
         ,'Total'::text AS country_name
         ,'Total'::text AS region
         ,'Total'::text AS store_commercial_split
         ,retention_group
         ,segment
         ,SUM(submitted_orders) AS submitted_orders
         ,SUM(paid_orders) AS paid_orders
    FROM order_region
    GROUP BY 1,2,3,4,5,6
)

SELECT * FROM orders
UNION ALL
SELECT * FROM order_country
UNION ALL
SELECT * FROM order_region
UNION ALL
SELECT * FROM orders_total
WITH NO SCHEMA BINDING;
GRANT SELECT ON TABLE dwh.v_daily_kpi_report_conversions TO matillion;
