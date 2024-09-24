CREATE OR REPLACE VIEW dm_finance.purchase_price_monitoring_weekly_numbers AS 
WITH avg_pur_price AS (
SELECT
  DATE_TRUNC('week',date) AS reporting_date,
  variant_sku ,
  ROUND(AVG(initial_price),2) avg_purchase_price
FROM master.asset_historical ah2 
WHERE TRUE 
  AND date > '2024-06-01'
GROUP BY 1,2
)
SELECT 
  DATE_TRUNC('week',a.date) AS reporting_date,
  a.purchased_date ,
  a.capital_source_name ,
  count(DISTINCT a.asset_id) AS number_of_assets
FROM master.asset_historical a
  INNER JOIN avg_pur_price b ON
    a.variant_sku = b.variant_sku
    AND  DATE_TRUNC('week',a.date) = b.reporting_date
WHERE TRUE  
  AND a.date > '2024-06-01'
  AND (a.initial_price / NULLIF(b.avg_purchase_price,0) < 0.5 
       OR a.initial_price / NULLIF(b.avg_purchase_price,0) > 1.5)
  AND a.capital_source_name IN ('Grover Finance I GmbH','SUSTAINABLE TECH RENTAL EUROPE II GMBH','USA_test')
GROUP BY 1,2,3
WITH NO SCHEMA BINDING
;
