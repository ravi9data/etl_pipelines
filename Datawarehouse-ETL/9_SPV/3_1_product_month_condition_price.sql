DELETE FROM ods_external.product_month_condition_price 
WHERE reporting_date = CURRENT_DATE - 1
;

INSERT INTO ods_external.product_month_condition_price 
WITH a AS (    
SELECT 
  reporting_month
 ,product_sku
 ,asset_condition
 ,src
 ,max_available_date AS max_available_reporting_month
 ,AVG(CASE
   WHEN price_rank <= 3 
    THEN price
   ELSE NULL
  END) AS avg_3_lowest_price
 ,AVG(price) AS avg_price
FROM ods_spv_historical.spv_used_asset_price
WHERE TRUE 
  AND UPPER(asset_condition) <> 'OTHERS'
  AND product_sku IS NOT NULL 
  AND price IS NOT NULL
  AND reporting_date = CURRENT_DATE - 1
GROUP BY 1,2,3,4,5
)
SELECT 
 CURRENT_DATE - 1 AS reporting_date
 ,a.product_sku
 ,p.product_name
 ,p.category_name
 ,p.subcategory_name
 ,p.brand
 ,a.asset_condition
 ,a.reporting_month
 ,AVG(a.avg_3_lowest_price) AS avg_3_lowest_price
 ,AVG(a.avg_price) AS avg_avg_price
FROM a
  LEFT JOIN ods_production.product p 
    ON p.product_sku = a.product_sku
GROUP BY 1,2,3,4,5,6,7,8
;