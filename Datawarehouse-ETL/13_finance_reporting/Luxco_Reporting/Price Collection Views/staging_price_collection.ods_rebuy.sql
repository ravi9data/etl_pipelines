CREATE OR REPLACE VIEW staging_price_collection.ods_rebuy AS
WITH a AS
(
SELECT DISTINCT 
  a.*,
  b.product_name,
  b.brand 
FROM staging_price_collection.historical_rebuy a 
  LEFT JOIN master.variant  b 
    ON a.product_sku = b.product_sku
)
,final AS (
SELECT DISTINCT
  'GERMANY' AS region,
  'REBUY' AS src,
  item_id::INT AS item_id,
  product_name AS product_name, 
  brand, 
  product_sku AS product_sku, 
  asset_condition,
  NULLIF(REGEXP_REPLACE(price , '([^0-9.])', ''),'')::NUMERIC AS price,
  NULLIF(REGEXP_REPLACE(price , '([^0-9.])', ''),'')::NUMERIC AS price_in_euro, 
  'EUR' AS currency,
  reporting_month::DATE AS reporting_month,
  COALESCE(added_at::DATE,reporting_month::DATE) added_at,
  'MOZENDA' AS crawler
FROM a
WHERE price IS NOT NULL
UNION ALL
SELECT DISTINCT
  'GERMANY' AS region,
  'REBUY' AS src,
  item_id::INT AS item_id,
  product_name AS product_name, 
  brand, 
  product_sku AS product_sku, 
  CASE
   WHEN condition = 'Neu' 
    THEN 'Neu'
   WHEN condition ILIKE '%Wie neu%' 
    THEN 'Wie neu'
   WHEN condition ILIKE '%Sehr gut%' 
    THEN 'Sehr gut'
   WHEN condition ILIKE '%Gut%' 
    THEN 'Gut'
   WHEN condition ILIKE  '%Stark genutzt%' THEN 'Akzeptabel'
   ELSE 'Others' 
  END AS asset_condition,
  NULLIF(REGEXP_REPLACE(REPLACE(REPLACE(price, '€', ''), ',', '.') , '([^0-9.])', ''),'')::DECIMAL(38,2) AS price,
  NULLIF(REGEXP_REPLACE(REPLACE(REPLACE(price, '€', ''), ',', '.') , '([^0-9.])', ''),'')::DECIMAL(38,2) AS price_in_euro, 
  'EUR' AS currency,
  CASE 
   WHEN crawled_at::DATE = '2000-01-01' 
    THEN inserted_at::DATE
   WHEN DATE_TRUNC('month',inserted_at::DATE) >= '2023-04-01'
    THEN 
     CASE 
      WHEN DATE_TRUNC('month',crawled_at::DATE) = DATE_TRUNC('month',inserted_at::DATE)
       THEN crawled_at::DATE
      ELSE NULL 
     END 
   ELSE crawled_at::DATE  
  END AS reporting_month,
  inserted_at::DATE AS added_at ,
  'MOZENDA' AS crawler
FROM staging_price_collection.rebuy
WHERE price <> '' 
UNION 
SELECT DISTINCT
  'GERMANY' AS region,
  'REBUY' AS src,
  9999::INT AS item_id,
  input_product_name AS product_name, 
  input_brand AS BRAND, 
  input_product_sku AS product_sku, 
  CASE
   WHEN result_condition = 'A1' 
    THEN 'Wie neu'
   WHEN result_condition = 'A2'  
    THEN 'Sehr gut'
   WHEN result_condition = 'A3'  
    THEN 'Gut'
   WHEN result_condition = 'A4' 
    THEN 'Akzeptabel'
   ELSE 'Others'
  END AS asset_condition,
  NULLIF(REGEXP_REPLACE(REPLACE(REPLACE(result_price, '€', ''), ',', '.') , '([^0-9.])', ''),'')::DECIMAL(38,2) AS price,
  NULLIF(REGEXP_REPLACE(REPLACE(REPLACE(result_price, '€', ''), ',', '.') , '([^0-9.])', ''),'')::DECIMAL(38,2) AS price_in_euro,
  'EUR' AS currency,
  extracted_at::DATE AS reporting_month,
  inserted_at::DATE AS added_at ,
  'SCOPAS' AS crawler 
FROM staging_price_collection.scopas_rebuy_eu 
WHERE result_price <> '' 
  AND reporting_month >= '2022-06-01'
)
SELECT * 
FROM final 
WHERE price IS NOT NULL 
WITH NO SCHEMA BINDING 
;
