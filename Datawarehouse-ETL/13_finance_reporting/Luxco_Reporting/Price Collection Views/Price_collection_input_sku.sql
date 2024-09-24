/*Creating one source for SKU list for inputing into Cralwers*/   
CREATE OR REPLACE VIEW staging_price_collection.v_crawler_input_list AS 
WITH sku_base AS (
SELECT DISTINCT product_sku,
product_name,
brand,
variant_sku,
ean FROM master.asset)
,base AS (
SELECT 
DISTINCT 
COALESCE(a.product_sku,v.product_sku) AS product_sku,
COALESCE(a.variant_sku,v.variant_sku) AS variant_sku,
COALESCE(a.product_name,v.product_name) AS product_name,
COALESCE(a.brand,v.brand) AS brand,
COALESCE(REGEXP_SUBSTR(COALESCE(a.ean,v.ean,ov.ean),'[0-9]*'),NULL) AS ean,---excluding eans with letters like '%wrong%'
COALESCE(REGEXP_SUBSTR(ov.upcs,'[0-9]*'),NULL) AS upcs
FROM sku_base a 
LEFT JOIN master.variant_historical v
 ON a.variant_sku=v.variant_sku
LEFT JOIN ods_production.variant AS ov
ON v.variant_sku=ov.variant_sku
 WHERE date =current_date-1)
 SELECT 
 DISTINCT 
    product_sku,
    variant_sku,
    product_name,
    brand,
    ean,
    NULL AS upcs,
    'EU' AS Region
 FROM  base eu 
 WHERE ean IS NOT NULL 
  AND len(ean)>5 ---to remove few odd EAN with less than 5 digits
 UNION ALL 
 SELECT 
 distinct
    product_sku,
    variant_sku,
    product_name,
    brand,
    NULL AS ean,
    upcs,
    'US' AS Region
 FROM  base eu 
 WHERE upcs IS NOT NULL
 AND len(upcs)>5 ---to remove few odd UPCS with less than 5 digits)
 WITH NO SCHEMA Binding;

GRANT SELECT ON staging_price_collection.v_crawler_input_list TO tableau;


