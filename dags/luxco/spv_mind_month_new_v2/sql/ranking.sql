--give  priority code to each country-------------------
DROP TABLE IF EXISTS ods_spv_historical.all_eu_crawler_m_zero_values_{{ params.tbl_suffix }};
CREATE TABLE ods_spv_historical.all_eu_crawler_m_zero_values_{{ params.tbl_suffix }} AS 
SELECT 
  1 AS code,
  m_since_as_good_as_new_price,
  agan_price_standardized,
  * 
FROM ods_spv_historical.price_per_condition_1_mid_month
WHERE TRUE 
  AND m_since_as_good_as_new_price = 0
 
UNION ALL 

SELECT
  2 AS code,
  m_since_as_good_as_new_price,
  agan_price_standardized,
  *  
FROM ods_spv_historical.price_per_condition_2_mid_month
WHERE TRUE 
  AND m_since_as_good_as_new_price = 0
 
/*UNION ALL 

SELECT 
  4 AS code,
  m_since_as_good_as_new_price,
  agan_price_standardized,
  *  
FROM ods_spv_historical.price_per_condition_4
WHERE TRUE 
  AND m_since_as_good_as_new_price = 0
 
UNION ALL 

SELECT 
  5 AS code,
  m_since_as_good_as_new_price,
  agan_price_standardized,
  * 
FROM ods_spv_historical.price_per_condition_5
WHERE TRUE 
  AND m_since_as_good_as_new_price = 0

UNION ALL 

SELECT 
  6 AS code,
  m_since_as_good_as_new_price,
  agan_price_standardized,
  * 
FROM ods_spv_historical.price_per_condition_6
WHERE TRUE 
  AND m_since_as_good_as_new_price = 0
  
  UNION ALL 

SELECT 
  7 AS code,
  m_since_as_good_as_new_price,
  agan_price_standardized,
  * 
FROM ods_spv_historical.price_per_condition_7
WHERE TRUE 
  AND m_since_as_good_as_new_price = 0
  UNION ALL 

SELECT 
  8 AS code,
  m_since_as_good_as_new_price,
  agan_price_standardized,
  * 
FROM ods_spv_historical.price_per_condition_8
WHERE TRUE 
  AND m_since_as_good_as_new_price = 0*/;
 

  
 
DROP TABLE IF EXISTS ods_spv_historical.all_eu_prioritization_new_{{ params.tbl_suffix }};
CREATE TABLE ods_spv_historical.all_eu_prioritization_new_{{ params.tbl_suffix }} AS 

 WITH price_country_rank AS
(
SELECT 
  rank() OVER (PARTITION BY product_sku ORDER BY code ASC) AS price_rank,
  * 
FROM 
ods_spv_historical.all_eu_crawler_m_zero_values_{{ params.tbl_suffix }}
)
SELECT *
FROM price_country_rank
WHERE price_rank = 1
;



 
DROP TABLE IF EXISTS ods_spv_historical.price_per_condition_{{ params.tbl_suffix }}_eu;
CREATE TABLE ods_spv_historical.price_per_condition_{{ params.tbl_suffix }}_eu AS 
SELECT 
  a.reporting_date,
  a.product_sku,
  a.neu_price,
  nvl(b.as_good_as_new_price,a.as_good_as_new_price) AS as_good_as_new_price,
  a.sehr_gut_price,
  a.gut_price,
  a.akzeptabel_price,
  a.neu_price_before_discount,
  nvl(b.as_good_as_new_price_before_discount,a.as_good_as_new_price_before_discount) AS as_good_as_new_price_before_discount,
  a.sehr_gut_price_before_discount,
  a.gut_price_before_discount,
  a.akzeptabel_price_before_discount,
  a.m_since_neu_price,
  nvl(b.m_since_as_good_as_new_price,a.m_since_as_good_as_new_price) AS m_since_as_good_as_new_price,
  a.m_since_sehr_gut_price,
  a.m_since_gut_price,
  a.m_since_akzeptabel_price,
  a.new_price_standardized,
  nvl(b.agan_price_standardized,a.agan_price_standardized) AS agan_price_standardized,
  nvl(b.agan_price_standardized_before_discount,a.agan_price_standardized_before_discount) AS agan_price_standardized_before_discount,
  nvl(b.m_since_agan_price_standardized,a.m_since_agan_price_standardized) AS m_since_agan_price_standardized,
  a.used_price_standardized,
  a.used_price_standardized_before_discount,
  a.m_since_used_price_standardized
FROM ods_spv_historical.price_per_condition_2_mid_month a
  LEFT JOIN ods_spv_historical.all_eu_prioritization_new_{{ params.tbl_suffix }} b
    ON a.product_sku = b.product_sku
;
