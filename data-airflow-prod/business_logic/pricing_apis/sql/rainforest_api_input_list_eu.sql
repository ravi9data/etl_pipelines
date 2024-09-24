WITH eu_request_data AS (
  SELECT 
    'offers'::text AS "type",
    'true'::text AS offers_condition_new,
    'true'::text AS offers_condition_used_like_new,
    'true'::text AS offers_condition_used_very_good,
    10 AS max_page,
    'amazon.de'::text AS amazon_domain,
    'eur'::text AS currency,
    'en_US'::text AS "language",
    'false'::text AS skip_gtin_cache
  UNION ALL
  SELECT 
    'offers'::text AS "type",
    'true'::text AS offers_condition_new,
    'true'::text AS offers_condition_used_like_new,
    'true'::text AS offers_condition_used_very_good,
    10 AS max_page,
    'amazon.nl'::text AS amazon_domain,
    'eur'::text AS currency,
    'en_US'::text AS "language",
    'false'::text AS skip_gtin_cache
  UNION ALL
  SELECT 
    'offers'::text AS "type",
    'true'::text AS offers_condition_new,
    'true'::text AS offers_condition_used_like_new,
    'true'::text AS offers_condition_used_very_good,
    10 AS max_page,
    'amazon.es'::text AS amazon_domain,
    'eur'::text AS currency,
    'en_US'::text AS "language",
    'false'::text AS skip_gtin_cache

),
eu_input_list as (
  SELECT p.ean gtin,
    p.product_sku
  FROM staging_price_collection.v_crawler_input_list p
  WHERE gtin IS NOT NULL
)
SELECT r."type",
  r.offers_condition_new,
  r.offers_condition_used_like_new,
  r.offers_condition_used_very_good,
  r.max_page,
  r.amazon_domain,
  r.currency,
  r."language",
  r.skip_gtin_cache,
  i.gtin,
  i.product_sku custom_id
FROM eu_request_data r
  CROSS JOIN eu_input_list i
ORDER BY gtin,
  amazon_domain;
