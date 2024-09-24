with eu_request_data as (
  SELECT *
  FROM (
      VALUES (
          'offers',
          'true',
          'true',
          'true',
          5,
          'amazon.de',
          'eur',
          'en_US',
          'false'
        )
        -- ('offers',5,'amazon.es','eur','en_US','false'),
        -- ('offers',5,'amazon.nl','eur','en_US','false')
    ) AS t (
      "type",
      offers_condition_new,
      offers_condition_used_like_new,
      offers_condition_used_very_good,
      max_page,
      amazon_domain,
      currency,
      "language",
      skip_gtin_cache
    )
),
us_request_data AS (
  SELECT *
  FROM (
      VALUES (
          'offers',
          'true',
          'true',
          'true',
          5,
          'amazon.com',
          'usd',
          'en_US',
          'false'
        )
    ) AS t (
      "type",
      offers_condition_new,
      offers_condition_used_like_new,
      offers_condition_used_very_good,
      max_page,
      amazon_domain,
      currency,
      "language",
      skip_gtin_cache
    )
),
us_product_skus AS (
  SELECT *
  FROM TABLE (
      redshift.system.query(
        'SELECT
                distinct product_sku
        FROM
            master.asset_historical
        WHERE
            capital_source_name in (''USA_test'')'
      )
    ) a
),
eu_input_list as (
  SELECT p.ean gtin,
    p.product_sku
    FROM staging_price_collection.v_crawler_input_list p
),
us_input_list as (
  select DISTINCT p.sku product_sku,
    case
      when length(upcs.value) >= 12 then upcs.value
      else lpad(upcs.value, 12, '0')
    end gtin
  from data_production_rds_datawarehouse_api_production.spree_products p
    INNER JOIN us_product_skus pskus on p.sku = pskus.product_sku
    LEFT JOIN data_production_rds_datawarehouse_api_production.spree_variants v ON v.product_id = p.id
    LEFT JOIN data_production_rds_datawarehouse_api_production.upcs upcs ON v.id = upcs.variant_id
)
select *
from (
    select "type",
      offers_condition_new,
      offers_condition_used_like_new,
      offers_condition_used_very_good,
      max_page,
      amazon_domain,
      currency,
      "language",
      skip_gtin_cache,
      gtin,
      product_sku custom_id
    from eu_request_data
      cross join eu_input_list
  )
union all
(
  select "type",
    offers_condition_new,
    offers_condition_used_like_new,
    offers_condition_used_very_good,
    max_page,
    amazon_domain,
    currency,
    "language",
    skip_gtin_cache,
    gtin,
    product_sku custom_id
  from us_request_data
    cross join us_input_list
  where gtin is not null
    and regexp_like(gtin, '^\d{12}$')
)
order by gtin,
  amazon_domain
