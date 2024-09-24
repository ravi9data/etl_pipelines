with eu_request_data as (
  SELECT
    *
  FROM
    (
      VALUES
        ('search', 5, 'ebay.de', 'de', 'best_match'),
        ('search', 5, 'ebay.es', 'es', 'best_match'),
        ('search', 5, 'ebay.at', 'at', 'best_match'),
        ('search', 5, 'ebay.nl', 'nl', 'best_match')
    ) as t (
      "type",
      max_page,
      ebay_domain,
      customer_location,
      sort_by
    )
),
us_request_data as (
  SELECT
    *
  FROM
    (
      VALUES
        ('search', 5, 'ebay.com', 'us', 'best_match')
    ) as t (
      "type",
      max_page,
      ebay_domain,
      customer_location,
      sort_by
    )
),
eu_product_skus AS (
  SELECT
    *
  FROM
    TABLE (
      redshift.system.query(
        'SELECT
            distinct product_sku
        FROM
            master.asset_historical
        WHERE
            capital_source_name in (
              ''Grover Finance I GmbH'',
              ''SUSTAINABLE TECH RENTAL EUROPE II GMBH''
            )'
      )
    ) a
),
us_product_skus AS (
  SELECT
    *
  FROM
    TABLE (
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
  select
    DISTINCT p.sku product_sku,
    case
      when length(eans.value) >= 13 then eans.value
      when eans.value is null
      and upcs.value is not null then lpad(upcs.value, 13, '0')
      else lpad(eans.value, 13, '0')
    end search_term
  from
    data_production_rds_datawarehouse_api_production.spree_products p
    INNER JOIN eu_product_skus pskus on p.sku = pskus.product_sku
    LEFT JOIN data_production_rds_datawarehouse_api_production.spree_variants v ON v.product_id = p.id
    LEFT JOIN data_production_rds_datawarehouse_api_production.eans eans ON v.id = eans.variant_id
    LEFT JOIN data_production_rds_datawarehouse_api_production.upcs upcs ON v.id = upcs.variant_id
),
us_input_list as (
  select
    DISTINCT p.sku product_sku,
    case
      when length(upcs.value) >= 12 then upcs.value
      else lpad(upcs.value, 12, '0')
    end search_term
  from
    data_production_rds_datawarehouse_api_production.spree_products p
    INNER JOIN us_product_skus pskus on p.sku = pskus.product_sku
    LEFT JOIN data_production_rds_datawarehouse_api_production.spree_variants v ON v.product_id = p.id
    LEFT JOIN data_production_rds_datawarehouse_api_production.upcs upcs ON v.id = upcs.variant_id
)
select
  *
from
  (
    select
      "type",
      max_page,
      ebay_domain,
      customer_location,
      sort_by,
      search_term,
      product_sku custom_id
    from
      eu_request_data
      cross join eu_input_list
    where
      search_term is not null
      and regexp_like(search_term, '^\d{13,14}$')
  )
union all
  (
    select
      "type",
      max_page,
      ebay_domain,
      customer_location,
      sort_by,
      search_term,
      product_sku custom_id
    from
      us_request_data
      cross join us_input_list
    where
      search_term is not null
      and regexp_like(search_term, '^\d{12}$')
  )
order by
  search_term,
  ebay_domain