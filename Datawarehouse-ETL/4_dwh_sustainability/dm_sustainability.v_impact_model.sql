CREATE OR REPLACE VIEW dm_sustainability.v_impact_model AS
WITH duration AS (
    SELECT
        a.asset_id,
        SUM(CASE
                WHEN a.delivered_at IS NOT NULL
                    THEN (COALESCE(a.cancellation_returned_at::date,
                                   s.cancellation_date::date,
                                   a.return_delivery_date::date,
                                   DATEADD('day', -1, DATE_TRUNC('year', CURRENT_DATE))
                          )
                    - (a.delivered_at::date))
                ELSE 0
            END
        ) AS days_on_rent,
        SUM(s.effective_duration) AS effective_duration
    FROM master.allocation_historical a
    LEFT JOIN master.subscription_historical s
        ON a.subscription_id = s.subscription_id
        AND a."date" = s."date"
    WHERE a."date" = DATEADD('day', -1, DATE_TRUNC('year', CURRENT_DATE))
    GROUP BY 1
)
, a as (
    SELECT
        --ast.asset_id,
        DATE_PART('year', purchased_date) AS purchase_cohort,
        brand,
        subcategory_name,
        category_name,
        asset_status_original,
        asset_status_detailed,
        capital_source_name AS capital_source,
        COUNT(*) AS number_of_assets,
        SUM(COALESCE(delivered_allocations, 0)) AS number_of_rental_cycles,
        --avg(coalesce(delivered_allocations,0)::decimal(10,2))::decimal(10,2) as avg_number_of_cycles,
        SUM(COALESCE(effective_duration, 0)) AS total_rental_period,
        --total_rental_period / NULLIF(number_of_rental_cycles,0)::decimal(10,2) as avg_rental_period_asset,
        SUM(COALESCE(days_on_rent, 0)) AS total_days_on_rent,
        SUM(COALESCE(months_on_book, 0)) AS total_months_on_book,
        --avg(case when asset_status_new = 'SOLD' then months_on_book::decimal(10,2) end)::decimal(10,2) as age_when_sold,
        COUNT(CASE WHEN asset_status_detailed IN ('SOLD 1-euro', 'SOLD to Customer')
                   THEN ast.asset_id END)
        AS asset_sold_to_customer,
        COUNT(CASE WHEN asset_status_detailed IN ('SOLD to 3rd party')
                   THEN ast.asset_id END)
        AS asset_sold_to_third_party--,
        --avg(months_on_book::decimal(10,2))::decimal(10,2) as avg_product_age
    FROM master.asset_historical ast
    LEFT JOIN duration allo
        ON allo.asset_id = ast.asset_id
    WHERE ast."date" = DATEADD('day', -1, DATE_TRUNC('year', CURRENT_DATE))
    GROUP BY 1, 2, 3, 4, 5, 6, 7--, 8
    ORDER BY 1 DESC, 4
)
select
	a.*,
	w.value::float as weight_assumption,
    f.value::float as footprint_assumption,
    sp.value::float as shipment_proxy,
    lr.value::float as likelihood_repair,
    efr.value::float as emission_factor_repair,
    efru.value::float as emission_factor_refurbishment,
    acc.value::float as additional_cycles_customers,
    actp.value::float as additional_cycles_3rd_party,
    ac.value::float as additional_consumption
FROM a
LEFT JOIN staging_google_sheet.impact_model_assumptions w
  ON a.subcategory_name = w.subcategory
  AND w.assumption_type = 'weight'
LEFT JOIN staging_google_sheet.impact_model_assumptions f
  ON a.subcategory_name = f.subcategory
  AND f.assumption_type = 'footprint'
LEFT JOIN staging_google_sheet.impact_model_assumptions sp
  ON sp.assumption_type = 'shipment_proxy'
LEFT JOIN staging_google_sheet.impact_model_assumptions lr
  ON lr.assumption_type = 'likelihood_repair'
LEFT JOIN staging_google_sheet.impact_model_assumptions efr
  ON efr.assumption_type = 'emission_factor_repair'
LEFT JOIN staging_google_sheet.impact_model_assumptions efru
  ON efru.assumption_type = 'emission_factor_refurbishment'
LEFT JOIN staging_google_sheet.impact_model_assumptions acc
  ON acc.assumption_type = 'additional_cycles_customers'
LEFT JOIN staging_google_sheet.impact_model_assumptions actp
  ON actp.assumption_type = 'additional_cycles_3rd_party'
LEFT JOIN staging_google_sheet.impact_model_assumptions ac
  ON ac.assumption_type = 'additional_consumption'
WITH NO SCHEMA BINDING
;

GRANT SELECT ON dm_sustainability.v_impact_model TO tableau;