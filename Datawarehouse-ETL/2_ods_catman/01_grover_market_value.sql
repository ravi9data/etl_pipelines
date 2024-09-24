
WITH fact_date AS (
SELECT
  datum
 ,product_sku
FROM public.dim_dates
  CROSS JOIN ods_production.product
WHERE TRUE
  AND datum BETWEEN '2018-08-01'AND CURRENT_DATE
)
,spv AS (
SELECT DISTINCT 
  datum
 ,a.product_sku 
 -- AVG_AVG_PRICE
 ,LAST_VALUE(avg_avg_price) OVER (PARTITION BY p.product_sku, DATE_TRUNC('MONTH', reporting_date::DATE)
             ORDER BY reporting_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS price_spv
FROM fact_date a
  LEFT JOIN ods_external.product_month_condition_price p 
    ON a.datum = DATE_TRUNC('MONTH', p.reporting_date::DATE)
   AND a.product_sku = p.product_sku
WHERE p.asset_condition = 'Neu'
)
,media_markt_raw AS (
SELECT DISTINCT 
  week_date AS week_date
 ,product_sku
 ,price
 ,AVG(price) OVER (PARTITION BY product_sku
      ORDER BY week_date ROWS 2 PRECEDING) AS mm_price_3_weeks
FROM ods_external.mm_price_data
)
,media_markt_processed AS (
SELECT DISTINCT 
  DATE_TRUNC('MONTH', mm.week_date)::DATE AS datum
 ,a.product_sku
 ,LAST_VALUE(mm.price::NUMERIC) OVER (PARTITION BY mm.product_sku, DATE_TRUNC('MONTH', mm.week_date)
             ORDER BY mm.week_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS price_mm
 ,LAST_VALUE(mm.mm_price_3_weeks::NUMERIC) OVER (PARTITION BY mm.product_sku, DATE_TRUNC('MONTH', mm.week_date)
             ORDER BY mm.week_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS price_mm_value_3_weeks
FROM fact_date a
  LEFT JOIN media_markt_raw  mm 
    ON a.datum = mm.week_date
   AND a.product_sku = mm.product_sku
)
,asset AS (
SELECT DISTINCT 
  d.datum
 ,a.product_sku
 ,LAST_VALUE(a.initial_price) OVER (PARTITION BY a.product_sku, DATE_TRUNC('MONTH', a.purchased_date::DATE)
             ORDER BY a.purchased_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS price_pp
 ,LAST_VALUE(a.amount_rrp) OVER (PARTITION BY a.product_sku, DATE_TRUNC('MONTH', a.purchased_date::DATE)
             ORDER BY a.purchased_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS price_rrp
FROM fact_date d
  LEFT JOIN ods_production.asset a
    ON d.datum = DATE_TRUNC('MONTH', a.purchased_date::DATE)
   AND d.product_sku = a.product_sku
)
SELECT DISTINCT 
  DATE_TRUNC('MONTH', fd.datum)::DATE AS datum
 ,fd.product_sku
 ,spv.price_spv
 ,mm.price_mm
 ,mm.price_mm_value_3_weeks
 ,a.price_pp
 ,a.price_rrp
 ,CASE
   WHEN a.price_pp > 
     CASE
      WHEN mm.price_mm_value_3_weeks > spv.price_spv 
       THEN mm.price_mm_value_3_weeks
      WHEN spv.price_spv > mm.price_mm_value_3_weeks 
       THEN LEAST((mm.price_mm_value_3_weeks * 1.1), spv.price_spv)
      WHEN COALESCE(spv.price_spv, 0) > 0 
       THEN spv.price_spv
      WHEN COALESCE(mm.price_mm_value_3_weeks, 0) > 0
       THEN mm.price_mm_value_3_weeks
      ELSE NULL
     END 
    THEN a.price_pp
   ELSE 
     CASE
      WHEN mm.price_mm_value_3_weeks > spv.price_spv 
       THEN mm.price_mm_value_3_weeks
      WHEN spv.price_spv > mm.price_mm_value_3_weeks 
       THEN LEAST((mm.price_mm_value_3_weeks * 1.1), spv.price_spv)
      WHEN COALESCE(spv.price_spv, 0) > 0 
       THEN spv.price_spv
      WHEN COALESCE(mm.price_mm_value_3_weeks, 0) > 0 
       THEN mm.price_mm_value_3_weeks
      ELSE a.price_rrp
     END
 END AS mkp
FROM fact_date fd
  LEFT JOIN spv
    ON DATE_TRUNC('MONTH', fd.datum)::DATE = spv.datum
   AND fd.product_sku = spv.product_sku
  LEFT JOIN media_markt_processed mm
    ON DATE_TRUNC('MONTH', fd.datum)::DATE = mm.datum
   AND fd.product_sku = mm.product_sku
  LEFT JOIN asset a
    ON DATE_TRUNC('MONTH', fd.datum)::DATE = a.datum
   AND fd.product_sku = a.product_sku
;

