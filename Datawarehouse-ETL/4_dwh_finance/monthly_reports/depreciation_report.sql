DROP TABLE IF EXISTS dm_finance.depreciation_monthend_report;
CREATE TABLE dm_finance.depreciation_monthend_report AS 
WITH asset_event_date_pre AS (
SELECT 
  asset_id 
 ,CASE
   WHEN asset_status_original LIKE 'WRITTEN OFF%' 
    THEN date 
  END AS write_off_date
 ,CASE
   WHEN asset_status_original = 'LOST' 
    THEN date 
  END AS lost_date
 ,CASE
   WHEN asset_status_original = 'LOST SOLVED' 
    THEN date 
  END AS lost_solved_date
FROM master.asset_historical
)
,asset_event_date AS (   
SELECT
  asset_id 
 ,MIN(write_off_date) AS write_off_date
 ,MIN(lost_date) AS lost_date
 ,MIN(lost_solved_date) AS lost_solved_date
FROM asset_event_date_pre
GROUP BY 1
)
--- Extrating data FROM google sheets AND cleaning up the rate column.
,lkp_asset_depreciation_rates AS (
SELECT
  category
 ,subcategory
 ,brand
 ,ifrscategory
 ,REPLACE(rate,'%','')::FLOAT AS rate_lookup
FROM stg_external_apis.depreciation_category_split
)
--- Raw data extraction begins here
,depreciation_raw_stg1 AS (
SELECT 
  a.asset_id 
 ,a.asset_status_original 
 ,a.asset_status_new
 ,a.asset_status_detailed
 ,a.customer_id 
 ,a.subscription_id 
 ,a.created_at 
 ,a.updated_at 
 ,a.asset_allocation_id 
 ,a.asset_allocation_sf_id 
 ,a.warehouse 
 ,a.capital_source_name 
 ,a.supplier 
 ,a.first_allocation_store 
 ,a.first_allocation_store_name 
 ,a.serial_number 
 ,a.ean 
 ,a.product_sku 
 ,a.asset_name 
 ,a.asset_condition 
 ,a.asset_condition_spv 
 ,a.variant_sku 
 ,a.product_name 
 ,a.category_name 
 ,a.subcategory_name 
 ,a.brand 
 ,a.purchased_date 
 ,a.initial_price 
 ,a.sold_date
 ,DATE_TRUNC('MONTH',a.purchased_date)::DATE AS purchase_cohort
 ,CASE    
   WHEN a.sold_date IS NULL 
    THEN '2073-10-14'::DATE
   ELSE a.sold_date 
  END AS sold_date_clean
 ,CASE    
   WHEN a.sold_date IS NULL 
    THEN '2073-10-01'::DATE
   ELSE DATE_TRUNC('MONTH',a.sold_date)::DATE 
  END AS sold_cohort
 ,a.sold_price AS sold_price_actual
 ,CASE 
   WHEN DATE_PART_YEAR(a.purchased_date) < 2020 
     AND a.initial_price / (1.19) < 150 
    THEN 0
   WHEN DATE_PART_YEAR(a.purchased_date) < 2020 
     AND a.initial_price / (1.19) >= 150 
    THEN a.initial_price / (1.19)
   WHEN DATE_PART_YEAR(a.purchased_date) >= 2020 
     AND (a.purchased_date < '2020-07-01' OR a.purchased_date >= '2021-01-01') 
    THEN a.initial_price / (1.19)
   WHEN DATE_PART_YEAR(a.purchased_date) >= 2020 
     AND a.purchased_date BETWEEN '2020-07-01' AND '2020-12-31' 
    THEN a.initial_price / (1.16)
   ELSE -9999
  END AS net_vat_pp_after_150_expense
 ,CASE  
   WHEN b.rate_lookup IS NULL    
    THEN 0.018
   ELSE b.rate_lookup/100
  END ::FLOAT AS rate
 ,CASE   
   WHEN b.rate_lookup IS NULL
    THEN 1
   ELSE 0
  END AS fl_default_rate_applied
 ,d.lost_date 
 ,a.lost_reason
 ,d.lost_solved_date 
 ,d.write_off_date 
 ,rate * net_vat_pp_after_150_expense AS monthly_depreciated_value
FROM master.asset a  
  LEFT JOIN lkp_asset_depreciation_rates b
    ON a.category_name = b.category
   AND a.subcategory_name = b.subcategory
   AND a.brand = b.brand
  LEFT JOIN asset_event_date d 
    ON d.asset_id = a.asset_id 
)
--- Getting month END dates for snapshots
,depreciation_dim_date_stg2 AS(
SELECT 
  a.*
 ,dd.datum
FROM depreciation_raw_stg1 a
  LEFT JOIN public.dim_dates dd 
    ON dd.datum >= a.purchase_cohort
   AND dd.day_is_first_of_month = 1
   AND dd.datum <= DATE_TRUNC('MONTH',CURRENT_DATE)
)
--- Calculation of the deprecitaion for each month END starting FROM the the purchase date of the asset
,depr_int AS (
SELECT 
  *
 ,datum AS snapshot_month
 ,LAST_DAY(datum) AS report_date
 ,LEAD(datum,1) IGNORE NULLS OVER(PARTITION BY asset_id ORDER BY datum ASC) AS next_snapshot_month
 ,CASE 
   WHEN datum = purchase_cohort 
    THEN 1 
   ELSE 0 
  END AS fl_same_purchase_snapshot_cohort
 ,CASE 
   WHEN purchased_date < datum 
    THEN 1 
   ELSE 0 
  END AS fl_purchase_date_lt_snapshot
 ,CASE 
   WHEN sold_date_clean < next_snapshot_month 
    THEN 0 
   ELSE next_snapshot_month - datum 
  END AS day_diff
 ,ROUND(day_diff/30::FLOAT, 2) AS day_factor
 ,CASE
   WHEN fl_same_purchase_snapshot_cohort = 1 
    THEN net_vat_pp_after_150_expense 
   WHEN snapshot_month < purchase_cohort 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 AND sold_date_clean < next_snapshot_month 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 AND lost_date < next_snapshot_month 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 AND lost_solved_date < next_snapshot_month 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 AND write_off_date < next_snapshot_month 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 AND sold_date_clean >= next_snapshot_month 
    THEN -1 * day_factor * monthly_depreciated_value
   WHEN fl_purchase_date_lt_snapshot = 1 AND lost_date >= next_snapshot_month 
    THEN -1 * day_factor * monthly_depreciated_value
   WHEN fl_purchase_date_lt_snapshot = 1 AND lost_solved_date >= next_snapshot_month 
    THEN -1 * day_factor * monthly_depreciated_value
   WHEN fl_purchase_date_lt_snapshot = 1 AND write_off_date >= next_snapshot_month 
    THEN -1 * day_factor * monthly_depreciated_value
   ELSE -999
  END AS depreciation
 ,CASE
   WHEN fl_same_purchase_snapshot_cohort = 1 
    THEN net_vat_pp_after_150_expense 
   WHEN snapshot_month < purchase_cohort 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 
     AND sold_date_clean < next_snapshot_month 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 
     AND lost_date < next_snapshot_month 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 
     AND lost_solved_date < next_snapshot_month 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 
     AND write_off_date < next_snapshot_month 
    THEN 0
   WHEN fl_purchase_date_lt_snapshot = 1 
     AND sold_date_clean >= next_snapshot_month 
    THEN -1 * monthly_depreciated_value
   WHEN fl_purchase_date_lt_snapshot = 1 
     AND lost_date >= next_snapshot_month 
    THEN -1 * monthly_depreciated_value
   WHEN fl_purchase_date_lt_snapshot = 1 
     AND lost_solved_date >= next_snapshot_month 
    THEN -1 * monthly_depreciated_value
   WHEN fl_purchase_date_lt_snapshot = 1 
     AND write_off_date >= next_snapshot_month 
    THEN -1 * monthly_depreciated_value
   ELSE -999
  END AS depreciation_linear
FROM depreciation_dim_date_stg2 ddds
)
,depreciation_snapshots AS (
SELECT 
  *
 ,CASE
   WHEN depreciation = 0 OR depreciation = net_vat_pp_after_150_expense 
    THEN 0
   ELSE SUM(depreciation) OVER (PARTITION BY asset_id ORDER BY snapshot_month 
         ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 
  END AS residual_value
 ,CASE
   WHEN depreciation_linear = 0 OR depreciation_linear = net_vat_pp_after_150_expense 
    THEN 0
   ELSE SUM(depreciation_linear) OVER (PARTITION BY asset_id ORDER BY snapshot_month 
         ASC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) 
  END AS residual_value_linear
FROM depr_int
WHERE next_snapshot_month IS NOT NULL
)
,depreciation_raw AS (
SELECT 
  *
 ,CASE 
   WHEN residual_value <= 0 
    THEN  0
   WHEN -1 * depreciation > residual_value 
    THEN residual_value * -1
   ELSE depreciation 
  END AS depreciation_final
 ,CASE  
   WHEN residual_value_linear <= 0 
    THEN 0
   WHEN -1 * depreciation_linear > residual_value_linear 
    THEN residual_value_linear*-1
   ELSE depreciation_linear 
  END AS depreciation_final_linear
 ,CASE 
   WHEN residual_value < 0 
    THEN 0
   ELSE residual_value 
  END AS residual_value_final
 ,CASE 
   WHEN residual_value_linear < 0 
    THEN 0
   ELSE residual_value_linear 
  END AS residual_value_linear_final
 ,CASE 
   WHEN -1 * depreciation > residual_value 
    THEN 0
   ELSE SUM(depreciation) OVER(PARTITION BY asset_id ORDER BY snapshot_month 
         ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
  END AS book_value_asset
 ,CASE  
   WHEN -1 * depreciation_linear > residual_value_linear 
    THEN 0
   ELSE SUM(depreciation_linear) OVER (PARTITION BY asset_id ORDER BY snapshot_month 
         ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
  END AS book_value_asset_linear
 ,CASE
   WHEN sold_cohort <= snapshot_month 
    THEN 1
   ELSE 0
  END AS fl_asset_sold
FROM depreciation_snapshots
)
SELECT 
  asset_id
 ,asset_status_original
 ,asset_status_new
 ,CASE
   WHEN asset_status_new = 'SOLD'
    THEN asset_status_detailed
   END AS sold_details
 ,CASE 
   WHEN asset_status_new = 'WRITTEN OFF' 
    THEN asset_status_detailed
  END AS written_off_details
 ,asset_name
 ,report_date
 ,created_at
 ,purchased_date
 ,CASE 
   WHEN sold_date>report_date 
    THEN NULL 
   ELSE sold_date 
  END AS sold_date
 ,asset_condition
 ,category_name
 ,subcategory_name
 ,capital_source_name
 ,brand
 ,initial_price
 ,ROUND(residual_value_final, 2) as residual_value
 ,lost_reason
 ,lost_date
 ,write_off_date
 ,ROUND(net_vat_pp_after_150_expense, 2) AS net_purchase_price_after_vat
 ,CASE 
   WHEN sold_date>report_date 
    THEN NULL
   WHEN fl_asset_sold = 1 
    THEN ROUND(sold_price_actual, 2)
   ELSE NULL
  END AS sold_price_actual
FROM depreciation_raw
WHERE report_date >= '2022-01-31'
;                
   
GRANT SELECT ON dm_finance.depreciation_monthend_report TO tableau;