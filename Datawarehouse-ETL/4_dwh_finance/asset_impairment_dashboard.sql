DROP TABLE IF EXISTS dm_finance.impairments_asset_historical;
CREATE TABLE dm_finance.impairments_asset_historical AS
WITH written_off_asset_list AS (
SELECT DISTINCT asset_id 
FROM master.asset_historical  
WHERE asset_status_original IN ('%WRITTEN OF DC%', 'WRITTEN OFF DC', 'WRITTEN OFF OPS')
)
,written_off_asset_prev_status AS (
SELECT 
  ah.asset_id 
 ,ah.date
 ,ah.asset_status_original
 ,LAG(asset_status_original) OVER (PARTITION BY ah.asset_id ORDER BY ah.date) previous_asset_status_original
FROM master.asset_historical ah
  INNER JOIN written_off_asset_list w
    ON ah.asset_id = w.asset_id
 )
,last_written_off_date AS (
SELECT
  asset_id 
 ,MAX(date) latest_written_off_date
FROM written_off_asset_prev_status
WHERE asset_status_original IN ('%WRITTEN OF DC%', 'WRITTEN OFF DC', 'WRITTEN OFF OPS')
  AND COALESCE(previous_asset_status_original, 'n/a') NOT IN ('%WRITTEN OF DC%', 'WRITTEN OFF DC', 'WRITTEN OFF OPS')
GROUP BY 1  
)
SELECT 
  ah.date
 ,ah.asset_id
 ,ah.asset_allocation_sf_id 
 ,ah.asset_allocation_id 
 ,ah.active_subscription_id 
 ,ah.capital_source_name 
 ,ah.asset_status_original
 ,LAG(ah.asset_status_original) OVER (PARTITION BY ah.asset_id ORDER BY date) previous_asset_status_original
 ,COALESCE(dpd.dpd_new, ah.last_allocation_dpd) AS last_allocation_dpd_ 
 ,LAG(last_allocation_dpd_) OVER (PARTITION BY ah.asset_id ORDER BY date) previous_last_allocation_dpd
 ,CASE 
 	 WHEN last_allocation_dpd_ IS NOT NULL 
 	   AND previous_last_allocation_dpd IS NOT NULL
 	  THEN previous_last_allocation_dpd - last_allocation_dpd_
 END AS dpd_difference
 ,ah.max_paid_date 
 ,ah.initial_price 
 ,ah.residual_value_market_price 
 ,CASE 
   WHEN last_allocation_dpd_ BETWEEN 1 AND 30 
    THEN 'DPD 1-30'
   WHEN last_allocation_dpd_ BETWEEN 31 AND 60 
    THEN 'DPD 31-60'
   WHEN last_allocation_dpd_ BETWEEN 61 AND 90 
    THEN 'DPD 61-90'
   WHEN last_allocation_dpd_ BETWEEN 91 AND 120 
    THEN 'DPD 91-120'
   WHEN last_allocation_dpd_ BETWEEN 121 AND 150 
    THEN 'DPD 121-150'
   WHEN last_allocation_dpd_ BETWEEN 151 AND 180 
    THEN 'DPD 151-180'
   WHEN last_allocation_dpd_ BETWEEN 181 AND 210 
    THEN 'DPD 181-210'
   WHEN last_allocation_dpd_ BETWEEN 211 AND 240 
    THEN 'DPD 211-240'
   WHEN last_allocation_dpd_ BETWEEN 241 AND 270 
    THEN 'DPD 241-270'
   WHEN last_allocation_dpd_ BETWEEN 271 AND 300 
    THEN 'DPD 271-300'
   WHEN last_allocation_dpd_ BETWEEN 301 AND 330 
    THEN 'DPD 301-330'
   WHEN last_allocation_dpd_ BETWEEN 331 AND 360 
    THEN 'DPD 331-360'
   WHEN last_allocation_dpd_ >= 361
    THEN 'DPD 361+'
   ELSE ah.dpd_bucket
  END AS dpd_bucket_interval_30
 ,CASE 
   WHEN last_allocation_dpd_ BETWEEN 1 AND 30 
    THEN 'DPD 1-30'
   WHEN last_allocation_dpd_ BETWEEN 31 AND 60 
    THEN 'DPD 31-60'
   WHEN last_allocation_dpd_ BETWEEN 61 AND 90 
    THEN 'DPD 61-90'
   WHEN last_allocation_dpd_ BETWEEN 91 AND 120 
    THEN 'DPD 91-120'
   WHEN last_allocation_dpd_ BETWEEN 121 AND 150 
    THEN 'DPD 121-150'
   WHEN last_allocation_dpd_ BETWEEN 151 AND 180 
    THEN 'DPD 151-180'
   WHEN last_allocation_dpd_ BETWEEN 181 AND 360 
    THEN 'DPD 181-360'
   WHEN last_allocation_dpd_ >= 361
    THEN 'DPD 361+'
   ELSE ah.dpd_bucket
  END AS dpd_bucket_interval_180 
 ,w.latest_written_off_date
FROM master.asset_historical ah
  LEFT JOIN last_written_off_date w
    ON ah.asset_id = w.asset_id
  LEFT JOIN dm_finance.sub_dpd_corrected_final dpd
    ON ah.asset_id = dpd.asset_id 
   AND ah.date = dpd.reporting_date 
WHERE TRUE
  AND ah.date >= DATE_ADD('YEAR', -1, CURRENT_DATE)::DATE
  AND (ah.date = LAST_DAY(ah.date)
  OR ah.date = CURRENT_DATE - 1)
;

GRANT SELECT ON dm_finance.impairments_asset_historical TO tableau;
