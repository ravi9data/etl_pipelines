-- Query for Dashboard - Asset Analysis
DROP TABLE IF EXISTS dm_finance.impactkpi; 
CREATE TABLE dm_finance.impactkpi AS
SELECT 
  date as reporting_date
 ,asset_id
 ,created_at::DATE AS created_at
 ,initial_price
 ,capital_source_name
 ,category_name
 ,brand
 ,purchased_date
 ,asset_status_original
 ,asset_status_new
 ,dpd_bucket
 ,delivered_allocations
 ,months_since_purchase
 ,warehouse
FROM master.asset_historical a
WHERE TRUE
  AND purchased_date < DATE_TRUNC('month',CURRENT_DATE)
  AND date = LAST_DAY(date)
;

GRANT SELECT ON dm_finance.impactkpi TO tableau;
